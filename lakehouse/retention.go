package lakehouse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// RetentionPolicy 是历史版本保留策略，落盘持久化，重开表后继续生效。
//
// 零值（RetainVersions==0 且 Pinned 为空）表示保留全部历史版本，
// 与不启用清理时的行为完全一致。
type RetentionPolicy struct {
	// RetainVersions 保留最近 N 个已发布版本（始终包含当前版本）。
	// 0 表示不按数量裁剪，历史全部保留（默认行为）。
	RetainVersions int `json:"retain_versions"`
	// Pinned 是长期保留版本集合：即使超出最近 N 个范围也不会被清理。
	Pinned []int64 `json:"pinned,omitempty"`
}

// retentionState 是保留策略与清理进度的持久化状态。
// 对它的任何修改都在提交锁内、以原子 rename 发布。
type retentionState struct {
	Policy RetentionPolicy `json:"policy"`
	// Expired 记录“曾经存在、已被清理（或清理进行中）”的版本，
	// 使读取过期版本能稳定返回 SNAPSHOT_EXPIRED，与文件删除进度无关。
	Expired []int64 `json:"expired,omitempty"`
}

func (t *Table) retentionPath() string {
	return filepath.Join(t.dir, "metadata", "retention.json")
}

func loadRetentionState(dir string) (retentionState, error) {
	st := retentionState{}
	err := readJSON(filepath.Join(dir, "metadata", "retention.json"), &st)
	if err != nil {
		if errIsNotExist(err) {
			return retentionState{}, nil
		}
		return st, err
	}
	return st, nil
}

// RetentionPolicy 返回当前生效的保留策略（落盘值的拷贝）。
func (t *Table) RetentionPolicy() RetentionPolicy {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.retention.Policy.clone()
}

// IsExpired 报告版本是否已被保留策略清理。
func (t *Table) IsExpired(version int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return containsInt64(t.retention.Expired, version)
}

func (p RetentionPolicy) clone() RetentionPolicy {
	return RetentionPolicy{
		RetainVersions: p.RetainVersions,
		Pinned:         append([]int64(nil), p.Pinned...),
	}
}

func normalizePolicy(p RetentionPolicy) (RetentionPolicy, error) {
	if p.RetainVersions < 0 {
		return p, newError(CodeRetentionInvalid,
			"retain_versions must be >= 0 (0 means keep all), got %d", p.RetainVersions)
	}
	out := RetentionPolicy{RetainVersions: p.RetainVersions}
	seen := map[int64]bool{}
	for _, v := range p.Pinned {
		if v < 1 {
			return p, newError(CodeRetentionInvalid, "pinned version %d is not a valid version", v)
		}
		if !seen[v] {
			seen[v] = true
			out.Pinned = append(out.Pinned, v)
		}
	}
	sort.Slice(out.Pinned, func(i, j int) bool { return out.Pinned[i] < out.Pinned[j] })
	return out, nil
}

// SetRetentionPolicy 配置并落盘保留策略。
//
// 该方法只更新策略本身，不删除任何数据；空间回收由 PurgeExpired 完成。
// 与提交/恢复/清理共用提交锁：拿到锁的调用顺序生效，拿不到锁返回
// CONFLICT_IN_PROGRESS，调用方可重试。
func (t *Table) SetRetentionPolicy(ctx context.Context, policy RetentionPolicy) error {
	_ = ctx
	p, err := normalizePolicy(policy)
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := tryAcquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return err
	}
	defer lock.release()

	if err := t.reloadStateLocked(); err != nil {
		return err
	}
	for _, v := range p.Pinned {
		if v > t.meta.CurrentVersion {
			return newError(CodePinTargetInvalid,
				"cannot pin version %d which does not exist yet (current %d)", v, t.meta.CurrentVersion)
		}
		if containsInt64(t.retention.Expired, v) {
			return newError(CodePinTargetInvalid,
				"cannot pin version %d which has already been purged", v)
		}
		if _, statErr := os.Stat(t.snapshotPath(v)); statErr != nil {
			if errIsNotExist(statErr) {
				return newError(CodePinTargetInvalid, "version %d snapshot is missing", v)
			}
			return statErr
		}
	}
	t.retention.Policy = p
	return writeJSONAtomic(t.retentionPath(), t.retention)
}

// PinVersions 把版本追加到长期保留集合（保持其它策略不变）。
func (t *Table) PinVersions(ctx context.Context, versions ...int64) error {
	cur := t.RetentionPolicy()
	m := map[int64]bool{}
	for _, v := range cur.Pinned {
		m[v] = true
	}
	for _, v := range versions {
		m[v] = true
	}
	cur.Pinned = cur.Pinned[:0]
	for v := range m {
		cur.Pinned = append(cur.Pinned, v)
	}
	return t.SetRetentionPolicy(ctx, cur)
}

// UnpinVersions 从长期保留集合移除版本（不影响最近 N 个版本的保留）。
func (t *Table) UnpinVersions(ctx context.Context, versions ...int64) error {
	cur := t.RetentionPolicy()
	drop := map[int64]bool{}
	for _, v := range versions {
		drop[v] = true
	}
	kept := cur.Pinned[:0]
	for _, v := range cur.Pinned {
		if !drop[v] {
			kept = append(kept, v)
		}
	}
	cur.Pinned = kept
	return t.SetRetentionPolicy(ctx, cur)
}

// PurgeResult 汇报一次清理删除的内容。
type PurgeResult struct {
	ExpiredVersions  []int64 // 本次判定为超期且未长期保留的版本
	RemovedFiles     int     // 实际释放的数据/sidecar 文件数
	RemovedSnapshots int     // 删除的快照清单文件数
	RemovedSchemas   int     // 删除的、不再被任何存活快照引用的 schema 文件数
}

// PurgeExpired 按保留策略清理旧版本并真正释放数据文件。
//
// 清理规则：
//   - 当前版本永远保留，永远不会被动；
//   - RetainVersions>0 时保留最近 N 个版本；
//   - Pinned 中的版本长期保留；
//   - 其余旧版本的快照清单被删除，其独占的数据/sidecar 文件被物理释放；
//   - 任何仍被存活快照引用的数据文件一律不删。
//
// 崩溃安全：版本先原子登记为 EXPIRED，再物理删除文件；中途被杀后
// 重新 Open 会自动续完清理。读取行为始终是“版本完整可读”或
// “明确 SNAPSHOT_EXPIRED”二者之一，绝不出现半截状态。
func (t *Table) PurgeExpired(ctx context.Context) (PurgeResult, error) {
	_ = ctx
	res := PurgeResult{}
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := tryAcquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return res, err
	}
	defer lock.release()

	if err := t.reloadStateLocked(); err != nil {
		return res, err
	}
	err = t.finishPurgeLockedHoldingMu(&res, true)
	return res, err
}

// finishPurgeLockedHoldingMu 在已持有提交锁与 t.mu 的前提下执行/续完清理。
func (t *Table) finishPurgeLockedHoldingMu(res *PurgeResult, allowHook bool) error {
	current := t.meta.CurrentVersion
	policy := t.retention.Policy.clone()
	expired := map[int64]bool{}
	for _, v := range t.retention.Expired {
		expired[v] = true
	}

	// 0) 枚举磁盘现存快照。
	existing := existingSnapshotVersions(t.dir)

	// 1) 存活版本集合：当前版本 + 最近 N 个 + pinned。
	live := map[int64]bool{current: true}
	if policy.RetainVersions == 0 && len(policy.Pinned) == 0 {
		// 默认策略：历史全部保留（显式 RetainVersions=0 且无 pin 即默认）。
		for _, v := range existing {
			if v <= current {
				live[v] = true
			}
		}
	} else if policy.RetainVersions > 0 {
		lo := current - int64(policy.RetainVersions) + 1
		if lo < 1 {
			lo = 1
		}
		for v := lo; v <= current; v++ {
			live[v] = true
		}
	}
	for _, v := range policy.Pinned {
		if v >= 1 && v <= current {
			live[v] = true
		}
	}

	// 2) 确定本次新过期的版本。
	toExpire := []int64{}
	for _, v := range existing {
		if v <= current && !live[v] && !expired[v] {
			toExpire = append(toExpire, v)
		}
	}

	// 3) 崩溃安全关键点：先原子登记过期，再删任何文件。
	if len(toExpire) > 0 {
		t.retention.Expired = append(t.retention.Expired, toExpire...)
		sort.Slice(t.retention.Expired, func(i, j int) bool {
			return t.retention.Expired[i] < t.retention.Expired[j]
		})
		err := writeJSONAtomic(t.retentionPath(), t.retention)
		if err != nil {
			return err
		}
		if allowHook && t.hook != nil {
			if err := t.hook("purge-after-expire-mark"); err != nil {
				return err
			}
		}
	}
	res.ExpiredVersions = append(res.ExpiredVersions, toExpire...)

	// 4) 按磁盘现存快照计算引用：过期快照不贡献任何引用。
	referencedFiles := map[string]bool{}
	liveSchema := map[int64]bool{}
	expiredOnDisk := []int64{}
	for _, v := range existing {
		snap := &SnapshotMeta{}
		if err := readJSON(t.snapshotPath(v), snap); err != nil {
			if errIsNotExist(err) {
				continue
			}
			return err
		}
		// 过期登记是权威的：续跑时已登记快照直接当作过期处理。
		if expired[v] {
			expiredOnDisk = append(expiredOnDisk, v)
			continue
		}
		liveSchema[snap.SchemaID] = true
		for _, df := range snap.Files {
			referencedFiles[df.Path] = true
			if df.DeleteSidecar != "" {
				referencedFiles[df.DeleteSidecar] = true
			}
		}
	}

	// 5) 物理释放不被任何存活快照引用的数据文件。
	dataDir := filepath.Join(t.dir, "data")
	if entries, err := os.ReadDir(dataDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			rel := filepath.Join("data", e.Name())
			if !referencedFiles[rel] {
				if err := os.Remove(filepath.Join(dataDir, e.Name())); err == nil {
					res.RemovedFiles++
				} else if !errIsNotExist(err) {
					return err
				}
			}
		}
	} else if !errIsNotExist(err) {
		return err
	}
	if allowHook && t.hook != nil {
		if err := t.hook("purge-after-data-delete"); err != nil {
			return err
		}
	}

	// 6) 删除过期快照清单，再删除无存活引用的 schema 文件。
	sort.Slice(expiredOnDisk, func(i, j int) bool { return expiredOnDisk[i] < expiredOnDisk[j] })
	for _, v := range expiredOnDisk {
		if err := os.Remove(t.snapshotPath(v)); err == nil {
			res.RemovedSnapshots++
		} else if !errIsNotExist(err) {
			return err
		}
	}
	schemaMatches, _ := filepath.Glob(filepath.Join(t.dir, "metadata", "schema-*.json"))
	for _, m := range schemaMatches {
		var sid int64
		if _, err := fmt.Sscanf(filepath.Base(m), "schema-%d.json", &sid); err != nil {
			continue
		}
		if !liveSchema[sid] {
			if err := os.Remove(m); err == nil {
				res.RemovedSchemas++
			} else if !errIsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func existingSnapshotVersions(dir string) []int64 {
	matches, _ := filepath.Glob(filepath.Join(dir, "metadata", "snap-*.json"))
	out := make([]int64, 0, len(matches))
	for _, m := range matches {
		var v int64
		if _, err := fmt.Sscanf(filepath.Base(m), "snap-%d.json", &v); err == nil {
			out = append(out, v)
		}
	}
	return out
}

func containsInt64(xs []int64, x int64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// pendingPurgeAfterOpen 报告是否存在“已登记过期、但清理未完成”的状态：
// 任一过期版本的快照清单仍在磁盘上，即清理在数据删除阶段/快照删除前被杀。
func (t *Table) pendingPurgeAfterOpen() (bool, error) {
	for _, v := range t.retention.Expired {
		if _, err := os.Stat(t.snapshotPath(v)); err == nil {
			return true, nil
		} else if !errIsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}
