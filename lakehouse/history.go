package lakehouse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// RetentionPolicy 是历史版本保留策略，落盘持久化，重开后继续生效。
//   - KeepVersions <= 0（默认）：保留全部历史版本；
//   - KeepVersions == N > 0：始终保留最近 N 个“仍存在”的版本；
//   - Pinned 中的版本无论多老都长期保留；当前版本永远保留。
//
// 只有同时满足“超出最近 N 个版本”且“未被 pin”的旧版本才会被清理。
type RetentionPolicy struct {
	KeepVersions int     `json:"keep_versions"`
	Pinned       []int64 `json:"pinned,omitempty"`
}

// HistoryMeta 是版本生命周期的落盘清单（metadata/history.json）。
// 它与 table.json 一样只在持有提交锁时更新，发布走临时文件+原子 rename。
type HistoryMeta struct {
	// NextVersion 是版本高水位（已用过的最大版本号 +1）。
	// 版本号单调递增、永不回收重用，恢复/清理都不例外。
	NextVersion int64 `json:"next_version"`
	// Existing 是当前仍可读的版本集合；初始空快照 v1 也在其中。
	Existing []int64 `json:"existing"`
	// Expired 是曾存在、后被保留策略清理掉的版本集合，
	// 用于把“已被清理”和“压根不存在”区分成两个错误码。
	Expired []int64 `json:"expired,omitempty"`
	// Policy 是持久化的保留策略。
	Policy RetentionPolicy `json:"policy"`
}

func (h *HistoryMeta) sortedExisting() []int64 {
	out := append([]int64(nil), h.Existing...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (h *HistoryMeta) contains(v int64) bool {
	for _, x := range h.Existing {
		if x == v {
			return true
		}
	}
	return false
}

func (h *HistoryMeta) containsExpired(v int64) bool {
	for _, x := range h.Expired {
		if x == v {
			return true
		}
	}
	return false
}

func (h *HistoryMeta) pinnedSet() map[int64]bool {
	m := make(map[int64]bool, len(h.Policy.Pinned))
	for _, v := range h.Policy.Pinned {
		m[v] = true
	}
	return m
}

// removableVersions 按策略计算可清理版本：
// 超出最近 N 个现存版本、未被 pin、不是当前版本。N<=0 时不清理任何版本。
func (h *HistoryMeta) removableVersions(currentVersion int64) []int64 {
	if h.Policy.KeepVersions <= 0 {
		return nil
	}
	existing := h.sortedExisting()
	pinned := h.pinnedSet()
	cut := len(existing) - h.Policy.KeepVersions
	if cut <= 0 {
		return nil
	}
	var out []int64
	for _, v := range existing[:cut] {
		if v == currentVersion || pinned[v] {
			continue
		}
		out = append(out, v)
	}
	return out
}

// ExpireReport 是一次清理的结果。
type ExpireReport struct {
	ExpiredVersions []int64
	RemovedFiles    []string
	// MetadataRemoved 为 true 且 ExpiredVersions 为空时，表示清单早已翻转、
	// 本次只完成了上次中断后的物理文件续删。
	MetadataRemoved bool
}

func (t *Table) historyPath() string { return filepath.Join(t.dir, "metadata", "history.json") }

// loadHistoryLocked 读取 history.json；升级前的老表没有该文件时，
// 以磁盘上实际存在的 snap-<v>.json 引导一份，保证平滑升级、不丢任何历史。
// 调用方必须已持有提交锁。
func (t *Table) loadHistoryLocked() (*HistoryMeta, error) {
	h := &HistoryMeta{}
	if err := readJSON(t.historyPath(), h); err != nil {
		if !errIsNotExist(err) {
			return nil, err
		}
		matches, gerr := filepath.Glob(filepath.Join(t.dir, "metadata", "snap-*.json"))
		if gerr != nil {
			return nil, gerr
		}
		var maxV int64
		for _, m := range matches {
			var v int64
			if n, e := fmt.Sscanf(filepath.Base(m), "snap-%d.json", &v); e == nil && n == 1 && v > maxV {
				maxV = v
			}
		}
		h = &HistoryMeta{NextVersion: maxV + 1}
		for v := int64(1); v <= maxV; v++ {
			h.Existing = append(h.Existing, v)
		}
	}
	return h, nil
}

// RetentionPolicyView 返回当前持久化的保留策略。
func (t *Table) RetentionPolicyView(ctx context.Context) (RetentionPolicy, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return RetentionPolicy{}, err
	}
	defer lock.release()
	if err := t.refreshLocked(); err != nil {
		return RetentionPolicy{}, err
	}
	h, err := t.loadHistoryLocked()
	if err != nil {
		return RetentionPolicy{}, err
	}
	return h.Policy, nil
}

// SetRetentionPolicy 配置保留策略并落盘（策略本身不是数据提交，不产生新版本）。
// 实际清理发生在随后的 ExpireOldVersions；keepVersions 为负数非法。
func (t *Table) SetRetentionPolicy(ctx context.Context, baseVersion int64, p RetentionPolicy) error {
	_ = ctx
	if p.KeepVersions < 0 {
		return newError(CodeRetentionInvalid, "keep_versions must be >= 0 (0 means keep all)")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return err
	}
	defer lock.release()
	if err := t.refreshLocked(); err != nil {
		return err
	}
	if baseVersion != t.meta.CurrentVersion {
		return newError(CodeConflictSnapshotStale,
			"base version %d is stale (current %d)", baseVersion, t.meta.CurrentVersion)
	}
	h, err := t.loadHistoryLocked()
	if err != nil {
		return err
	}
	// 只改保留窗口大小；pin 集合是独立的长期保留标记，必须原样保留。
	pins := append([]int64(nil), h.Policy.Pinned...)
	if len(p.Pinned) > 0 {
		pins = append(pins, p.Pinned...)
	}
	h.Policy = RetentionPolicy{
		KeepVersions: p.KeepVersions,
		Pinned:       sortedDedupInt64(pins),
	}
	return writeJSONAtomic(t.historyPath(), h)
}

// PinVersions 把指定版本标记为长期保留（即使超出最近 N 个版本也不清理）。
// 版本不存在返回 SNAPSHOT_NOT_FOUND，已被清理返回 SNAPSHOT_EXPIRED。
func (t *Table) PinVersions(ctx context.Context, versions ...int64) error {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return err
	}
	defer lock.release()
	if err := t.refreshLocked(); err != nil {
		return err
	}
	h, err := t.loadHistoryLocked()
	if err != nil {
		return err
	}
	pinned := h.pinnedSet()
	for _, v := range versions {
		switch {
		case h.contains(v):
			pinned[v] = true
		case h.containsExpired(v):
			return newError(CodeSnapshotExpired, "cannot pin expired version %d", v)
		default:
			return newError(CodeSnapshotNotFound, "snapshot version %d does not exist", v)
		}
	}
	h.Policy.Pinned = mapKeysSorted(pinned)
	return writeJSONAtomic(t.historyPath(), h)
}

// UnpinVersions 取消长期保留标记；对未 pin 的版本是幂等无操作。
// 版本压根不存在仍返回 SNAPSHOT_NOT_FOUND。
func (t *Table) UnpinVersions(ctx context.Context, versions ...int64) error {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return err
	}
	defer lock.release()
	if err := t.refreshLocked(); err != nil {
		return err
	}
	h, err := t.loadHistoryLocked()
	if err != nil {
		return err
	}
	pinned := h.pinnedSet()
	for _, v := range versions {
		switch {
		case h.contains(v), h.containsExpired(v):
			delete(pinned, v)
		default:
			return newError(CodeSnapshotNotFound, "snapshot version %d does not exist", v)
		}
	}
	h.Policy.Pinned = mapKeysSorted(pinned)
	return writeJSONAtomic(t.historyPath(), h)
}

// ExpireOldVersions 按落盘策略清理旧版本：
//   - 只清理“超出最近 N 个现存版本且未 pin 且非当前”的版本；
//   - 任何仍保留版本依赖的数据/sidecar/schema 文件一律不删；
//   - 当前版本永远不动；
//   - 清单翻转（history.json 原子发布）先于物理文件删除，
//     中断后重新打开再调用本方法会续删，绝无“读半截”的版本。
func (t *Table) ExpireOldVersions(ctx context.Context, baseVersion int64) (*ExpireReport, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return nil, err
	}
	defer lock.release()
	if err := t.refreshLocked(); err != nil {
		return nil, err
	}
	if baseVersion != t.meta.CurrentVersion {
		return nil, newError(CodeConflictSnapshotStale,
			"base version %d is stale (current %d)", baseVersion, t.meta.CurrentVersion)
	}

	h, err := t.loadHistoryLocked()
	if err != nil {
		return nil, err
	}

	// 续删：历史上中断的清理（版本已标记过期、文件可能还在）也要把空间释放掉。
	report, err := t.deleteExpiredFilesLocked(h)
	if err != nil {
		return nil, err
	}

	victims := h.removableVersions(t.meta.CurrentVersion)
	if len(victims) == 0 {
		return report, nil
	}
	victimSet := map[int64]bool{}
	for _, v := range victims {
		victimSet[v] = true
	}

	// 清理后仍保留版本引用的物理文件集合 —— 删除保护集合，
	// 任何被其他版本（含恢复产生的新快照）共享的文件都不会被删。
	protected := t.collectReferencedFilesLocked(h, victimSet)

	var filesToRemove []string
	for _, v := range victims {
		snap := &SnapshotMeta{}
		if err := readJSON(t.snapshotPath(v), snap); err != nil {
			return nil, err
		}
		for _, df := range snap.Files {
			if !protected[df.Path] {
				filesToRemove = append(filesToRemove, df.Path)
			}
			if df.DeleteSidecar != "" && !protected[df.DeleteSidecar] {
				filesToRemove = append(filesToRemove, df.DeleteSidecar)
			}
		}
	}
	filesToRemove = sortedDedupString(filesToRemove)
	sort.Strings(filesToRemove)

	// 阶段1：原子翻转清单 —— 受害者立刻变为“已过期”，读取得到 SNAPSHOT_EXPIRED。
	remaining := make([]int64, 0, len(h.Existing)-len(victims))
	for _, v := range h.Existing {
		if !victimSet[v] {
			remaining = append(remaining, v)
		}
	}
	h.Existing = remaining
	h.Expired = sortedDedupInt64(append(h.Expired, victims...))
	if err := writeJSONAtomic(t.historyPath(), h); err != nil {
		return nil, err
	}
	if t.hook != nil {
		if err := t.hook("after-expire-manifest"); err != nil {
			return nil, err
		}
	}

	// 阶段2：删除快照/schema 元数据与独占数据文件。此后中断也安全：
	// 清单已是最终状态，重开后续删即可。
	keepSchema := t.collectUsedSchemaIDsLocked(h.Existing)
	for _, v := range victims {
		snap := &SnapshotMeta{}
		if err := readJSON(t.snapshotPath(v), snap); err != nil {
			return nil, err
		}
		if !keepSchema[snap.SchemaID] {
			_ = os.Remove(t.schemaPath(snap.SchemaID))
		}
		_ = os.Remove(t.snapshotPath(v))
	}
	for _, rel := range filesToRemove {
		if err := os.Remove(filepath.Join(t.dir, rel)); err == nil {
			report.RemovedFiles = append(report.RemovedFiles, rel)
		} else if !errIsNotExist(err) {
			return nil, err
		}
	}
	sort.Strings(report.RemovedFiles)
	report.ExpiredVersions = victims
	return report, nil
}

// deleteExpiredFilesLocked 续删“清单已标记过期、物理文件仍残留”的文件，
// 用于清理中断后重开的恢复。
func (t *Table) deleteExpiredFilesLocked(h *HistoryMeta) (*ExpireReport, error) {
	report := &ExpireReport{}
	if len(h.Expired) == 0 {
		return report, nil
	}
	expired := map[int64]bool{}
	for _, v := range h.Expired {
		expired[v] = true
	}
	protected := t.collectReferencedFilesLocked(h, nil)
	keepSchema := t.collectUsedSchemaIDsLocked(h.Existing)
	removedAny := false
	for v := range expired {
		snap := &SnapshotMeta{}
		if err := readJSON(t.snapshotPath(v), snap); err != nil {
			if errIsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, df := range snap.Files {
			if !protected[df.Path] {
				if removeIfExists(filepath.Join(t.dir, df.Path)) {
					report.RemovedFiles = append(report.RemovedFiles, df.Path)
					removedAny = true
				}
			}
			if df.DeleteSidecar != "" && !protected[df.DeleteSidecar] {
				if removeIfExists(filepath.Join(t.dir, df.DeleteSidecar)) {
					report.RemovedFiles = append(report.RemovedFiles, df.DeleteSidecar)
					removedAny = true
				}
			}
		}
		if !keepSchema[snap.SchemaID] {
			removeIfExists(t.schemaPath(snap.SchemaID))
		}
		if removeIfExists(t.snapshotPath(v)) {
			removedAny = true
		}
	}
	report.MetadataRemoved = removedAny
	sort.Strings(report.RemovedFiles)
	return report, nil
}

// collectReferencedFilesLocked 统计“除 skip 版本外所有现存版本”引用的
// 数据文件与 sidecar 集合（相对路径）。
func (t *Table) collectReferencedFilesLocked(h *HistoryMeta, skip map[int64]bool) map[string]bool {
	ref := map[string]bool{}
	for _, v := range h.Existing {
		if skip[v] {
			continue
		}
		snap := &SnapshotMeta{}
		if err := readJSON(t.snapshotPath(v), snap); err != nil {
			continue
		}
		for _, df := range snap.Files {
			ref[df.Path] = true
			if df.DeleteSidecar != "" {
				ref[df.DeleteSidecar] = true
			}
		}
	}
	return ref
}

// collectUsedSchemaIDsLocked 返回现存版本仍在使用的 schema ID 集合。
func (t *Table) collectUsedSchemaIDsLocked(versions []int64) map[int64]bool {
	used := map[int64]bool{}
	for _, v := range versions {
		snap := &SnapshotMeta{}
		if err := readJSON(t.snapshotPath(v), snap); err != nil {
			continue
		}
		used[snap.SchemaID] = true
	}
	return used
}

func removeIfExists(path string) bool {
	if err := os.Remove(path); err == nil {
		return true
	}
	return false
}
