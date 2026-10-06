package lakehouse

import (
	"context"
	"path/filepath"
	"sort"
)

// RestoreToVersion 把表恢复到 targetVersion 当时的样子，并作为一次新的提交发布：
//   - 恢复后当前表的可见列与数据与“读 targetVersion”逐行一致；
//   - 不修改任何历史版本：targetVersion 及恢复前最新版本之后仍可原样读回；
//   - 新版本号在高水位上继续递增，永不回收重用；
//   - 目标版本压根不存在返回 SNAPSHOT_NOT_FOUND，已被清理返回 SNAPSHOT_EXPIRED；
//   - 基版本过期返回 CONFLICT_SNAPSHOT_STALE，与其他改表操作互斥；
//   - 中途失败/被杀：重启后要么仍是恢复前版本，要么是完整恢复结果。
func (t *Table) RestoreToVersion(ctx context.Context, baseVersion, targetVersion int64, message string) (int64, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := acquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return 0, err
	}
	defer lock.release()

	if err := t.refreshLocked(); err != nil {
		return 0, err
	}
	h, err := t.loadHistoryLocked()
	if err != nil {
		return 0, err
	}
	switch {
	case h.containsExpired(targetVersion):
		return 0, newError(CodeSnapshotExpired,
			"snapshot version %d has been expired by retention policy and cannot be restored", targetVersion)
	case !h.contains(targetVersion):
		return 0, newError(CodeSnapshotNotFound, "snapshot version %d does not exist", targetVersion)
	}
	if baseVersion != t.meta.CurrentVersion {
		return 0, newError(CodeConflictSnapshotStale,
			"base version %d is stale (current %d)", baseVersion, t.meta.CurrentVersion)
	}

	target := &SnapshotMeta{}
	if err := readJSON(t.snapshotPath(targetVersion), target); err != nil {
		return 0, err
	}
	targetSchema := &Schema{}
	if err := readJSON(t.schemaPath(target.SchemaID), targetSchema); err != nil {
		return 0, err
	}

	newV := h.NextVersion
	// schema 内容克隆为全新 schema 版本：恢复快照绑定“当下”的 schema 版本，
	// 但列 ID 完全沿用，物理文件按 ID 对齐，读取逐行一致。
	newSchemaV := t.meta.SchemaVersion + 1
	files := append([]DataFile(nil), target.Files...)

	if message == "" {
		message = "restore to version " + itoa(targetVersion)
	}
	snap := &SnapshotMeta{
		Version:       newV,
		ParentVersion: t.meta.CurrentVersion,
		SchemaID:      newSchemaV,
		Files:         files,
		Operation:     "restore",
		Message:       message,
		RestoredFrom:  targetVersion,
	}

	// 恢复不产生任何新数据文件（复用目标版本的不可变文件清单），
	// 发布顺序与普通提交一致：schema -> 快照 -> 指针，唯一可见性切换点是 table.json。
	if err := writeJSONAtomic(t.schemaPath(newSchemaV), targetSchema); err != nil {
		return 0, err
	}
	if t.hook != nil {
		if err := t.hook("after-restore-schema"); err != nil {
			return 0, err
		}
	}
	if err := writeJSONAtomic(t.snapshotPath(newV), snap); err != nil {
		return 0, err
	}
	if t.hook != nil {
		if err := t.hook("after-restore-snapshot"); err != nil {
			return 0, err
		}
	}

	t.meta.CurrentVersion = newV
	t.meta.SchemaVersion = newSchemaV
	if err := writeJSONAtomic(t.metaPath(), t.meta); err != nil {
		return 0, err
	}

	h.NextVersion = newV + 1
	h.Existing = sortedDedupInt64(append(h.Existing, newV))
	if err := writeJSONAtomic(t.historyPath(), h); err != nil {
		// table.json 已翻转，恢复结果已可见；history 只影响后续清理，
		// 重开引导会把新版本补登，不影响读取完整性。
		return newV, nil
	}

	t.schemas[newSchemaV] = targetSchema
	t.current = snap
	return newV, nil
}

// RestoredFrom 返回某快照是否由恢复产生及其来源版本；非恢复快照返回 (0,false)。
func (t *Table) RestoredFrom(version int64) (int64, bool) {
	snap := &SnapshotMeta{}
	if err := readJSON(t.snapshotPath(version), snap); err != nil {
		return 0, false
	}
	if snap.Operation == "restore" {
		return snap.RestoredFrom, true
	}
	return 0, false
}

func sortedDedupInt64(xs []int64) []int64 {
	seen := map[int64]bool{}
	out := make([]int64, 0, len(xs))
	for _, v := range xs {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedDedupString(xs []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(xs))
	for _, v := range xs {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

func mapKeysSorted(m map[int64]bool) []int64 {
	out := make([]int64, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
