package lakehouse

import (
	"context"
	"path/filepath"
)

// Restore 把表恢复到某个仍然存在的历史版本，并把恢复本身作为一次新提交发布。
//
// 语义：
//   - 恢复后直接读当前表（Head/Scan/Get）与读 targetVersion 逐行一致，
//     包括当时可见的列集合（schema 视图也回到目标版本绑定的 schema）；
//   - 恢复是一次新的提交：版本号继续递增、不回收重用；旧快照一律不改写，
//     恢复前的最新状态之后仍可按其版本号读回；
//   - 崩溃安全：沿用“数据文件（无）-> 快照元数据 -> current 指针原子翻转”
//     协议。current 翻转前被杀则恢复完全不可见；翻转后即为完整恢复结果，
//     不存在恢复了一半的表状态；
//   - targetVersion 从未存在返回 SNAPSHOT_NOT_FOUND；
//     targetVersion 已被保留策略清理返回 SNAPSHOT_EXPIRED。
//
// 与所有写操作共用提交锁和 CAS：baseVersion 过期返回
// CONFLICT_SNAPSHOT_STALE，锁被占用返回 CONFLICT_IN_PROGRESS。
func (t *Table) Restore(ctx context.Context, baseVersion, targetVersion int64, message string) (int64, error) {
	_ = ctx
	// 先在锁外给出目标版本可分辨的失败原因（状态在锁内还会复核一次）。
	t.mu.Lock()
	preErr := t.checkRestoreTargetLocked(targetVersion)
	t.mu.Unlock()
	if preErr != nil {
		return 0, preErr
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := tryAcquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return 0, err
	}
	defer lock.release()

	if err := t.reloadStateLocked(); err != nil {
		return 0, err
	}
	if baseVersion != t.meta.CurrentVersion {
		return 0, newError(CodeConflictSnapshotStale,
			"base version %d is stale (current %d)", baseVersion, t.meta.CurrentVersion)
	}
	if err := t.checkRestoreTargetLocked(targetVersion); err != nil {
		return 0, err
	}

	target := &SnapshotMeta{}
	if err := readJSON(t.snapshotPath(targetVersion), target); err != nil {
		return 0, err
	}
	targetSchema := &Schema{}
	if err := readJSON(t.schemaPath(target.SchemaID), targetSchema); err != nil {
		return 0, err
	}

	newV := t.meta.CurrentVersion + 1
	snap := &SnapshotMeta{
		Version:       newV,
		ParentVersion: t.meta.CurrentVersion,
		// 直接复用目标版本的文件清单与 schema：不复制任何数据文件，
		// 因此恢复不产生新的数据占用，也绝不改变旧版本依赖的文件。
		SchemaID:  target.SchemaID,
		Files:     append([]DataFile(nil), target.Files...),
		Operation: "restore",
		Message:   message,
	}

	// 阶段1：写恢复快照元数据（尚不可见）。
	if err := writeJSONAtomic(t.snapshotPath(newV), snap); err != nil {
		return 0, err
	}
	if t.hook != nil {
		if err := t.hook("restore-after-snapshot-meta"); err != nil {
			return 0, err
		}
	}

	// 阶段2：原子翻转 current 指针——唯一可见性切换点。
	t.meta.CurrentVersion = newV
	t.meta.SchemaVersion = target.SchemaID
	if err := writeJSONAtomic(t.metaPath(), t.meta); err != nil {
		return 0, err
	}
	if t.hook != nil {
		if err := t.hook("restore-after-current-publish"); err != nil {
			return 0, err
		}
	}

	t.current = snap
	t.schemas[target.SchemaID] = targetSchema
	return newV, nil
}

// checkRestoreTargetLocked 要求调用方持有 t.mu。
func (t *Table) checkRestoreTargetLocked(targetVersion int64) error {
	current := t.meta.CurrentVersion
	expired := containsInt64(t.retention.Expired, targetVersion)
	switch {
	case targetVersion < 1 || targetVersion > current:
		return newError(CodeSnapshotNotFound,
			"snapshot version %d does not exist (current %d)", targetVersion, current)
	case expired:
		return newError(CodeSnapshotExpired,
			"snapshot version %d has been purged by retention policy and cannot be restored", targetVersion)
	}
	return nil
}
