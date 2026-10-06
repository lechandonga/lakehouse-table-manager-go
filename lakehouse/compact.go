package lakehouse

import (
	"context"
	"os"
	"path/filepath"
)

// Compact 对当前快照做小文件合并：
//   - 回放当前快照得到全部存活行（删除标记可靠生效，墓碑不落新文件）；
//   - 全部存活行写入一个新的不可变数据文件，生成新快照；
//   - 旧数据文件原样保留，历史快照清单不变，因此历史读取结果逐版本一致；
//   - 与普通提交共用提交锁与 CAS，过期 baseVersion 返回快照冲突。
func (t *Table) Compact(ctx context.Context, baseVersion int64) (int64, error) {
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
	if baseVersion != t.meta.CurrentVersion {
		return 0, newError(CodeConflictSnapshotStale,
			"base version %d is stale (current %d)", baseVersion, t.meta.CurrentVersion)
	}

	r := &Reader{t: t, snap: t.current, sch: t.schemas[t.meta.SchemaVersion]}
	current, keys, err := r.materialize()
	if err != nil {
		return 0, err
	}

	newV := h.NextVersion
	path := filepath.Join("data", "v-compact-"+itoa(newV)+"-"+uniqueSuffix()+".jsonl")
	rowsOut := make([]map[int]any, 0, len(keys))
	for _, k := range keys {
		rowsOut = append(rowsOut, map[int]any(current[k]))
	}
	if err := writeJSONL(filepath.Join(t.dir, path), rowsOut); err != nil {
		return 0, err
	}
	df := DataFile{Path: path, Records: len(rowsOut)}
	snap := &SnapshotMeta{
		Version:       newV,
		ParentVersion: t.meta.CurrentVersion,
		SchemaID:      t.meta.SchemaVersion,
		Files:         []DataFile{df},
		Operation:     "compact",
		Message:       "compaction into a single data file",
	}
	if err := writeJSONAtomic(t.snapshotPath(newV), snap); err != nil {
		return 0, err
	}
	t.meta.CurrentVersion = newV
	if err := writeJSONAtomic(t.metaPath(), t.meta); err != nil {
		return 0, err
	}
	h.NextVersion = newV + 1
	h.Existing = sortedDedupInt64(append(h.Existing, newV))
	if err := writeJSONAtomic(t.historyPath(), h); err != nil {
		return 0, err
	}
	t.current = snap
	return newV, nil
}

// GCUnreferencedFiles 回收未被任何已发布快照引用的数据文件
// （崩溃提交留下的孤儿文件）。任一历史快照引用的文件都不会被删除。
func (t *Table) GCUnreferencedFiles(ctx context.Context) (int, error) {
	_ = ctx
	t.mu.Lock()
	t.mu.Unlock()

	t.mu.Lock()
	lock, err := acquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		t.mu.Unlock()
		return 0, err
	}
	defer lock.release()
	if err := t.refreshLocked(); err != nil {
		t.mu.Unlock()
		return 0, err
	}
	h, err := t.loadHistoryLocked()
	t.mu.Unlock()
	if err != nil {
		return 0, err
	}
	referenced := map[string]bool{}
	for _, v := range h.Existing {
		snap := &SnapshotMeta{}
		if err := readJSON(t.snapshotPath(v), snap); err != nil {
			return 0, err
		}
		for _, df := range snap.Files {
			referenced[df.Path] = true
			if df.DeleteSidecar != "" {
				referenced[df.DeleteSidecar] = true
			}
		}
	}

	removed := 0
	dataDir := filepath.Join(t.dir, "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if errIsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		rel := filepath.Join("data", e.Name())
		if !referenced[rel] {
			if err := os.Remove(filepath.Join(dataDir, e.Name())); err == nil {
				removed++
			}
		}
	}
	return removed, nil
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
