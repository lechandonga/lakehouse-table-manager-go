package lakehouse

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Compact 合并当前最新快照：把所有活行（去重、剔除墓碑）重写为单个新文件，
// 生成 op=compact 的新快照。新清单只含新文件，旧文件随可达性回收。
// 规则保证：
//   - 同一主键只保留按“文件路径升序、文件内后者胜”归并后的唯一活行；
//   - 墓碑随重写丢弃（被删行不再出现，效果不变）；
//   - 结构与每行取值不变；旧快照仍指向旧文件，历史读取逐位一致。
//
// 若合并期间有其他确认提交推进了快照，返回 ErrConflictConcurrentCommit。
func (t *Table) Compact() (*SnapshotView, error) {
	lock, err := acquireTableLock(t.root)
	if err != nil {
		return nil, err
	}
	defer lock.release()

	_, fresh, err := loadLatest(t.root)
	if err != nil {
		return nil, err
	}
	base := &SnapshotView{root: t.root, snap: fresh}
	merged, err := base.readMerged()
	if err != nil {
		return nil, err
	}

	newVersion := fresh.Version + 1
	name := fmt.Sprintf("%010d-compact-000000.jsonl", newVersion)
	txnPath := filepath.Join(t.root, dirTxn, name)
	finalPath := filepath.Join(t.root, dirData, name)

	keys := make([]string, 0, len(merged))
	rowCount := 0
	for k, mr := range merged {
		if mr.tomb {
			continue
		}
		keys = append(keys, k)
		rowCount++
	}
	sort.Strings(keys)

	f, err := os.OpenFile(txnPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	for _, k := range keys {
		rec := Record{Values: merged[k].values}
		if err := enc.Encode(rec); err != nil {
			f.Close()
			return nil, err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(txnPath, finalPath); err != nil {
		return nil, err
	}
	if err := fsyncDir(filepath.Join(t.root, dirData)); err != nil {
		return nil, err
	}

	entry := ManifestEntry{Path: name}
	if rowCount > 0 {
		entry.MinPK = keys[0]
		entry.MaxPK = keys[len(keys)-1]
	}
	entry.Rows = rowCount

	files := []ManifestEntry{}
	if rowCount > 0 {
		files = []ManifestEntry{entry}
	}
	snap := &Snapshot{
		Version:    newVersion,
		Parent:     fresh.Version,
		Schema:     fresh.Schema,
		Files:      files,
		Operation:  "compact",
		CommitTime: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeJSONFileAtomic(snapPath(t.root, newVersion), snap); err != nil {
		return nil, err
	}
	checksum, err := checksumFile(snapPath(t.root, newVersion))
	if err != nil {
		return nil, err
	}
	ptr := Latest{Version: newVersion, Path: fmt.Sprintf("snap-%010d.json", newVersion), Checksum: checksum}
	if err := writeJSONFileAtomic(latestPath(t.root), ptr); err != nil {
		return nil, err
	}
	if err := fsyncDir(filepath.Join(t.root, dirMetadata)); err != nil {
		return nil, err
	}
	t.latest = snap
	return &SnapshotView{root: t.root, snap: snap}, nil
}
