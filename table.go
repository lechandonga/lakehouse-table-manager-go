package lakehouse

import (
	"fmt"
	"os"
	"path/filepath"
)

// Table 是本地湖仓表句柄。提交锁只在 Open 恢复与 Commit 发布期间短暂持有；
// 查询走快照对象本身，因此任何读到的快照都完整自洽。
type Table struct {
	root   string
	latest *Snapshot // 打开时缓存的最新快照；提交后同步刷新
}

// Create 在空目录上建表并写入 version=1 的初始快照（空数据 + 初始结构）。
func Create(root string, schema *Schema) (*Table, error) {
	if schema == nil {
		return nil, fmt.Errorf("lakehouse: nil schema")
	}
	if err := schema.validate(); err != nil {
		return nil, err
	}
	for _, d := range []string{dirMetadata, dirData, dirTxn} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(latestPath(root)); err == nil {
		return nil, fmt.Errorf("lakehouse: table already exists at %s", root)
	}

	lock, err := acquireTableLock(root)
	if err != nil {
		return nil, err
	}
	t := &Table{root: root}
	snap := &Snapshot{Version: 1, Schema: schema, Files: nil, Operation: "create"}
	if err := t.publishSnapshot(snap); err != nil {
		lock.release()
		return nil, err
	}
	if err := fsyncDir(filepath.Join(root, dirMetadata)); err != nil {
		lock.release()
		return nil, err
	}
	lock.release()
	t.latest = snap
	return t, nil
}

// Open 打开已存在的表：执行崩溃恢复（清理未确认提交），读取最新快照。
func Open(root string) (*Table, error) {
	lock, err := acquireTableLock(root)
	if err != nil {
		return nil, err
	}
	t := &Table{root: root}

	// 崩溃恢复：删除 txn/ 暂存，并沿 latest 父链回收不可达快照/数据文件。
	if err := t.recover(); err != nil {
		lock.release()
		return nil, err
	}
	_, snap, err := loadLatest(root)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("lakehouse: no table at %s", root)
		}
		lock.release()
		return nil, err
	}
	lock.release()
	t.latest = snap
	return t, nil
}

// Close 关闭句柄（提交锁仅在提交期间短暂持有）。
func (t *Table) Close() error { return nil }

// LatestVersion 返回当前最新已确认版本号。
func (t *Table) LatestVersion() int64 { return t.latest.Version }

// Latest 返回最新快照的只读视图。
func (t *Table) Latest() *SnapshotView { return t.Snapshot(t.latest.Version) }

// Snapshot 打开任意历史版本读取。历史快照自带提交当时的结构与清单。
func (t *Table) Snapshot(version int64) *SnapshotView {
	s, err := loadSnapshot(t.root, version)
	if err != nil {
		return &SnapshotView{err: err}
	}
	return &SnapshotView{root: t.root, snap: s}
}

// publishSnapshot 写不可变快照文件并原子切换 latest 指针。
func (t *Table) publishSnapshot(snap *Snapshot) error {
	snapFile := snapPath(t.root, snap.Version)
	if err := writeJSONFileAtomic(snapFile, snap); err != nil {
		return err
	}
	checksum, err := checksumFile(snapFile)
	if err != nil {
		return err
	}
	ptr := Latest{
		Version:  snap.Version,
		Path:     filepath.Base(snapFile),
		Checksum: checksum,
	}
	return writeJSONFileAtomic(latestPath(t.root), ptr)
}

func (t *Table) recover() error {
	txnDir := filepath.Join(t.root, dirTxn)
	entries, err := os.ReadDir(txnDir)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(txnDir, 0o755); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(txnDir, e.Name())); err != nil {
			return err
		}
	}
	metaDir := filepath.Join(t.root, dirMetadata)
	if metaEntries, rerr := os.ReadDir(metaDir); rerr == nil {
		for _, e := range metaEntries {
			if filepath.Ext(e.Name()) == ".tmp" {
				_ = os.Remove(filepath.Join(metaDir, e.Name()))
			}
		}
	}
	return t.garbageCollect()
}

// garbageCollect 从最新快照沿 parent 链确定“可达”快照与数据文件并删除其余内容。
// 覆盖 data-renamed（数据已落地但无快照引用）与 snapshot-written
// （快照已写但 latest 未切换）两个提交中断点；任何历史快照清单指向的
// 文件都保留，因此历史读取结果不变。
func (t *Table) garbageCollect() error {
	_, latest, err := loadLatest(t.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 建表前
		}
		return err
	}

	reachableSnaps := map[int64]bool{}
	liveData := map[string]bool{}
	for cur := latest; cur != nil; {
		reachableSnaps[cur.Version] = true
		for _, f := range cur.Files {
			liveData[filepath.Base(f.Path)] = true
		}
		if cur.Parent == 0 {
			break
		}
		next, err := loadSnapshot(t.root, cur.Parent)
		if err != nil {
			return fmt.Errorf("%w: missing parent version %d", ErrTableCorrupted, cur.Parent)
		}
		cur = next
	}

	metaDir := filepath.Join(t.root, dirMetadata)
	if metaEntries, rerr := os.ReadDir(metaDir); rerr == nil {
		for _, e := range metaEntries {
			name := e.Name()
			if len(name) == 20 && name[:5] == "snap-" && filepath.Ext(name) == ".json" {
				n, err := parseSnapVersion(name[5 : len(name)-5])
				if err == nil && !reachableSnaps[n] {
					_ = os.Remove(filepath.Join(metaDir, name))
				}
			}
		}
	}

	dataDir := filepath.Join(t.root, dirData)
	if dataEntries, rerr := os.ReadDir(dataDir); rerr == nil {
		for _, e := range dataEntries {
			if !e.IsDir() && !liveData[e.Name()] {
				_ = os.Remove(filepath.Join(dataDir, e.Name()))
			}
		}
	}
	return nil
}

func parseSnapVersion(s string) (int64, error) {
	var n int64
	if _, err := fmt.Sscanf(s, "%010d", &n); err != nil {
		return 0, err
	}
	return n, nil
}
