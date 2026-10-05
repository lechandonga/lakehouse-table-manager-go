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

// ErrNothingToCommit 事务既无数据变更也无结构变更。
var ErrNothingToCommit = fmt.Errorf("lakehouse: nothing to commit")

// Txn 基于打开时的最新快照（baseVersion）。提交时若该快照已被
// 其他确认提交推进，Commit 返回 ErrConflictConcurrentCommit。
type Txn struct {
	table        *Table
	baseVersion  int64
	schema       *Schema // 事务内演进中的结构
	staged       map[string]*Record
	hasDeletes   bool
	schemaDirty  bool
	closed       bool
	crashAtStage string
}

// Begin 开启读写事务，快照基线为当前最新版本。
func (t *Table) Begin() *Txn {
	s := *t.latest.Schema
	cols := make([]Column, len(s.Columns))
	copy(cols, s.Columns)
	s.Columns = cols
	return &Txn{
		table:       t,
		baseVersion: t.latest.Version,
		schema:      &s,
		staged:      map[string]*Record{},
	}
}

// BaseVersion 返回事务所基于的快照版本（冲突排查可解释）。
func (tx *Txn) BaseVersion() int64 { return tx.baseVersion }

// AddColumn 加列。新列必须给出与类型匹配的缺省值；历史行读取时按该值填充。
// nullable=true 且 default==nil 表示缺省填充 NULL。
func (tx *Txn) AddColumn(name, typ string, nullable bool, def any) error {
	if tx.closed {
		return ErrTxnClosed
	}
	if _, ok := tx.schema.columnByName(name); ok {
		return fmt.Errorf("%w: column %q already exists", ErrColumnExists, name)
	}
	if !validType(typ) {
		return fmt.Errorf("%w: unknown type %q", ErrUnsupportedEvolution, typ)
	}
	col := Column{ID: tx.schema.nextID, Name: name, Type: typ, Nullable: nullable, Default: def}
	if def == nil && !nullable {
		return fmt.Errorf("%w: cannot add non-nullable column %q without default", ErrUnsupportedEvolution, name)
	}
	if def != nil {
		v, err := coerceValue(def, col)
		if err != nil {
			return fmt.Errorf("%w: invalid default for %q: %w", ErrUnsupportedEvolution, name, err)
		}
		col.Default = v
	}
	tx.schema.Columns = append(tx.schema.Columns, col)
	tx.schema.nextID++
	tx.schemaDirty = true
	return tx.schema.validate()
}

// DropColumn 删列。主键列不允许删除（无法安全支持）。
func (tx *Txn) DropColumn(name string) error {
	if tx.closed {
		return ErrTxnClosed
	}
	col, ok := tx.schema.columnByName(name)
	if !ok {
		return fmt.Errorf("%w: %q", ErrColumnNotFound, name)
	}
	if col.ID == tx.schema.PrimaryID {
		return fmt.Errorf("%w: cannot drop primary key column %q", ErrUnsupportedEvolution, name)
	}
	out := tx.schema.Columns[:0]
	for _, c := range tx.schema.Columns {
		if c.ID != col.ID {
			out = append(out, c)
		}
	}
	tx.schema.Columns = out
	tx.schemaDirty = true
	return nil
}

// RenameColumn 列改名（ID 不变，物理数据不迁移）。改成已存在的名字会被拒绝。
func (tx *Txn) RenameColumn(oldName, newName string) error {
	if tx.closed {
		return ErrTxnClosed
	}
	col, ok := tx.schema.columnByName(oldName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrColumnNotFound, oldName)
	}
	if _, exists := tx.schema.columnByName(newName); exists {
		return fmt.Errorf("%w: target name %q already exists", ErrColumnExists, newName)
	}
	for i := range tx.schema.Columns {
		if tx.schema.Columns[i].ID == col.ID {
			tx.schema.Columns[i].Name = newName
		}
	}
	tx.schemaDirty = true
	return nil
}

// Upsert 按主键插入或更新整行。同一事务内重复写同一主键：最后一次为准。
func (tx *Txn) Upsert(row map[string]any) error {
	if tx.closed {
		return ErrTxnClosed
	}
	pkCol, ok := tx.schema.columnByID(tx.schema.PrimaryID)
	if !ok {
		return ErrUnsupportedEvolution
	}
	pkVal, hasPK := row[pkCol.Name]
	if !hasPK || pkVal == nil {
		return fmt.Errorf("%w: column %q", ErrPrimaryKeyMissing, pkCol.Name)
	}
	pkNorm, err := coerceValue(pkVal, pkCol)
	if err != nil {
		return err
	}
	rec := &Record{Values: map[int64]any{pkCol.ID: pkNorm}}
	for name, val := range row {
		col, ok := tx.schema.columnByName(name)
		if !ok {
			return fmt.Errorf("%w: unknown column %q", ErrColumnNotFound, name)
		}
		if col.ID == pkCol.ID {
			continue
		}
		if val == nil {
			if !col.Nullable {
				return fmt.Errorf("%w: column %q", ErrNullViolation, name)
			}
			rec.Values[col.ID] = nil
			continue
		}
		v, err := coerceValue(val, col)
		if err != nil {
			return err
		}
		rec.Values[col.ID] = v
	}
	tx.staged[primaryKeyString(pkNorm)] = rec
	return nil
}

// Delete 按主键删除，写入墓碑记录；主键不存在也可靠生效（幂等）。
func (tx *Txn) Delete(pk any) error {
	if tx.closed {
		return ErrTxnClosed
	}
	pkCol, ok := tx.schema.columnByID(tx.schema.PrimaryID)
	if !ok {
		return ErrUnsupportedEvolution
	}
	pkNorm, err := coerceValue(pk, pkCol)
	if err != nil {
		return err
	}
	tx.staged[primaryKeyString(pkNorm)] = &Record{
		Values: map[int64]any{pkCol.ID: pkNorm},
		Tomb:   true,
	}
	tx.hasDeletes = true
	return nil
}

// Rollback 丢弃全部暂存改动。
func (tx *Txn) Rollback() {
	tx.closed = true
	tx.staged = nil
}

// Commit 原子提交：
//  1. 持提交锁，重读 latest，基线过期 => ErrConflictConcurrentCommit；
//  2. 数据写入 txn/ 再 rename 进 data/（崩溃后由 GC 回收未引用文件）；
//  3. 发布不可变快照文件，再原子切换 latest 指针。
func (tx *Txn) Commit() (*SnapshotView, error) {
	if tx.closed {
		return nil, ErrTxnClosed
	}
	t := tx.table
	if len(tx.staged) == 0 && !tx.schemaDirty {
		return nil, ErrNothingToCommit
	}

	lock, err := acquireTableLock(t.root)
	if err != nil {
		return nil, err
	}
	defer lock.release()

	ptr, fresh, err := loadLatest(t.root)
	if err != nil {
		return nil, err
	}
	if fresh.Version != tx.baseVersion {
		return nil, fmt.Errorf("%w: base=%d current=%d", ErrConflictConcurrentCommit, tx.baseVersion, fresh.Version)
	}

	newVersion := fresh.Version + 1
	op := tx.operationLabel()

	var newEntry *ManifestEntry
	if len(tx.staged) > 0 {
		entry, err := tx.writeDataFile(newVersion)
		if err != nil {
			return nil, err
		}
		newEntry = &entry
	}
	tx.crashHook("data-renamed")

	files := make([]ManifestEntry, len(fresh.Files), len(fresh.Files)+1)
	copy(files, fresh.Files)
	if newEntry != nil {
		files = append(files, *newEntry)
	}
	sortManifest(files)

	snap := &Snapshot{
		Version:    newVersion,
		Parent:     fresh.Version,
		Schema:     tx.schema,
		Files:      files,
		Operation:  op,
		CommitTime: time.Now().UTC().Format(time.RFC3339Nano),
	}
	snapFile := snapPath(t.root, newVersion)
	if err := writeJSONFileAtomic(snapFile, snap); err != nil {
		return nil, err
	}
	if err := fsyncDir(filepath.Join(t.root, dirMetadata)); err != nil {
		return nil, err
	}
	checksum, err := checksumFile(snapFile)
	if err != nil {
		return nil, err
	}
	tx.crashHook("snapshot-written")

	newPtr := Latest{Version: newVersion, Path: filepath.Base(snapFile), Checksum: checksum}
	if err := writeJSONFileAtomic(latestPath(t.root), newPtr); err != nil {
		return nil, err
	}
	if err := fsyncDir(filepath.Join(t.root, dirMetadata)); err != nil {
		return nil, err
	}
	_ = ptr
	tx.closed = true
	t.latest = snap
	return &SnapshotView{root: t.root, snap: snap}, nil
}

func (tx *Txn) crashHook(stage string) {
	if tx.crashAtStage == stage {
		os.Exit(99)
	}
}

// setCrashHook 仅供测试：提交协议到达指定阶段后以 os.Exit 模拟掉电。
func (tx *Txn) setCrashHook(stage string) { tx.crashAtStage = stage }

func (tx *Txn) operationLabel() string {
	switch {
	case tx.schemaDirty:
		return "evolve"
	case tx.hasDeletes:
		return "delete"
	default:
		return "upsert"
	}
}

// writeDataFile 将暂存记录按主键排序写入：同一事务重复写只保留最后一次。
func (tx *Txn) writeDataFile(version int64) (ManifestEntry, error) {
	keys := make([]string, 0, len(tx.staged))
	for k := range tx.staged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	name := fmt.Sprintf("%010d-0000000000.jsonl", version)
	txnPath := filepath.Join(tx.table.root, dirTxn, name)
	finalPath := filepath.Join(tx.table.root, dirData, name)
	if err := os.MkdirAll(filepath.Dir(txnPath), 0o755); err != nil {
		return ManifestEntry{}, err
	}
	f, err := os.OpenFile(txnPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return ManifestEntry{}, err
	}
	bw := bufio.NewWriter(f)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	pkMin, pkMax := keys[0], keys[len(keys)-1]
	for _, k := range keys {
		if err := enc.Encode(tx.staged[k]); err != nil {
			f.Close()
			return ManifestEntry{}, err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return ManifestEntry{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return ManifestEntry{}, err
	}
	if err := f.Close(); err != nil {
		return ManifestEntry{}, err
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return ManifestEntry{}, err
	}
	if err := os.Rename(txnPath, finalPath); err != nil {
		return ManifestEntry{}, err
	}
	if err := fsyncDir(filepath.Join(tx.table.root, dirData)); err != nil {
		return ManifestEntry{}, err
	}
	return ManifestEntry{Path: name, MinPK: pkMin, MaxPK: pkMax, Rows: len(keys)}, nil
}
