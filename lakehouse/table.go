package lakehouse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Op 标识一次数据提交的操作类型。
type Op string

const (
	OpUpsert Op = "upsert"
	OpDelete Op = "delete"
)

// CrashHook 在提交关键点被调用；返回错误会模拟提交在该点崩溃。
// 仅用于测试：发布顺序为“数据文件 -> 快照元数据 -> current 指针”，
// 任何在 current 翻转前的崩溃都使该提交完全不可见。
type CrashHook func(stage string) error

type Options struct {
	CrashHook CrashHook
}

// Table 是一个本地湖仓表句柄，并发安全。
type Table struct {
	dir string
	mu  sync.Mutex

	meta    TableMeta
	schemas map[int64]*Schema
	current *SnapshotMeta

	hook CrashHook
}

// Create 在空目录上创建表并写入初始快照 v1。
func Create(ctx context.Context, dir, name string, sch *Schema, opts *Options) (*Table, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(entries) != 0 {
		return nil, newError(CodeValidationFailed, "directory %q is not empty", dir)
	}
	for _, d := range []string{"metadata", "data", "txns"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			return nil, err
		}
	}
	t := &Table{dir: dir, schemas: map[int64]*Schema{}}
	if opts != nil {
		t.hook = opts.CrashHook
	}

	t.meta = TableMeta{Name: name, CurrentVersion: 1, SchemaVersion: 1}
	t.schemas[1] = sch
	t.current = &SnapshotMeta{Version: 1, SchemaID: 1, Operation: "create", Message: "initial empty snapshot"}

	if err := writeJSONAtomic(t.schemaPath(1), sch); err != nil {
		return nil, err
	}
	if err := writeJSONAtomic(t.snapshotPath(1), t.current); err != nil {
		return nil, err
	}
	if err := writeJSONAtomic(t.metaPath(), t.meta); err != nil {
		return nil, err
	}
	return t, nil
}

// Open 打开表并执行崩溃恢复：清理 txns 残留锁、以 current 指针为准装载状态。
func Open(ctx context.Context, dir string, opts *Options) (*Table, error) {
	_ = ctx
	t := &Table{dir: dir, schemas: map[int64]*Schema{}}
	if opts != nil {
		t.hook = opts.CrashHook
	}
	// 崩溃恢复：进程死亡后 flock 已由内核释放；残留锁文件与孤儿数据文件
	// 均不被任何已发布快照引用，删除锁文件即可，数据文件留给 GC。
	if lockFiles, err := filepath.Glob(filepath.Join(dir, "txns", "*.lock")); err == nil {
		for _, lf := range lockFiles {
			_ = os.Remove(lf)
		}
	}
	if err := readJSON(t.metaPath(), &t.meta); err != nil {
		return nil, fmt.Errorf("read table meta: %w", err)
	}
	snap := &SnapshotMeta{}
	if err := readJSON(t.snapshotPath(t.meta.CurrentVersion), snap); err != nil {
		return nil, fmt.Errorf("read current snapshot: %w", err)
	}
	// 装载当前 schema（历史 schema 在按版本读取时按需加载）。
	sch := &Schema{}
	if err := readJSON(t.schemaPath(t.meta.SchemaVersion), sch); err != nil {
		return nil, fmt.Errorf("read current schema: %w", err)
	}
	t.schemas[t.meta.SchemaVersion] = sch
	t.current = snap
	return t, nil
}

func (t *Table) Close() error { return nil }

// CurrentVersion 返回当前已发布快照版本。
func (t *Table) CurrentVersion() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.meta.CurrentVersion
}

// CurrentSchema 返回当前 schema 的深拷贝（只读视图）。
func (t *Table) CurrentSchema() *Schema {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.schemas[t.meta.SchemaVersion].clone()
}

// Commit 以 baseVersion 为乐观并发基版本提交 upsert/delete。
// baseVersion 过期时返回 CodeConflictSnapshotStale；
// 另一提交正进行时返回 CodeConflictInProgress。
func (t *Table) Commit(ctx context.Context, baseVersion int64, op Op, rows []Row, message string) (int64, error) {
	_ = ctx
	t.mu.Lock()
	sch := t.schemas[t.meta.SchemaVersion]
	t.mu.Unlock()

	records := make([]record, 0, len(rows))
	for _, r := range rows {
		rec, err := rowToRecord(sch, r)
		if err != nil {
			return 0, err
		}
		records = append(records, rec)
	}
	// 批内确定性合并：同一主键最后一条生效。
	records = dedupeRecords(sch, records)

	if op != OpUpsert && op != OpDelete {
		return 0, newError(CodeValidationFailed, "unknown op %q", op)
	}
	return t.publish(baseVersion, "append", message, records, op == OpDelete, sch)
}

// Evolve 提交一次 schema 结构演进（纯元数据提交，生成新快照与新 schema 版本）。
func (t *Table) Evolve(ctx context.Context, baseVersion int64, next *Schema, message string) (int64, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := tryAcquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return 0, err
	}
	defer lock.release()

	if baseVersion != t.meta.CurrentVersion {
		return 0, newError(CodeConflictSnapshotStale,
			"base version %d is stale (current %d)", baseVersion, t.meta.CurrentVersion)
	}
	newSchemaV := t.meta.SchemaVersion + 1
	newSnapV := t.meta.CurrentVersion + 1
	snap := &SnapshotMeta{
		Version:       newSnapV,
		ParentVersion: t.meta.CurrentVersion,
		SchemaID:      newSchemaV,
		Files:         append([]DataFile(nil), t.current.Files...),
		Operation:     "schema",
		Message:       message,
	}
	// 发布顺序：新 schema -> 新快照 -> current 指针。
	if err := writeJSONAtomic(t.schemaPath(newSchemaV), next); err != nil {
		return 0, err
	}
	if err := writeJSONAtomic(t.snapshotPath(newSnapV), snap); err != nil {
		return 0, err
	}
	t.meta.SchemaVersion = newSchemaV
	t.meta.CurrentVersion = newSnapV
	if err := writeJSONAtomic(t.metaPath(), t.meta); err != nil {
		return 0, err
	}
	t.schemas[newSchemaV] = next
	t.current = snap
	return newSnapV, nil
}

// AddColumn / DropColumn / RenameColumn 是 Evolve 的便捷封装。
func (t *Table) AddColumn(ctx context.Context, baseVersion int64, c Column) (int64, error) {
	next, err := t.CurrentSchema().AddColumn(c)
	if err != nil {
		return 0, err
	}
	return t.Evolve(ctx, baseVersion, next, fmt.Sprintf("add column %q", c.Name))
}

func (t *Table) DropColumn(ctx context.Context, baseVersion int64, name string) (int64, error) {
	cur := t.CurrentSchema()
	next, err := cur.DropColumn(name, t.CurrentVersion()+1)
	if err != nil {
		return 0, err
	}
	return t.Evolve(ctx, baseVersion, next, fmt.Sprintf("drop column %q", name))
}

func (t *Table) RenameColumn(ctx context.Context, baseVersion int64, oldName, newName string) (int64, error) {
	next, err := t.CurrentSchema().RenameColumn(oldName, newName)
	if err != nil {
		return 0, err
	}
	return t.Evolve(ctx, baseVersion, next, fmt.Sprintf("rename column %q -> %q", oldName, newName))
}

// publish 执行崩溃安全的四阶段提交。
func (t *Table) publish(baseVersion int64, operation, message string, records []record, isDelete bool, sch *Schema) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	lock, err := tryAcquireLock(filepath.Join(t.dir, "txns", "commit.lock"))
	if err != nil {
		return 0, err
	}
	defer lock.release()

	// CAS：只有基快照等于当前已发布快照才允许提交。
	if baseVersion != t.meta.CurrentVersion {
		return 0, newError(CodeConflictSnapshotStale,
			"base version %d is stale (current %d)", baseVersion, t.meta.CurrentVersion)
	}

	newV := t.meta.CurrentVersion + 1
	uniq := uniqueSuffix()
	df := DataFile{}
	if isDelete {
		df.Path = filepath.Join("data", fmt.Sprintf("v-delete-%d-%s.jsonl", newV, uniq))
	} else {
		df.Path = filepath.Join("data", fmt.Sprintf("v-upsert-%d-%s.jsonl", newV, uniq))
	}

	// 阶段1：写不可变数据文件（失败则什么都没发布）。
	if !isDelete {
		rowsOut := make([]map[int]any, 0, len(records))
		for _, r := range records {
			rowsOut = append(rowsOut, map[int]any(r))
		}
		if err := writeJSONL(filepath.Join(t.dir, df.Path), rowsOut); err != nil {
			return 0, err
		}
		df.Records = len(records)
	} else {
		del := make(map[string]int64, len(records))
		for _, r := range records {
			del[encodeKey(sch, r)] = newV
		}
		df.Deletes = del
		df.DeleteSidecar = filepath.Join("data", fmt.Sprintf("v-delete-%d-%s.deletes.json", newV, uniq))
		if err := writeJSONAtomic(filepath.Join(t.dir, df.DeleteSidecar), del); err != nil {
			return 0, err
		}
	}
	if t.hook != nil {
		if err := t.hook("after-data-files"); err != nil {
			return 0, err
		}
	}

	files := append(append([]DataFile(nil), t.current.Files...), df)
	snap := &SnapshotMeta{
		Version:       newV,
		ParentVersion: t.meta.CurrentVersion,
		SchemaID:      t.meta.SchemaVersion,
		Files:         files,
		Operation:     operation,
		Message:       message,
	}

	// 阶段2：写新快照元数据（尚不可见）。
	if err := writeJSONAtomic(t.snapshotPath(newV), snap); err != nil {
		return 0, err
	}
	if t.hook != nil {
		if err := t.hook("after-snapshot-meta"); err != nil {
			return 0, err
		}
	}

	// 阶段3：原子翻转 current 指针 —— 唯一的可见性切换点。
	t.meta.CurrentVersion = newV
	if err := writeJSONAtomic(t.metaPath(), t.meta); err != nil {
		return 0, err
	}
	if t.hook != nil {
		if err := t.hook("after-current-publish"); err != nil {
			// 极端情况：数据已发布但进程被杀；重启恢复后数据仍可见。
			return 0, err
		}
	}

	// 阶段4：更新内存状态。
	t.current = snap
	return newV, nil
}

// dedupeRecords 批内同主键确定性合并：按主键出现顺序，后者覆盖前者。
func dedupeRecords(sch *Schema, rows []record) []record {
	order := make([]string, 0, len(rows))
	byKey := map[string]record{}
	for _, r := range rows {
		k := encodeKey(sch, r)
		if _, ok := byKey[k]; !ok {
			order = append(order, k)
		}
		byKey[k] = r
	}
	out := make([]record, 0, len(order))
	for _, k := range order {
		out = append(out, byKey[k])
	}
	return out
}

func (t *Table) metaPath() string { return filepath.Join(t.dir, "metadata", "table.json") }
func (t *Table) snapshotPath(v int64) string {
	return filepath.Join(t.dir, "metadata", fmt.Sprintf("snap-%d.json", v))
}
func (t *Table) schemaPath(v int64) string {
	return filepath.Join(t.dir, "metadata", fmt.Sprintf("schema-%d.json", v))
}

// loadSchema 按版本加载（带缓存）。
func (t *Table) loadSchema(id int64) (*Schema, error) {
	t.mu.Lock()
	if s, ok := t.schemas[id]; ok {
		t.mu.Unlock()
		return s, nil
	}
	t.mu.Unlock()
	s := &Schema{}
	if err := readJSON(t.schemaPath(id), s); err != nil {
		return nil, err
	}
	t.mu.Lock()
	t.schemas[id] = s
	t.mu.Unlock()
	return s, nil
}
