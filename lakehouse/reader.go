package lakehouse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Reader 是某个历史快照的只读视图；后续提交绝不影响其读取结果。
type Reader struct {
	t    *Table
	snap *SnapshotMeta
	sch  *Schema
}

// Snapshot 打开指定历史版本。版本不存在时返回 CodeSnapshotNotFound。
func (t *Table) Snapshot(ctx context.Context, version int64) (*Reader, error) {
	_ = ctx
	t.mu.Lock()
	maxV := t.meta.CurrentVersion
	t.mu.Unlock()
	if version < 1 || version > maxV {
		return nil, newError(CodeSnapshotNotFound, "snapshot version %d does not exist (current %d)", version, maxV)
	}
	snap := &SnapshotMeta{}
	if err := readJSON(t.snapshotPath(version), snap); err != nil {
		return nil, fmt.Errorf("read snapshot %d: %w", version, err)
	}
	sch, err := t.loadSchema(snap.SchemaID)
	if err != nil {
		return nil, err
	}
	return &Reader{t: t, snap: snap, sch: sch}, nil
}

// Head 打开最新快照。
func (t *Table) Head(ctx context.Context) (*Reader, error) {
	return t.Snapshot(ctx, t.CurrentVersion())
}

func (r *Reader) Version() int64  { return r.snap.Version }
func (r *Reader) Schema() *Schema { return r.sch.clone() }

// materialize 按快照文件顺序回放，返回每个物理主键的最终物理行。
// 合并规则（完全确定）：
//  1. 文件按快照清单顺序回放（即提交顺序）
//  2. upsert 后写覆盖前写；delete 标记删除；
//  3. 已删除主键再次 upsert 则复活，之后 delete 再次删除。
func (r *Reader) materialize() (map[string]record, []string, error) {
	current := map[string]record{}

	applyUpsert := func(path string) error {
		rows, err := readJSONLRecords(filepath.Join(r.t.dir, path), r.sch)
		if err != nil {
			return err
		}
		for _, rec := range rows {
			current[encodeKey(r.sch, rec)] = rec
		}
		return nil
	}

	for _, df := range r.snap.Files {
		if df.DeleteSidecar != "" {
			// 删除标记以 sidecar 为物理真源（df.Deletes 是清单内的镜像）。
			del := map[string]int64{}
			if err := readJSON(filepath.Join(r.t.dir, df.DeleteSidecar), &del); err != nil {
				return nil, nil, err
			}
			for k := range del {
				delete(current, k)
			}
			continue
		}
		if err := applyUpsert(df.Path); err != nil {
			return nil, nil, err
		}
	}

	// 输出按主键排序保证确定、稳定的行顺序。
	keys := make([]string, 0, len(current))
	for k := range current {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return current, keys, nil
}

// Scan 返回快照内全部存活行，按主键编码排序（确定性输出）。
func (r *Reader) Scan(ctx context.Context) ([]Row, error) {
	_ = ctx
	current, keys, err := r.materialize()
	if err != nil {
		return nil, err
	}
	out := make([]Row, 0, len(keys))
	for _, k := range keys {
		rec := current[k]
		out = append(out, recordToRow(r.sch, rec))
	}
	return out, nil
}

// Get 按主键查询单行；不存在返回 (nil, nil)。
// keyCols 必须与当前查询 schema 的主键列名一致（改名后须用新名）。
func (r *Reader) Get(ctx context.Context, keyCols Row) (Row, error) {
	_ = ctx
	// 先校验调用方给出的列名（改名后用旧名必须得到可区分错误）。
	for name := range keyCols {
		if _, err := r.sch.FindByName(name); err != nil {
			return nil, err
		}
	}
	keyRec := record{}
	for _, name := range r.sch.PrimaryKey {
		c, err := r.sch.FindByName(name)
		if err != nil {
			return nil, err
		}
		v, ok := keyCols[name]
		if !ok {
			return nil, newError(CodeValidationFailed, "missing primary key column %q", name)
		}
		nv, ok := normalizeValue(v, c.Type)
		if !ok || nv == nil {
			return nil, newError(CodeValidationFailed, "invalid primary key value for %q", name)
		}
		keyRec[c.ID] = nv
	}
	want := encodeKey(r.sch, keyRec)
	current, _, err := r.materialize()
	if err != nil {
		return nil, err
	}
	if rec, ok := current[want]; ok {
		return recordToRow(r.sch, rec), nil
	}
	return nil, nil
}

// Project 返回指定列的投影；列名为旧名/已删除名时返回可区分错误。
func (r *Reader) Project(ctx context.Context, names []string) ([]Row, error) {
	for _, n := range names {
		if _, err := r.sch.FindByName(n); err != nil {
			return nil, err
		}
	}
	rows, err := r.Scan(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		p, err := projectRow(row, names)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func readJSONLRecords(path string, sch *Schema) ([]record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		m := map[int]json.RawMessage{}
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, err
		}
		rec := record{}
		for id, raw := range m {
			if c, ok := sch.byID(id); ok {
				rec[id] = decodeNumber(raw, c.Type)
			} else {
				rec[id] = decodeRawScalar(raw)
			}
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}
