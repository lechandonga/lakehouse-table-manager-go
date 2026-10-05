package lakehouse

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// Row 是面向用户的行：键为当前快照结构中的列名，按列 ID 投影而来。
// 历史快照里残留的、当前结构已删除的同名列不会出现（物理按 ID 映射）。
type Row map[string]any

// SnapshotView 是某个不可变快照的只读视图。
type SnapshotView struct {
	root string
	snap *Snapshot
	err  error
}

// Err 返回打开历史版本时的错误（如版本不存在）。
func (v *SnapshotView) Err() error { return v.err }

// Version 返回快照版本号。
func (v *SnapshotView) Version() int64 {
	if v.err != nil {
		return 0
	}
	return v.snap.Version
}

// SchemaVersion 返回该快照自包含的结构副本信息（列名/类型）。
func (v *SnapshotView) Schema() Schema {
	if v.err != nil || v.snap.Schema == nil {
		return Schema{}
	}
	return *v.snap.Schema
}

type mergedRow struct {
	values map[int64]any
	tomb   bool
}

// readMerged 按清单顺序归并：文件路径升序、文件内靠后者胜出，
// 规则完全确定；墓碑覆盖任何先前值。
func (v *SnapshotView) readMerged() (map[string]*mergedRow, error) {
	merged := map[string]*mergedRow{}
	files := make([]ManifestEntry, len(v.snap.Files))
	copy(files, v.snap.Files)
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	for _, mf := range files {
		path := filepath.Join(v.root, dirData, filepath.Base(mf.Path))
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		for dec.More() {
			var raw struct {
				V map[int64]json.RawMessage `json:"v"`
				T bool                      `json:"t,omitempty"`
			}
			if err := dec.Decode(&raw); err != nil {
				return nil, err
			}
			rec := Record{Values: map[int64]any{}, Tomb: raw.T}
			for id, msg := range raw.V {
				rec.Values[id] = decodeValue(msg)
			}
			key, err := v.recordKey(rec)
			if err != nil {
				return nil, err
			}
			merged[key] = &mergedRow{values: rec.Values, tomb: rec.Tomb}
		}
	}
	return merged, nil
}

func decodeValue(msg json.RawMessage) any {
	if string(msg) == "null" {
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(msg, &n); err == nil {
		if i, err := n.Int64(); err == nil {
			return i
		}
		f, _ := n.Float64()
		return f
	}
	var s string
	if err := json.Unmarshal(msg, &s); err == nil {
		return s
	}
	var b bool
	if err := json.Unmarshal(msg, &b); err == nil {
		return b
	}
	return nil
}

func (v *SnapshotView) recordKey(r Record) (string, error) {
	val, ok := r.Values[v.snap.Schema.PrimaryID]
	if !ok {
		return "", ErrPrimaryKeyMissing
	}
	return primaryKeyString(val), nil
}

// Scan 返回该快照下全部存活行，按主键确定性排序。
func (v *SnapshotView) Scan() ([]Row, error) {
	if v.err != nil {
		return nil, v.err
	}
	merged, err := v.readMerged()
	if err != nil {
		return nil, err
	}
	rows := make([]Row, 0, len(merged))
	for _, mr := range merged {
		if mr.tomb {
			continue
		}
		row, err := v.project(mr.values)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	sortRows(rows, v.snap.Schema)
	return rows, nil
}

// Get 按主键读取单行；found=false 表示不存在或已被删除。
func (v *SnapshotView) Get(pk any) (row Row, found bool, err error) {
	if v.err != nil {
		return nil, false, v.err
	}
	merged, err := v.readMerged()
	if err != nil {
		return nil, false, err
	}
	mr, ok := merged[primaryKeyString(pk)]
	if !ok || mr.tomb {
		return nil, false, nil
	}
	row, err = v.project(mr.values)
	return row, err == nil, err
}

// project 按“提交当时的结构”把物理列 ID 投影成列名行：
// 新增列对历史行填充确定缺省值（无默认则 NULL）；
// 已删除列的物理值被丢弃；改名列沿用同一 ID，旧名不再可解析。
func (v *SnapshotView) project(values map[int64]any) (Row, error) {
	row := Row{}
	for _, col := range v.snap.Schema.Columns {
		val, present := values[col.ID]
		if !present {
			val = col.Default // 加列前写入的历史行：确定缺省
		}
		converted, err := coerceValue(val, col)
		if err != nil {
			return nil, err
		}
		row[col.Name] = converted
	}
	return row, nil
}

func sortRows(rows []Row, s *Schema) {
	pkCol, _ := s.columnByID(s.PrimaryID)
	sort.Slice(rows, func(i, j int) bool {
		return primaryKeyString(rows[i][pkCol.Name]) < primaryKeyString(rows[j][pkCol.Name])
	})
}
