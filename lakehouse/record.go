package lakehouse

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Row 是对外的逻辑行：列名 -> 值。数值统一为 int64/float64。
type Row map[string]any

// record 是物理行：列 ID -> 值。按 ID 存储使改名/加列/删列都不会错列。
type record map[int]any

// encodeKey 生成确定的主键物理编码：\x01 分隔的规范化标量值。
// 内存写入与从 JSONL 读回（json.Number）必须生成相同编码。
func encodeKey(sch *Schema, r record) string {
	out := make([]string, 0, len(sch.PrimaryKey))
	for _, name := range sch.PrimaryKey {
		c, err := sch.FindByName(name)
		if err != nil {
			panic(err)
		}
		out = append(out, scalarKeyString(r[c.ID]))
	}
	return strings.Join(out, "\x01")
}

func scalarKeyString(v any) string {
	switch x := v.(type) {
	case nil:
		return "\x00"
	case json.Number:
		return x.String()
	case json.RawMessage:
		return "r:" + string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		return "s:" + x
	case bool:
		if x {
			return "b:1"
		}
		return "b:0"
	default:
		return fmt.Sprint(x)
	}
}

// rowToRecord 把逻辑行转成物理行，并做严格校验。
func rowToRecord(sch *Schema, row Row) (record, error) {
	idx := sch.indexByName()
	rec := record{}
	for name, v := range row {
		c, ok := idx[name]
		if !ok {
			// 可能是旧名（改名）或已删列：给出可区分原因，绝不静默丢弃到别的列。
			if _, err := sch.FindByName(name); err != nil {
				return nil, err
			}
		}
		nv, ok := normalizeValue(v, c.Type)
		if !ok {
			return nil, newError(CodeValidationFailed,
				"column %q expects %s but got %s", name, c.Type, typeName(v))
		}
		if nv == nil && !c.Nullable {
			return nil, newError(CodeValidationFailed, "column %q is not nullable", name)
		}
		rec[c.ID] = nv
	}
	for _, c := range sch.Columns {
		if !c.Nullable {
			if v, exists := rec[c.ID]; !exists || v == nil {
				return nil, newError(CodeValidationFailed, "missing value for non-nullable column %q", c.Name)
			}
		}
	}
	return rec, nil
}

// recordToRow 按目标 schema 投影物理行：
//   - 目标里不存在的列 ID（删列残留、旧 schema 多列）被忽略；
//   - 物理行缺少的列 ID（后加列）填 nil（稳定缺省值）。
func recordToRow(sch *Schema, rec record) Row {
	row := Row{}
	for _, c := range sch.Columns {
		row[c.Name] = rec[c.ID]
	}
	return row
}

func projectRow(row Row, names []string) (Row, error) {
	out := Row{}
	for _, n := range names {
		v, ok := row[n]
		if !ok {
			return nil, newError(CodeSchemaColumnNotFound, "column %q does not exist in result", n)
		}
		out[n] = v
	}
	return out, nil
}
