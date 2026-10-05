package lakehouse

import (
	"fmt"
	"sort"
)

// 支持的列类型。加列时新列在历史快照中按类型零值/null 读取。
const (
	TypeInt    = "int"
	TypeString = "string"
	TypeBool   = "bool"
	TypeFloat  = "float"
)

// Column 以 ID 为稳定身份；改名不改 ID，删列后 ID 永不复用。
type Column struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// RenameEntry 记录改名历史，使按旧名读取时能明确报错而不是静默错列。
type RenameEntry struct {
	ColumnID int    `json:"column_id"`
	OldName  string `json:"old_name"`
	NewName  string `json:"new_name"`
}

// DroppedName 记录被删除列曾经使用过的名字，避免加列同名时串历史。
type DroppedName struct {
	ColumnID  int    `json:"column_id"`
	Name      string `json:"name"`
	LastUsedV int64  `json:"last_used_v"`
}

// Schema 是不可变值对象；演进返回新的 Schema，不修改旧值。
type Schema struct {
	Columns    []Column      `json:"columns"`     // 顺序按 ID 稳定排列
	PrimaryKey []string      `json:"primary_key"` // 主键列名（主键列不允许删除/改名）
	NextColID  int           `json:"next_col_id"`
	Renames    []RenameEntry `json:"renames,omitempty"`
	Dropped    []DroppedName `json:"dropped_names,omitempty"`
}

func (s *Schema) indexByName() map[string]Column {
	m := make(map[string]Column, len(s.Columns))
	for _, c := range s.Columns {
		m[c.Name] = c
	}
	return m
}

// FindByName 返回列；不存在时错误码区分“曾改名”和“已删除/不存在”。
func (s *Schema) FindByName(name string) (Column, error) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, nil
		}
	}
	for _, r := range s.Renames {
		if r.OldName == name {
			return Column{}, newError(CodeSchemaColumnRenamed,
				"column %q was renamed to %q (id=%d); read by the new name", name, r.NewName, r.ColumnID)
		}
	}
	for _, d := range s.Dropped {
		if d.Name == name {
			return Column{}, newError(CodeSchemaColumnNotFound,
				"column %q (id=%d) has been dropped", name, d.ColumnID)
		}
	}
	return Column{}, newError(CodeSchemaColumnNotFound, "column %q does not exist", name)
}

func (s *Schema) byID(id int) (Column, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

// NewSchema 创建初始 schema：列 ID 从 1 开始按名字稳定分配。
func NewSchema(primaryKey []string, cols ...Column) (*Schema, error) {
	if len(primaryKey) == 0 {
		return nil, newError(CodeValidationFailed, "primary key must contain at least one column")
	}
	seen := map[string]bool{}
	maxID := 0
	for i := range cols {
		c := &cols[i]
		if c.Name == "" {
			return nil, newError(CodeValidationFailed, "column name must not be empty")
		}
		if seen[c.Name] {
			return nil, newError(CodeSchemaColumnExists, "duplicate column %q", c.Name)
		}
		seen[c.Name] = true
		if c.ID == 0 {
			c.ID = i + 1
		}
		if !validType(c.Type) {
			return nil, newError(CodeValidationFailed, "column %q has unsupported type %q", c.Name, c.Type)
		}
		if c.ID > maxID {
			maxID = c.ID
		}
	}
	for _, pk := range primaryKey {
		if !seen[pk] {
			return nil, newError(CodeValidationFailed, "primary key column %q is not in schema", pk)
		}
	}
	s := &Schema{Columns: append([]Column(nil), cols...), PrimaryKey: append([]string(nil), primaryKey...), NextColID: maxID + 1}
	sort.Slice(s.Columns, func(i, j int) bool { return s.Columns[i].ID < s.Columns[j].ID })
	for i := range s.Columns {
		if contains(s.PrimaryKey, s.Columns[i].Name) {
			s.Columns[i].Nullable = false
		}
	}
	return s, nil
}

// AddColumn 追加新列；历史数据按零值/null 读取。复用“已删除列 ID”绝不发生。
func (s *Schema) AddColumn(c Column) (*Schema, error) {
	if c.Name == "" {
		return nil, newError(CodeValidationFailed, "column name must not be empty")
	}
	if !validType(c.Type) {
		return nil, newError(CodeValidationFailed, "unsupported type %q", c.Type)
	}
	if _, err := s.FindByName(c.Name); err == nil {
		return nil, newError(CodeSchemaColumnExists, "column %q already exists", c.Name)
	} else if ErrorCode(err) == CodeSchemaColumnRenamed {
		return nil, err
	}
	next := s.clone()
	c.ID = s.NextColID
	next.Columns = append(next.Columns, c)
	next.NextColID = s.NextColID + 1
	return next, nil
}

// DropColumn 删除列；历史文件中残留同名列（同 ID）在读取时直接忽略。
func (s *Schema) DropColumn(name string, atVersion int64) (*Schema, error) {
	if contains(s.PrimaryKey, name) {
		return nil, newError(CodeSchemaIsPrimaryKey, "cannot drop primary key column %q", name)
	}
	c, err := s.FindByName(name)
	if err != nil {
		return nil, err
	}
	next := s.clone()
	kept := next.Columns[:0]
	for _, x := range next.Columns {
		if x.ID != c.ID {
			kept = append(kept, x)
		}
	}
	next.Columns = kept
	next.Dropped = append(next.Dropped, DroppedName{ColumnID: c.ID, Name: c.Name, LastUsedV: atVersion})
	for _, r := range s.Renames {
		if r.ColumnID == c.ID {
			next.Dropped = append(next.Dropped, DroppedName{ColumnID: c.ID, Name: r.OldName, LastUsedV: atVersion})
		}
	}
	return next, nil
}

// RenameColumn 仅改名字，列 ID 不变；物理文件按 ID 对齐，绝不错列。
func (s *Schema) RenameColumn(oldName, newName string) (*Schema, error) {
	if contains(s.PrimaryKey, oldName) {
		return nil, newError(CodeSchemaIsPrimaryKey, "cannot rename primary key column %q", oldName)
	}
	c, err := s.FindByName(oldName)
	if err != nil {
		return nil, err
	}
	if _, err := s.FindByName(newName); err == nil {
		return nil, newError(CodeSchemaColumnExists, "column %q already exists", newName)
	} else if ErrorCode(err) == CodeSchemaColumnRenamed {
		return nil, err
	}
	next := s.clone()
	for i := range next.Columns {
		if next.Columns[i].ID == c.ID {
			next.Columns[i].Name = newName
		}
	}
	next.Renames = append(next.Renames, RenameEntry{ColumnID: c.ID, OldName: oldName, NewName: newName})
	return next, nil
}

// ChangeType 被明确拒绝：类型变更无法保证所有历史数据安全可读。
func (s *Schema) ChangeType(name, newType string) (*Schema, error) {
	if _, err := s.FindByName(name); err != nil {
		return nil, err
	}
	return nil, newError(CodeSchemaTypeChange,
		"changing type of column %q to %q is not supported; add a new column and backfill explicitly", name, newType)
}

func (s *Schema) clone() *Schema {
	next := &Schema{
		Columns:    append([]Column(nil), s.Columns...),
		PrimaryKey: append([]string(nil), s.PrimaryKey...),
		NextColID:  s.NextColID,
		Renames:    append([]RenameEntry(nil), s.Renames...),
		Dropped:    append([]DroppedName(nil), s.Dropped...),
	}
	return next
}

// SortedColumnIDs 返回按 ID 排序的列 ID（用于确定的投影与序列化）。
func (s *Schema) SortedColumnIDs() []int {
	ids := make([]int, 0, len(s.Columns))
	for _, c := range s.Columns {
		ids = append(ids, c.ID)
	}
	sort.Ints(ids)
	return ids
}

func validType(t string) bool {
	switch t {
	case TypeInt, TypeString, TypeBool, TypeFloat:
		return true
	}
	return false
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case int64:
		return TypeInt
	case string:
		return TypeString
	case bool:
		return TypeBool
	case float64:
		return TypeFloat
	default:
		return fmt.Sprintf("%T", v)
	}
}
