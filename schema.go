package lakehouse

import (
	"fmt"
	"sort"
)

// 物理数据类型：仅支持 JSON 可直接表达、规则明确的类型。
const (
	TypeInt    = "int"    // int64
	TypeFloat  = "float"  // float64
	TypeString = "string" // string
	TypeBool   = "bool"   // bool
)

// Column 是列定义。列以 ID 作为物理身份，改名不改 ID；
// 删除后同名字段在数据文件中的残留（旧快照里）不会串列，因为所有读取按 ID 映射。
type Column struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	// Default 为加列后历史行填充的确定缺省值；null 表示填充 NULL。
	Default any `json:"default,omitempty"`
}

// Schema 是某个快照的表结构（全量快照内自包含）。
type Schema struct {
	Columns   []Column `json:"columns"`
	PrimaryID int64    `json:"primary_id"`
	nextID    int64
}

// NewSchema 创建单主键表结构。主键列默认非空。
func NewSchema(primary Column, others ...Column) (*Schema, error) {
	if primary.Name == "" {
		return nil, fmt.Errorf("%w: empty column name", ErrUnsupportedEvolution)
	}
	primary.ID = 1
	primary.Nullable = false
	s := &Schema{Columns: []Column{primary}, PrimaryID: 1, nextID: 2}
	for _, c := range others {
		c.ID = s.nextID
		s.nextID++
		s.Columns = append(s.Columns, c)
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Schema) validate() error {
	names := map[string]bool{}
	ids := map[int64]bool{}
	hasPK := false
	for _, c := range s.Columns {
		if c.Name == "" {
			return fmt.Errorf("%w: empty column name", ErrUnsupportedEvolution)
		}
		if names[c.Name] {
			return fmt.Errorf("%w: duplicate column name %q", ErrUnsupportedEvolution, c.Name)
		}
		if ids[c.ID] {
			return fmt.Errorf("%w: duplicate column id %d", ErrUnsupportedEvolution, c.ID)
		}
		if !validType(c.Type) {
			return fmt.Errorf("%w: unknown type %q for column %q", ErrUnsupportedEvolution, c.Type, c.Name)
		}
		names[c.Name] = true
		ids[c.ID] = true
		if c.ID == s.PrimaryID {
			hasPK = true
		}
	}
	if !hasPK {
		return fmt.Errorf("%w: primary key column missing", ErrUnsupportedEvolution)
	}
	return nil
}

func validType(t string) bool {
	return t == TypeInt || t == TypeFloat || t == TypeString || t == TypeBool
}

func (s *Schema) columnByID(id int64) (Column, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

func (s *Schema) columnByName(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// SortedColumnIDs 返回按 ID 升序的列 ID，保证输出顺序确定。
func (s *Schema) sortedColumnIDs() []int64 {
	ids := make([]int64, 0, len(s.Columns))
	for _, c := range s.Columns {
		ids = append(ids, c.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
