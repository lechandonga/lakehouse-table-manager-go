package lakehouse

// DataFile 是不可变数据文件描述符。
type DataFile struct {
	Path    string `json:"path"`
	Records int    `json:"records"`
	// DeleteSidecar 非空时指向该文件的删除标记 sidecar（物理主键 -> 删除所在版本）。
	DeleteSidecar string `json:"delete_sidecar,omitempty"`
	// Deletes 是本次提交新增的删除标记（key -> 删除所在版本）。
	Deletes map[string]int64 `json:"deletes,omitempty"`
}

// SnapshotMeta 是不可变快照清单。
type SnapshotMeta struct {
	Version       int64      `json:"version"`
	ParentVersion int64      `json:"parent_version"` // 0 表示初始快照
	SchemaID      int64      `json:"schema_id"`      // 该快照绑定的 schema 版本
	Files         []DataFile `json:"files"`
	Operation     string     `json:"operation"` // append / overwrite / compact
	Message       string     `json:"message,omitempty"`
}

// TableMeta 是表级元数据指针。
type TableMeta struct {
	Name           string `json:"name"`
	CurrentVersion int64  `json:"current_version"`
	SchemaVersion  int64  `json:"schema_version"`
}
