package lakehouse

import (
	"errors"
	"fmt"
)

// Code 是可区分的失败原因。调用方可用 errors.Is(err, Err*) 或
// ErrorCode(err) 判定具体原因。
type Code string

const (
	CodeConflictSnapshotStale Code = "CONFLICT_SNAPSHOT_STALE" // 基快照已过期，快照级乐观并发冲突
	CodeConflictInProgress    Code = "CONFLICT_IN_PROGRESS"    // 另一个提交正持有提交锁

	CodeSchemaColumnNotFound Code = "SCHEMA_COLUMN_NOT_FOUND" // 列不存在（或已被删除）
	CodeSchemaColumnRenamed  Code = "SCHEMA_COLUMN_RENAMED"   // 列曾被改名（按旧名访问）
	CodeSchemaColumnExists   Code = "SCHEMA_COLUMN_EXISTS"    // 列名已存在
	CodeSchemaIsPrimaryKey   Code = "SCHEMA_IS_PRIMARY_KEY"   // 不允许删除/改名主键列
	CodeSchemaTypeChange     Code = "SCHEMA_TYPE_CHANGE_UNSUPPORTED"

	CodeValidationFailed Code = "VALIDATION_FAILED" // 行数据违反 schema
	CodeSnapshotNotFound Code = "SNAPSHOT_NOT_FOUND"
	CodeSnapshotExpired  Code = "SNAPSHOT_EXPIRED" // 版本曾经存在但已被保留策略清理

	CodePinTargetInvalid     Code = "PIN_TARGET_INVALID"     // 长期保留标记的目标版本不存在或已清理
	CodeRetentionInvalid     Code = "RETENTION_INVALID"      // 保留策略参数非法
)

// Error 携带稳定 Code，便于程序化区分冲突、结构演进与校验类失败。
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

func newError(code Code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ErrorCode 返回错误携带的稳定 Code；非本包错误返回空串。
func ErrorCode(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// IsConflict 报告错误是否属于并发提交冲突类。
func IsConflict(err error) bool {
	c := ErrorCode(err)
	return c == CodeConflictSnapshotStale || c == CodeConflictInProgress
}
