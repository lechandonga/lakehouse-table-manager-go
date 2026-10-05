package lakehouse

import "errors"

// 可区分的失败原因。调用方可用 errors.Is 精确判断。
var (
	// ErrConflictConcurrentCommit：事务基于的快照已被其他已确认提交推进。
	ErrConflictConcurrentCommit = errors.New("lakehouse: concurrent commit conflict (base snapshot is stale)")
	// ErrTxnClosed：事务提交/回滚后被再次使用。
	ErrTxnClosed = errors.New("lakehouse: transaction already closed")

	ErrColumnNotFound    = errors.New("lakehouse: column not found")
	ErrColumnExists      = errors.New("lakehouse: column already exists")
	ErrColumnDropped     = errors.New("lakehouse: column has been dropped in this snapshot")
	ErrTypeMismatch      = errors.New("lakehouse: value type does not match column type")
	ErrPrimaryKeyMissing = errors.New("lakehouse: primary key value is missing")
	ErrNullViolation     = errors.New("lakehouse: null value in non-nullable column without default")
	// ErrUnsupportedEvolution：无法安全支持的结构变更（改类型、改主键等）。
	ErrUnsupportedEvolution = errors.New("lakehouse: unsupported schema evolution")

	ErrSnapshotNotFound = errors.New("lakehouse: snapshot version not found")
	ErrTableCorrupted   = errors.New("lakehouse: table metadata corrupted")
)
