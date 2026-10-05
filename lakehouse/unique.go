package lakehouse

import (
	"crypto/rand"
	"encoding/hex"
)

// uniqueSuffix 为不可变数据文件生成唯一后缀，保证崩溃提交遗留的孤儿文件
// 与后续提交的文件名永不冲突，使 GC 能精确按引用关系回收。
func uniqueSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000"
	}
	return hex.EncodeToString(b[:])
}
