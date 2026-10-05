//go:build windows

package lakehouse

import (
	"errors"
	"os"
)

// Windows 上退化为不支持跨进程阻塞的占位实现（本地复现主要在 Unix）。
func flockTry(f *os.File) error {
	_ = f
	return nil
}

func flockRelease(f *os.File) error {
	_ = f
	return nil
}

func errIsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
