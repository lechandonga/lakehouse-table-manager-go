//go:build !windows

package lakehouse

import (
	"errors"
	"os"
	"syscall"
)

func flockTry(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func flockRelease(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

func errIsNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
