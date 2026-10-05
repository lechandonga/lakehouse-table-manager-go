package lakehouse

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// writeJSONAtomic：写临时文件 -> fsync -> 原子 rename，保证发布出去的元数据完整。
func writeJSONAtomic(path string, v any) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// fileLock 是跨进程的提交互斥锁（flock）；进程死亡后内核自动释放。
// 快照级冲突由 CAS（期望基版本 == 当前发布版本）判定，锁只负责串行化发布。
type fileLock struct {
	f *os.File
}

func tryAcquireLock(path string) (*fileLock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := flockTry(f); err != nil {
		_ = f.Close()
		return nil, newError(CodeConflictInProgress, "another commit is in progress: %v", err)
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) release() {
	if l == nil {
		return
	}
	_ = flockRelease(l.f)
	_ = l.f.Close()
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func writeJSONL(path string, rows []map[int]any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			_ = f.Close()
			_ = os.Remove(path + ".tmp")
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(path + ".tmp")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
