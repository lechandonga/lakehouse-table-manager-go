package lakehouse

import (
	"os"
	"sync"
	"syscall"
)

// tableLock 同时覆盖进程内互斥与跨进程 flock 排他锁。
// flock 关联打开文件描述，同进程不同 fd 之间互不阻塞，因此必须再叠加
// 按表根路径登记的进程内互斥锁，才能串行化同进程的并发提交。
type tableLock struct {
	root string
	f    *os.File
}

var (
	globalMu sync.Mutex
	tableMus = map[string]*sync.Mutex{}
)

func inProcessMu(root string) *sync.Mutex {
	globalMu.Lock()
	defer globalMu.Unlock()
	m, ok := tableMus[root]
	if !ok {
		m = &sync.Mutex{}
		tableMus[root] = m
	}
	return m
}

func acquireTableLock(root string) (*tableLock, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	mu := inProcessMu(root)
	mu.Lock()
	f, err := os.OpenFile(root+"/.lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		mu.Unlock()
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		mu.Unlock()
		return nil, err
	}
	return &tableLock{root: root, f: f}, nil
}

func (l *tableLock) release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
	l.f = nil
	inProcessMu(l.root).Unlock()
}
