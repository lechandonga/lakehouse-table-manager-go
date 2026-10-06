package lakehouse

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// TestMultiHandleWriters 业务：同一进程内多个独立打开的句柄并发写，
// 同一基线上恰好一个成功，后到者 CONFLICT_SNAPSHOT_STALE；
// 先确认的数据绝不会被后到者覆盖或吃掉。
func TestMultiHandleWriters(t *testing.T) {
	t.Log("[业务:多句柄并发] === TestMultiHandleWriters 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	base, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(0), "name": "seed", "score": int64(0)}}, "seed")
	if err != nil {
		t.Fatal(err)
	}

	tableDir := filepath.Join(dir, "table")
	var handles []*Table
	const n = 6
	for i := 0; i < n; i++ {
		h, err := Open(ctxbg, tableDir, nil)
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, stale, other := 0, 0, 0
	start := make(chan struct{})
	for i, h := range handles {
		i, h := i, h
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, e := h.Commit(ctxbg, base, OpUpsert,
				[]Row{{"id": int64(1000 + i), "name": fmt.Sprintf("h%d", i), "score": int64(i)}},
				fmt.Sprintf("handle %d", i))
			mu.Lock()
			defer mu.Unlock()
			switch ErrorCode(e) {
			case "":
				ok++
			case CodeConflictSnapshotStale:
				stale++
			default:
				other++
				t.Errorf("[用例:多句柄冲突] handle %d 非预期错误 %v", i, e)
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || stale != n-1 || other != 0 {
		t.Fatalf("[用例:多句柄冲突] 期望1成功/%d过期, 实际 ok=%d stale=%d other=%d", n-1, ok, stale, other)
	}
	t.Logf("[用例:多句柄冲突] %d 个独立句柄同基线写: 成功=%d stale=%d", n, ok, stale)

	// 句柄 A 落后，它基于旧基线的任何操作都必须失败；
	// 句柄 B 已确认提交不能被 A 的后到写入吃掉。
	head := tb.CurrentVersion()
	rowsHead := func() []Row {
		r, _ := tb.Head(ctxbg)
		rows, _ := r.Scan(ctxbg)
		return rows
	}
	before := rowsHead()
	for _, h := range handles {
		if _, e := h.Commit(ctxbg, base, OpUpsert,
			[]Row{{"id": int64(9999), "name": "stale-write", "score": int64(99)}}, "stale"); ErrorCode(e) != CodeConflictSnapshotStale {
			t.Fatalf("[用例:陈旧句柄拒写] 旧句柄应一律 stale, 实际 %v", e)
		}
		if _, e := h.Evolve(ctxbg, base, tb.CurrentSchema(), "stale evolve"); ErrorCode(e) != CodeConflictSnapshotStale {
			t.Fatalf("[用例:陈旧句柄拒写] 旧句柄演进应 stale, 实际 %v", e)
		}
		if _, e := h.Compact(ctxbg, base); ErrorCode(e) != CodeConflictSnapshotStale {
			t.Fatalf("[用例:陈旧句柄拒写] 旧句柄整理应 stale, 实际 %v", e)
		}
		if _, e := h.RestoreToVersion(ctxbg, base, base, "stale restore"); ErrorCode(e) != CodeConflictSnapshotStale {
			t.Fatalf("[用例:陈旧句柄拒写] 旧句柄恢复应 stale, 实际 %v", e)
		}
	}
	after := rowsHead()
	if !rowsEqual(before, after) {
		t.Fatalf("[用例:已确认不丢] 陈旧句柄的失败操作不得改动数据: before=%v after=%v", before, after)
	}
	if tb.CurrentVersion() != head {
		t.Fatalf("[用例:已确认不丢] current 不应被失败操作推进")
	}
	t.Logf("[用例:陈旧句柄拒写] 落后句柄的提交/演进/整理/恢复全部 CONFLICT_SNAPSHOT_STALE，已确认数据原样保留")

	// 落后句柄刷新感知到新版本后可正常重试成功，且 seed 行与胜出者行都在。
	laggard := handles[0]
	vRetry, err := laggard.Commit(ctxbg, laggard.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(2000), "name": "retry-handle", "score": int64(1)}}, "laggard retries")
	if err != nil {
		t.Fatal(err)
	}
	r, _ := laggard.Snapshot(ctxbg, vRetry)
	rows, _ := r.Scan(ctxbg)
	names := map[string]bool{}
	for _, row := range rows {
		names[fmt.Sprint(row["name"])] = true
	}
	if !names["seed"] || !names["retry-handle"] {
		t.Fatalf("[用例:刷新后续写] seed 与重试行应同时存在, 实际 %v", rows)
	}
	winnerFound := false
	for i := 0; i < n; i++ {
		if names[fmt.Sprintf("h%d", i)] {
			winnerFound = true
		}
	}
	if !winnerFound {
		t.Fatalf("[用例:刷新后续写] 胜出句柄已确认的数据不能丢, 实际 %v", rows)
	}
	t.Logf("[用例:刷新后续写] 落后句柄重读到新基线后提交成功 v%d，历史已确认行全部保留（%d行）",
		vRetry, len(rows))
	t.Log("[业务:多句柄并发] === TestMultiHandleWriters 通过 ===")
}

// multiProcessChild 是多进程并发写的子进程入口。
func TestMultiProcessChild(t *testing.T) {
	if os.Getenv("LAKEHOUSE_MPROC_CHILD") != "1" {
		t.Skip("multiprocess child helper")
	}
	tb, err := Open(ctxbg, os.Getenv("LAKEHOUSE_TABLE_DIR"), nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	base := int64(atoiSafe(os.Getenv("LAKEHOUSE_MPROC_BASE")))
	id := atoiSafe(os.Getenv("LAKEHOUSE_MPROC_ID"))
	_, err = tb.Commit(ctxbg, base, OpUpsert,
		[]Row{{"id": int64(3000 + id), "name": fmt.Sprintf("proc%d", id), "score": int64(id)}},
		fmt.Sprintf("process %d", id))
	if err == nil {
		fmt.Print("COMMITTED")
		os.Exit(0)
	}
	if ErrorCode(err) == CodeConflictSnapshotStale {
		fmt.Print("STALE")
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}

// TestMultiProcessWriters 业务：多个独立进程同时打开同一张表写，
// 同一基线上跨进程仍恰好一个成功（flock + 磁盘 current CAS）。
func TestMultiProcessWriters(t *testing.T) {
	t.Log("[业务:多进程并发] === TestMultiProcessWriters 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	base, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(0), "name": "seed", "score": int64(0)}}, "seed")
	if err != nil {
		t.Fatal(err)
	}
	tableDir := filepath.Join(dir, "table")
	_ = tb.Close()

	const n = 6
	var cmds []*exec.Cmd
	for i := 0; i < n; i++ {
		cmd := exec.Command(os.Args[0], "-test.run", "TestMultiProcessChild")
		cmd.Env = append(os.Environ(),
			"LAKEHOUSE_MPROC_CHILD=1",
			"LAKEHOUSE_TABLE_DIR="+tableDir,
			fmt.Sprintf("LAKEHOUSE_MPROC_BASE=%d", base),
			fmt.Sprintf("LAKEHOUSE_MPROC_ID=%d", i),
		)
		cmds = append(cmds, cmd)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed, stale, failed := 0, 0, 0
	for i, cmd := range cmds {
		i, cmd := i, cmd
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out, e := cmd.Output()
			mu.Lock()
			defer mu.Unlock()
			switch {
			case e == nil && string(out) == "COMMITTED":
				committed++
			case e == nil && string(out) == "STALE":
				stale++
			default:
				failed++
				t.Errorf("[用例:多进程冲突] proc%d 异常: out=%q err=%v", i, string(out), e)
			}
		}()
	}
	close(start)
	wg.Wait()
	if committed != 1 || stale != n-1 || failed != 0 {
		t.Fatalf("[用例:多进程冲突] 期望1成功/%d stale, 实际 committed=%d stale=%d failed=%d",
			n-1, committed, stale, failed)
	}
	t.Logf("[用例:多进程冲突] %d 个进程同基线写: committed=%d stale=%d（flock 互斥 + 磁盘 CAS）",
		n, committed, stale)

	reopened, err := Open(ctxbg, tableDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.CurrentVersion() != base+1 {
		t.Fatalf("[用例:多进程一致] 期望恰好前进到 v%d, 实际 v%d", base+1, reopened.CurrentVersion())
	}
	r, _ := reopened.Head(ctxbg)
	rows, _ := r.Scan(ctxbg)
	if len(rows) != 2 {
		t.Fatalf("[用例:多进程一致] 应只有 seed+胜出者两行（无脏写/覆盖），实际 %d 行 %v", len(rows), rows)
	}
	winnerSeen := false
	for _, row := range rows {
		if name, _ := row["name"].(string); len(name) >= 4 && name[:4] == "proc" {
			winnerSeen = true
		}
	}
	if !winnerSeen {
		t.Fatalf("[用例:多进程一致] 胜出进程的数据必须完整可见: %v", rows)
	}
	t.Logf("[用例:多进程一致] 重开后 current=v%d，仅 seed 与唯一胜出进程两行，已确认数据未被吃掉",
		base+1)

	// 失败进程用新基线重试可成功。
	retry := exec.Command(os.Args[0], "-test.run", "TestMultiProcessChild")
	retry.Env = append(os.Environ(),
		"LAKEHOUSE_MPROC_CHILD=1",
		"LAKEHOUSE_TABLE_DIR="+tableDir,
		fmt.Sprintf("LAKEHOUSE_MPROC_BASE=%d", base+1),
		"LAKEHOUSE_MPROC_ID=99",
	)
	out, err := retry.Output()
	if err != nil || string(out) != "COMMITTED" {
		t.Fatalf("[用例:多进程重试] 新基线上重试应提交成功: out=%q err=%v", string(out), err)
	}
	r2, _ := reopened.Snapshot(ctxbg, base+2)
	rows2, _ := r2.Scan(ctxbg)
	if len(rows2) != 3 {
		t.Fatalf("[用例:多进程重试] 重试后应3行, 实际 %v", rows2)
	}
	t.Logf("[用例:多进程重试] 失败进程基于 v%d 重试成功 -> v%d（3行）", base+1, base+2)
	t.Log("[业务:多进程并发] === TestMultiProcessWriters 通过 ===")
}
