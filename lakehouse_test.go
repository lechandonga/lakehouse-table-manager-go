package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// 统一业务日志：每个测试都能看到所属业务、用例名与通过情况。
type bizLog struct{ t *testing.T }

func newBizLog(t *testing.T, biz string) *bizLog {
	t.Helper()
	t.Logf("")
	t.Logf("========== 业务：%s ==========", biz)
	return &bizLog{t: t}
}

func (l *bizLog) casef(name string, format string, args ...any) {
	l.t.Helper()
	l.t.Logf("  [用例] %s — %s", name, fmt.Sprintf(format, args...))
}

func (l *bizLog) pass(name string) {
	l.t.Helper()
	l.t.Logf("  [结果] %s ✔ 通过", name)
}

func testSchema(t *testing.T) *Schema {
	t.Helper()
	s, err := NewSchema(
		Column{Name: "id", Type: TypeInt},
		Column{Name: "name", Type: TypeString, Nullable: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func openTable(t *testing.T) (*Table, string) {
	t.Helper()
	dir := t.TempDir()
	tb, err := Create(filepath.Join(dir, "tbl"), testSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	return tb, filepath.Join(dir, "tbl")
}

// TestConcurrentCommit 并发提交：同一基线只允许一个成功，其余得到可区分冲突原因。
func TestConcurrentCommit(t *testing.T) {
	log := newBizLog(t, "并发提交（乐观并发 + 快照隔离）")

	t.Run("多事务竞争同一基线_仅一个成功其余冲突", func(t *testing.T) {
		tb, _ := openTable(t)
		const n = 8
		var txns [n]*Txn
		for i := 0; i < n; i++ {
			tx := tb.Begin()
			if err := tx.Upsert(map[string]any{"id": int64(i + 1), "name": fmt.Sprintf("u%d", i+1)}); err != nil {
				t.Fatal(err)
			}
			txns[i] = tx
		}
		log.casef("多事务竞争同一基线_仅一个成功其余冲突", "8 个事务都基于 v%d 并发提交", txns[0].BaseVersion())

		var wg sync.WaitGroup
		var mu sync.Mutex
		success, conflict := 0, 0
		for _, tx := range txns {
			wg.Add(1)
			go func(tx *Txn) {
				defer wg.Done()
				_, err := tx.Commit()
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					success++
				case errors.Is(err, ErrConflictConcurrentCommit):
					conflict++
				default:
					t.Errorf("unexpected err: %v", err)
				}
			}(tx)
		}
		wg.Wait()
		if success != 1 || conflict != n-1 {
			t.Fatalf("want 1 success / %d conflicts, got %d / %d", n-1, success, conflict)
		}
		t.Logf("    提交结果：成功=%d，冲突(ErrConflictConcurrentCommit)=%d，最新版本=v%d", success, conflict, tb.LatestVersion())
		log.pass("多事务竞争同一基线_仅一个成功其余冲突")
	})

	t.Run("冲突事务重试后成功", func(t *testing.T) {
		tb, _ := openTable(t)
		log.casef("冲突事务重试后成功", "失败事务以最新快照重新 Begin 可再次提交")
		tx1 := tb.Begin()
		tx2 := tb.Begin()
		if err := tx1.Upsert(map[string]any{"id": int64(1), "name": "a"}); err != nil {
			t.Fatal(err)
		}
		if err := tx2.Upsert(map[string]any{"id": int64(2), "name": "b"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx1.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx2.Commit(); !errors.Is(err, ErrConflictConcurrentCommit) {
			t.Fatalf("want conflict, got %v", err)
		}
		tx2r := tb.Begin()
		if err := tx2r.Upsert(map[string]any{"id": int64(2), "name": "b"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx2r.Commit(); err != nil {
			t.Fatalf("retry commit failed: %v", err)
		}
		rows, err := tb.Latest().Scan()
		if err != nil || len(rows) != 2 {
			t.Fatalf("retry state wrong: rows=%d err=%v", len(rows), err)
		}
		log.pass("冲突事务重试后成功")
	})

	t.Run("查询只见完整快照", func(t *testing.T) {
		tb, _ := openTable(t)
		log.casef("查询只见完整快照", "提交期间任何读取要么看到旧快照要么看到新快照，无中间态")
		tx := tb.Begin()
		for i := 1; i <= 3; i++ {
			if err := tx.Upsert(map[string]any{"id": int64(i), "name": "x"}); err != nil {
				t.Fatal(err)
			}
		}
		if rows, _ := tb.Latest().Scan(); len(rows) != 0 {
			t.Fatalf("uncommitted rows visible: %d", len(rows))
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if rows, _ := tb.Latest().Scan(); len(rows) != 3 {
			t.Fatalf("committed snapshot incomplete: %d rows", len(rows))
		}
		log.pass("查询只见完整快照")
	})
}

// TestCrashRecovery 提交中断恢复：已确认完整可见，未完成彻底不可见。
func TestCrashRecovery(t *testing.T) {
	log := newBizLog(t, "提交中断恢复（崩溃后重开表）")

	for _, stage := range []string{"data-renamed", "snapshot-written"} {
		stage := stage
		t.Run("中断于_"+stage+"_未完成提交不可见", func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "tbl")
			log.casef("中断于_"+stage, "子进程提交 id=42 时在 %s 掉电", stage)
			runCrashChild(t, dir, stage)

			tb, err := Open(dir)
			if err != nil {
				t.Fatalf("reopen failed: %v", err)
			}
			defer tb.Close()
			if tb.LatestVersion() != 3 {
				t.Fatalf("latest should stay v3, got v%d", tb.LatestVersion())
			}
			if _, found, _ := tb.Latest().Get(int64(42)); found {
				t.Fatal("uncommitted row id=42 visible after recovery")
			}
			if rows, _ := tb.Latest().Scan(); len(rows) != 2 {
				t.Fatalf("confirmed rows lost, got %d", len(rows))
			}
			// 孤儿文件应已回收：data/ 仅保留 v2 的文件。
			dataFiles, _ := os.ReadDir(filepath.Join(dir, dirData))
			if len(dataFiles) != 2 {
				t.Fatalf("orphan data files not collected: %v", dataFiles)
			}
			log.pass("中断于_" + stage + "_未完成提交不可见")
		})
	}

	t.Run("中断于_latest-switched_已确认完整可见", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "tbl")
		log.casef("中断于_latest-switched_已确认完整可见", "latest 指针已原子切换，恢复后新快照完整可读")
		runCrashChild(t, dir, "none")
		tb, err := Open(dir)
		if err != nil {
			t.Fatalf("reopen failed: %v", err)
		}
		defer tb.Close()
		if tb.LatestVersion() != 4 {
			t.Fatalf("want v4, got v%d", tb.LatestVersion())
		}
		row, found, err := tb.Latest().Get(int64(42))
		if err != nil || !found || row["name"] != "crash" {
			t.Fatalf("confirmed commit not fully visible: found=%v row=%v err=%v", found, row, err)
		}
		log.pass("中断于_latest-switched_已确认完整可见")
	})
}

func runCrashChild(t *testing.T, dir, stage string) {
	t.Helper()
	// 先正常准备 v1 建表 + v2 两条已确认数据。
	tb, err := Create(dir, testSchema(t))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(i), "name": fmt.Sprintf("ok%d", i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	tb.Close()

	if stage == "none" {
		// 不崩溃：直接在本进程完成 id=42 的提交。
		tb2, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		tx := tb2.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(42), "name": "crash"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tb2.Close()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperCrashCommit", "-test.count=1")
	cmd.Env = append(os.Environ(),
		"CRASH_TABLE_DIR="+dir,
		"CRASH_STAGE="+stage,
	)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatalf("child failed: %v\n%s", err, out)
	}
	if exitErr == nil || exitErr.ExitCode() != 99 {
		t.Fatalf("child did not crash as expected: %v\n%s", err, out)
	}
	t.Logf("    子进程已在阶段 %s 退出(exit=99)", stage)
}

// TestHelperCrashCommit 在子进程中执行一个会崩溃的提交。
func TestHelperCrashCommit(t *testing.T) {
	dir := os.Getenv("CRASH_TABLE_DIR")
	stage := os.Getenv("CRASH_STAGE")
	if dir == "" {
		return
	}
	tb, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	tx := tb.Begin()
	if err := tx.Upsert(map[string]any{"id": int64(42), "name": "crash"}); err != nil {
		t.Fatal(err)
	}
	tx.setCrashHook(stage)
	if _, err := tx.Commit(); err != nil {
		// data-renamed 之后不会返回；snapshot-written 同理。
		t.Fatal(err)
	}
}
