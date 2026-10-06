package lakehouse

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// TestRestoreVersion 业务：按历史版本恢复。
// 恢复后当前表（列+数据）与目标历史版本逐行一致；恢复是新提交、不改写历史；
// 恢复前状态仍可读回；版本号继续递增不重用；目标缺失/已过期给出可分辨错误。
func TestRestoreVersion(t *testing.T) {
	t.Log("[业务:版本恢复] === TestRestoreVersion 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)

	v1, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert, []Row{
		{"id": int64(1), "name": "alice", "score": int64(10)},
		{"id": int64(2), "name": "bob", "score": int64(20)},
	}, "v1 good state")
	if err != nil {
		t.Fatal(err)
	}
	vAdd, err := tb.AddColumn(ctxbg, tb.CurrentVersion(),
		Column{Name: "city", Type: TypeString, Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	vBad, err := tb.Commit(ctxbg, vAdd, OpUpsert, []Row{
		{"id": int64(3), "name": "mistake", "score": int64(300), "city": "X"},
	}, "v2 bad change")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[用例:制造历史] 正确状态 v%d（id/name/score 三列），错误状态 v%d（加列后）", v1, vBad)

	// 目标版本不存在 / 已过期的错误可分辨。
	if _, err := tb.RestoreToVersion(ctxbg, tb.CurrentVersion(), vBad+50, ""); ErrorCode(err) != CodeSnapshotNotFound {
		t.Fatalf("[用例:恢复目标校验] 不存在版本应 SNAPSHOT_NOT_FOUND, 实际 %v", err)
	}
	t.Log("[用例:恢复目标校验] 恢复压根不存在的版本 -> SNAPSHOT_NOT_FOUND")

	vR, err := tb.RestoreToVersion(ctxbg, tb.CurrentVersion(), v1, "rollback bad change")
	if err != nil {
		t.Fatal(err)
	}
	if vR <= vBad {
		t.Fatalf("[用例:版本递增] 恢复版本必须继续递增, vR=%d vBad=%d", vR, vBad)
	}
	if from, ok := tb.RestoredFrom(vR); !ok || from != v1 {
		t.Fatalf("[用例:恢复来源] 期望 v%d 记录来源 v%d", vR, v1)
	}
	t.Logf("[用例:恢复提交] 恢复到 v%d 产生新版本 v%d（不重用旧号），来源可追溯", v1, vR)

	// 当前表与“读目标历史版本”逐行一致（包括可见列：city 必须消失）。
	head, _ := tb.Head(ctxbg)
	got, _ := head.Scan(ctxbg)
	targetReader, _ := tb.Snapshot(ctxbg, v1)
	want, _ := targetReader.Scan(ctxbg)
	if !rowsEqual(want, got) {
		t.Fatalf("[用例:恢复一致] 当前=%v 目标v%d=%v", got, v1, want)
	}
	if _, hasCity := got[0]["city"]; hasCity {
		t.Fatalf("[用例:恢复列一致] 恢复后 city 列必须随 v1 schema 消失, 实际 %v", got[0])
	}
	if _, err := head.Project(ctxbg, []string{"city"}); ErrorCode(err) != CodeSchemaColumnNotFound {
		t.Fatalf("[用例:恢复列一致] 按 city 投影应 NOT_FOUND, 实际 %v", err)
	}
	t.Logf("[用例:恢复一致] 当前表行=%v，列与 v%d 完全一致（无 city）", got, v1)

	// 恢复不改写任何历史：vBad 与恢复前最新状态仍可读回。
	badReader, _ := tb.Snapshot(ctxbg, vBad)
	badRows, _ := badReader.Scan(ctxbg)
	found := false
	for _, r := range badRows {
		if r["name"] == "mistake" && r["city"] == "X" {
			found = true
		}
	}
	if !found {
		t.Fatalf("[用例:历史不改写] v%d 恢复后仍应读到错误状态行, 实际 %v", vBad, badRows)
	}
	t.Logf("[用例:历史不改写] 恢复后 v%d 内容原样可读: %v", vBad, badRows)

	// 恢复后继续正常提交（版本号在 vR 之后继续走）。
	vNext, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(4), "name": "new", "score": int64(40)}}, "after restore")
	if err != nil {
		t.Fatal(err)
	}
	if vNext != vR+1 {
		t.Fatalf("[用例:恢复后提交] 期望 v%d, 实际 v%d", vR+1, vNext)
	}
	head2, _ := tb.Head(ctxbg)
	rows2, _ := head2.Scan(ctxbg)
	if len(rows2) != 3 {
		t.Fatalf("[用例:恢复后提交] 期望3行, 实际 %v", rows2)
	}
	t.Logf("[用例:恢复后提交] 恢复后新提交 -> v%d, 当前 %d 行", vNext, len(rows2))

	// 再恢复回“错误状态”验证双向时间旅行也不重用号。
	vR2, err := tb.RestoreToVersion(ctxbg, tb.CurrentVersion(), vBad, "")
	if err != nil {
		t.Fatal(err)
	}
	head3, _ := tb.Head(ctxbg)
	rows3, _ := head3.Scan(ctxbg)
	if _, ok := rows3[0]["city"]; !ok {
		t.Fatalf("[用例:恢复加列状态] 恢复到 v%d 后 city 列应回来, 实际 %v", vBad, rows3)
	}
	if vR2 != vNext+1 {
		t.Fatalf("[用例:版本递增] 第二次恢复应得 v%d, 实际 v%d", vNext+1, vR2)
	}
	t.Logf("[用例:恢复加列状态] 再恢复到 v%d -> v%d，city 列随 schema 回来: %d 行",
		vBad, vR2, len(rows3))
	t.Log("[业务:版本恢复] === TestRestoreVersion 通过 ===")
}

// TestRestoreExpiredTarget 业务：恢复已被清理的版本必须被明确拒绝。
func TestRestoreExpiredTarget(t *testing.T) {
	t.Log("[业务:版本恢复] === TestRestoreExpiredTarget 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	versions := buildLinearVersions(t, tb, 6)
	if err := tb.SetRetentionPolicy(ctxbg, tb.CurrentVersion(), RetentionPolicy{KeepVersions: 2}); err != nil {
		t.Fatal(err)
	}
	rep, err := tb.ExpireOldVersions(ctxbg, tb.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[用例:前置清理] 清理版本 %v", rep.ExpiredVersions)
	expired := versions[0]
	if _, err := tb.RestoreToVersion(ctxbg, tb.CurrentVersion(), expired, ""); ErrorCode(err) != CodeSnapshotExpired {
		t.Fatalf("[用例:恢复已清理版本] 期望 SNAPSHOT_EXPIRED, 实际 %v", err)
	}
	t.Logf("[用例:恢复已清理版本] 恢复已清理的 v%d -> SNAPSHOT_EXPIRED（与不存在可分辨）", expired)
	t.Log("[业务:版本恢复] === TestRestoreExpiredTarget 通过 ===")
}

// TestRestoreConflict 业务：恢复与其他改表操作共用 CAS，过期基版本冲突。
func TestRestoreConflict(t *testing.T) {
	t.Log("[业务:版本恢复] === TestRestoreConflict 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	v1, _ := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(1), "name": "a", "score": int64(1)}}, "v1")
	stale := v1
	v2, _ := tb.Commit(ctxbg, v1, OpUpsert,
		[]Row{{"id": int64(2), "name": "b", "score": int64(2)}}, "v2")
	if _, err := tb.RestoreToVersion(ctxbg, stale, v1, ""); ErrorCode(err) != CodeConflictSnapshotStale {
		t.Fatalf("[用例:恢复冲突] 过期基版本恢复应 CONFLICT_SNAPSHOT_STALE, 实际 %v", err)
	}
	t.Logf("[用例:恢复冲突] 基版本 v%d 已过期（当前 v%d）-> CONFLICT_SNAPSHOT_STALE", stale, v2)

	// 恢复进行中，并发的普通提交要么先成功（恢复变过期失败）要么后到（恢复成功后它过期）。
	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[string]int{}
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, e := tb.RestoreToVersion(ctxbg, v2, v1, "race restore")
		mu.Lock()
		defer mu.Unlock()
		if e == nil {
			codes["restore-ok"]++
		} else if ErrorCode(e) == CodeConflictSnapshotStale {
			codes["restore-stale"]++
		} else {
			codes["restore-other"]++
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		_, e := tb.Commit(ctxbg, v2, OpUpsert,
			[]Row{{"id": int64(3), "name": "c", "score": int64(3)}}, "race commit")
		mu.Lock()
		defer mu.Unlock()
		if e == nil {
			codes["commit-ok"]++
		} else if ErrorCode(e) == CodeConflictSnapshotStale {
			codes["commit-stale"]++
		} else {
			codes["commit-other"]++
		}
	}()
	close(start)
	wg.Wait()
	if codes["restore-other"] != 0 || codes["commit-other"] != 0 {
		t.Fatalf("[用例:恢复并发] 出现非预期错误: %v", codes)
	}
	if codes["restore-ok"]+codes["commit-ok"] != 1 {
		t.Fatalf("[用例:恢复并发] 同基线下恢复与提交应恰好一个成功, 实际 %v", codes)
	}
	t.Logf("[用例:恢复并发] 恢复与普通提交同基线竞争: %v（恰好一个成功，另一个可分辨失败）", codes)
	t.Log("[业务:版本恢复] === TestRestoreConflict 通过 ===")
}

func buildLinearVersions(t *testing.T, tb *Table, n int) []int64 {
	t.Helper()
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		v, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
			[]Row{{"id": int64(i + 1), "name": fmt.Sprintf("r%d", i+1), "score": int64(i + 1)}},
			fmt.Sprintf("v%d data", i+1))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

func restoreCrashChildCmd(dir, stage string, target int64) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run", "TestLifecycleCrashChild")
	cmd.Env = append(os.Environ(),
		"LAKEHOUSE_CRASH_CHILD=1",
		"LAKEHOUSE_CRASH_STAGE="+stage,
		"LAKEHOUSE_TABLE_DIR="+filepath.Join(dir, "table"),
		fmt.Sprintf("LAKEHOUSE_RESTORE_TARGET=%d", target),
	)
	return cmd
}

func expireCrashChildCmd(dir, stage string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run", "TestLifecycleCrashChild")
	cmd.Env = append(os.Environ(),
		"LAKEHOUSE_CRASH_CHILD=1",
		"LAKEHOUSE_CRASH_STAGE="+stage,
		"LAKEHOUSE_TABLE_DIR="+filepath.Join(dir, "table"),
		"LAKEHOUSE_DO_EXPIRE=1",
	)
	return cmd
}

// TestLifecycleCrashChild 是恢复/清理中断测试的子进程入口（普通 go test 时跳过）。
func TestLifecycleCrashChild(t *testing.T) {
	if os.Getenv("LAKEHOUSE_CRASH_CHILD") != "1" {
		t.Skip("crash child helper")
	}
	stage := os.Getenv("LAKEHOUSE_CRASH_STAGE")
	tb, err := Open(ctxbg, os.Getenv("LAKEHOUSE_TABLE_DIR"), &Options{
		CrashHook: func(s string) error {
			if s == stage {
				os.Exit(0)
			}
			return nil
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if os.Getenv("LAKEHOUSE_DO_EXPIRE") == "1" {
		if _, err := tb.ExpireOldVersions(ctxbg, tb.CurrentVersion()); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	} else {
		target := int64(atoiSafe(os.Getenv("LAKEHOUSE_RESTORE_TARGET")))
		if _, err := tb.RestoreToVersion(ctxbg, tb.CurrentVersion(), target, "crashing restore"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	os.Exit(0)
}

func atoiSafe(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}
