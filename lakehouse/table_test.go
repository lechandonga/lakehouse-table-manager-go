package lakehouse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var ctxbg = context.Background()

func testSchema(t *testing.T) *Schema {
	t.Helper()
	sch, err := NewSchema([]string{"id"},
		Column{Name: "id", Type: TypeInt, Nullable: false},
		Column{Name: "name", Type: TypeString, Nullable: true},
		Column{Name: "score", Type: TypeInt, Nullable: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func openCreated(t *testing.T, dir string) *Table {
	t.Helper()
	tb, err := Create(ctxbg, filepath.Join(dir, "table"), "users", testSchema(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func rowsEqual(a, b []Row) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if fmt.Sprint(a[i]) != fmt.Sprint(b[i]) {
			return false
		}
	}
	return true
}

func crashChildCmd(dir, stage string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run", "TestCrashRecoveryChild")
	cmd.Env = append(os.Environ(),
		"LAKEHOUSE_CRASH_CHILD=1",
		"LAKEHOUSE_CRASH_STAGE="+stage,
		"LAKEHOUSE_TABLE_DIR="+filepath.Join(dir, "table"),
	)
	return cmd
}

// TestConcurrentCommitConflict 业务：并发提交乐观冲突 + 快照隔离。
// 基于同一快照的 N 个并发提交恰好 1 个成功，其余全部 CONFLICT_SNAPSHOT_STALE；
// 查询永远只看到完整快照；失败方基于新版本重试可成功。
func TestConcurrentCommitConflict(t *testing.T) {
	t.Log("[业务:并发提交] === TestConcurrentCommitConflict 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)

	base := tb.CurrentVersion()
	v0, err := tb.Commit(ctxbg, base, OpUpsert,
		[]Row{{"id": int64(1), "name": "a", "score": int64(10)}}, "seed")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[用例:种子提交] v%d 提交成功；读取者只能看到完整快照", v0)

	const n = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, conflictStale, other := 0, 0, 0
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, cerr := tb.Commit(ctxbg, v0, OpUpsert,
				[]Row{{"id": int64(100 + i), "name": fmt.Sprintf("w%d", i), "score": int64(i)}},
				fmt.Sprintf("concurrent-%d", i))
			mu.Lock()
			defer mu.Unlock()
			switch ErrorCode(cerr) {
			case "":
				ok++
			case CodeConflictSnapshotStale:
				conflictStale++
			default:
				other++
				t.Errorf("[用例:冲突原因] worker %d 非预期错误: %v", i, cerr)
			}
		}()
	}
	close(start)
	wg.Wait()

	if ok != 1 || conflictStale != n-1 || other != 0 {
		t.Fatalf("[用例:冲突结果] 期望恰好1成功/%d过期冲突, 实际 ok=%d stale=%d other=%d",
			n-1, ok, conflictStale, other)
	}
	t.Logf("[用例:冲突结果] %d 个同基快照并发提交: 成功=%d, CONFLICT_SNAPSHOT_STALE=%d",
		n, ok, conflictStale)

	// 中间状态不可见：头部快照恰好包含 seed + 唯一胜出者两行。
	r, err := tb.Head(ctxbg)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Scan(ctxbg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("[用例:快照隔离] 期望2行（无中间状态/脏写），实际 %d 行: %v", len(rows), rows)
	}
	t.Logf("[用例:快照隔离] 头部快照只有完整提交结果: %v", rows)

	newV, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(200), "name": "retry", "score": int64(1)}}, "retry after conflict")
	if err != nil {
		t.Fatalf("[用例:冲突重试] %v", err)
	}
	t.Logf("[用例:冲突重试] 失败方基于最新快照重试成功 -> v%d", newV)
	t.Log("[业务:并发提交] === TestConcurrentCommitConflict 通过 ===")
}

// TestCrashRecoveryChild 是崩溃子进程入口（正常 go test 运行时直接跳过）。
func TestCrashRecoveryChild(t *testing.T) {
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
	_, _ = tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(99), "name": "half-done", "score": int64(9)}}, "crashed commit")
	os.Exit(0)
}

// TestCrashRecovery 业务：提交中断后的重启恢复。
// 在“数据文件写后/快照元数据写后”崩溃：current 未翻转 => 重启后该提交不可见；
// 已确认提交完整可见，表可继续提交；孤儿文件可 GC 回收。
func TestCrashRecovery(t *testing.T) {
	t.Log("[业务:重启恢复] === TestCrashRecovery 开始 ===")
	for _, stage := range []string{"after-data-files", "after-snapshot-meta"} {
		stage := stage
		t.Run("crash_before_publish/"+stage, func(t *testing.T) {
			dir := t.TempDir()
			tb := openCreated(t, dir)
			v, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
				[]Row{{"id": int64(1), "name": "visible", "score": int64(1)}}, "committed")
			if err != nil {
				t.Fatal(err)
			}
			_ = tb.Close()

			cmd := crashChildCmd(dir, stage)
			out, err := cmd.CombinedOutput()
			t.Logf("[用例:子进程崩溃] stage=%s 输出=%q err=%v", stage, string(out), err)

			reopened, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if cv := reopened.CurrentVersion(); cv != v {
				t.Fatalf("[用例:恢复可见性] 期望 current=v%d（崩溃提交不可见），实际 v%d", v, cv)
			}
			r, _ := reopened.Head(ctxbg)
			rows, _ := r.Scan(ctxbg)
			if len(rows) != 1 || rows[0]["name"] != "visible" {
				t.Fatalf("[用例:恢复可见性] 只应看到已确认数据，实际 %v", rows)
			}
			t.Logf("[用例:恢复可见性] stage=%s 重启后 current=v%d 行=%v（未完成改动不可见）", stage, v, rows)

			nv, err := reopened.Commit(ctxbg, reopened.CurrentVersion(), OpUpsert,
				[]Row{{"id": int64(2), "name": "after-recovery", "score": int64(2)}}, "post recovery")
			if err != nil {
				t.Fatal(err)
			}
			if nv != v+1 {
				t.Fatalf("[用例:恢复后提交] 期望 v%d，实际 v%d", v+1, nv)
			}
			t.Logf("[用例:恢复后提交] 恢复后可正常提交 -> v%d（无重复版本/空洞问题）", nv)

			n, err := reopened.GCUnreferencedFiles(ctxbg)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Fatalf("[用例:孤儿回收] 期望回收崩溃提交遗留文件")
			}
			r2, _ := reopened.Head(ctxbg)
			rows2, _ := r2.Scan(ctxbg)
			if len(rows2) != 2 {
				t.Fatalf("[用例:孤儿回收] GC 后读取应不变，实际 %v", rows2)
			}
			t.Logf("[用例:孤儿回收] 回收 %d 个未引用文件，读取结果不变(%d行)", n, len(rows2))
		})
	}
	t.Log("[业务:重启恢复] === TestCrashRecovery 通过 ===")
}

// TestSchemaEvolution 业务：加列/删列/改名 + 历史读取 + 不安全变更拒绝。
func TestSchemaEvolution(t *testing.T) {
	t.Log("[业务:结构演进] === TestSchemaEvolution 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)

	v1, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert, []Row{
		{"id": int64(1), "name": "alice", "score": int64(10)},
		{"id": int64(2), "name": "bob", "score": int64(20)},
	}, "initial data")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[用例:初始数据] 提交 -> v%d", v1)

	// 加列：历史快照仍绑定旧 schema（无 city）；加列后版本里历史行 city=nil，新行有值。
	vAdd, err := tb.AddColumn(ctxbg, tb.CurrentVersion(), Column{Name: "city", Type: TypeString, Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	oldReader, err := tb.Snapshot(ctxbg, v1)
	if err != nil {
		t.Fatal(err)
	}
	oldRows, _ := oldReader.Scan(ctxbg)
	if _, has := oldRows[0]["city"]; has {
		t.Fatalf("[用例:历史一致] v%d 快照不应出现后加的 city 列", v1)
	}
	t.Logf("[用例:历史一致] v%d 绑定提交当时的 schema: %v", v1, oldRows)

	vData, err := tb.Commit(ctxbg, vAdd, OpUpsert, []Row{
		{"id": int64(3), "name": "carol", "score": int64(30), "city": "SH"},
	}, "with city")
	if err != nil {
		t.Fatal(err)
	}
	rAdd, _ := tb.Snapshot(ctxbg, vData)
	rowsAdd, _ := rAdd.Scan(ctxbg)
	if rowsAdd[0]["city"] != nil || rowsAdd[2]["city"] != "SH" {
		t.Fatalf("[用例:加列缺省值] 期望历史行 city=nil、新行 city=SH, 实际 %v", rowsAdd)
	}
	t.Logf("[用例:加列缺省值] 加列后历史行 city=nil（确定稳定缺省），新行 city=SH: %v", rowsAdd)

	// 改名：列 ID 不变，物理文件按 ID 对齐，绝不静默错列。
	vRen, err := tb.RenameColumn(ctxbg, tb.CurrentVersion(), "score", "points")
	if err != nil {
		t.Fatal(err)
	}
	head, _ := tb.Head(ctxbg)
	if _, err := head.Project(ctxbg, []string{"score"}); ErrorCode(err) != CodeSchemaColumnRenamed {
		t.Fatalf("[用例:改名防错列] 按旧名投影期望 SCHEMA_COLUMN_RENAMED, 实际 %v", err)
	}
	if _, err := head.Get(ctxbg, Row{"score": int64(1)}); ErrorCode(err) != CodeSchemaColumnRenamed {
		t.Fatalf("[用例:改名防错列] 按旧名查询期望 SCHEMA_COLUMN_RENAMED, 实际 %v", err)
	}
	proj, err := head.Project(ctxbg, []string{"id", "points"})
	if err != nil {
		t.Fatal(err)
	}
	if proj[0]["points"] != int64(10) {
		t.Fatalf("[用例:改名不错列] id=1 的 points 应按列ID对齐为10, 实际 %v", proj[0])
	}
	t.Logf("[用例:改名不错列] score->points 按列ID对齐: %v；按旧名读取得到可区分错误", proj)

	if _, err := tb.Commit(ctxbg, vRen, OpUpsert,
		[]Row{{"id": int64(4), "name": "dan", "score": int64(40)}}, "old name write"); ErrorCode(err) != CodeSchemaColumnRenamed {
		t.Fatalf("[用例:改名防错列] 用旧名写入应失败 RENAMED, 实际 %v", err)
	}
	t.Log("[用例:改名防错列] 改名后用旧名写入被拒绝（SCHEMA_COLUMN_RENAMED）")

	histAtV1, _ := tb.Snapshot(ctxbg, v1)
	hrows, _ := histAtV1.Project(ctxbg, []string{"id", "score"})
	if hrows[0]["score"] != int64(10) {
		t.Fatalf("[用例:历史schema] v1 旧名读取应保持原状, 实际 %v", hrows)
	}
	t.Logf("[用例:历史schema] v%d 仍可按当时列名 score 读取: %v", v1, hrows)

	// 删列：结果中无残留；按名访问得到 SCHEMA_COLUMN_NOT_FOUND。
	vDrop, err := tb.DropColumn(ctxbg, tb.CurrentVersion(), "city")
	if err != nil {
		t.Fatal(err)
	}
	head2, _ := tb.Snapshot(ctxbg, vDrop)
	rows2, _ := head2.Scan(ctxbg)
	for _, row := range rows2 {
		if _, leaked := row["city"]; leaked {
			t.Fatalf("[用例:删列残留] 结果中不得出现已删除列 city: %v", row)
		}
	}
	if _, err := head2.Project(ctxbg, []string{"city"}); ErrorCode(err) != CodeSchemaColumnNotFound {
		t.Fatalf("[用例:删列残留] 投影已删列应得到 NOT_FOUND, 实际 %v", err)
	}
	t.Logf("[用例:删列残留] 删列后历史文件中的 city 数据被按ID忽略, 行数=%d 且无 city 列", len(rows2))

	// 同名加列得到全新列 ID，历史数据不会把旧值串到新列（一律 nil）。
	vReAdd, err := tb.AddColumn(ctxbg, tb.CurrentVersion(), Column{Name: "city", Type: TypeInt, Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	r3, _ := tb.Snapshot(ctxbg, vReAdd)
	rows3, _ := r3.Scan(ctxbg)
	for _, row := range rows3 {
		if row["city"] != nil {
			t.Fatalf("[用例:删列后同名列] 重新加的 city 必须是全新ID，历史值不得串列: %v", row)
		}
	}
	t.Logf("[用例:删列后同名列] 重新加列 city(新ID) 历史读取全部为 nil: %d 行", len(rows3))

	// 无法安全支持的变更：类型修改、删除/改名主键、重名列。
	cur := tb.CurrentSchema()
	if _, err := cur.ChangeType("points", TypeString); ErrorCode(err) != CodeSchemaTypeChange {
		t.Fatalf("[用例:拒绝不安全变更] 改类型应被拒绝 TYPE_CHANGE, 实际 %v", err)
	}
	if _, err := tb.DropColumn(ctxbg, tb.CurrentVersion(), "id"); ErrorCode(err) != CodeSchemaIsPrimaryKey {
		t.Fatalf("[用例:拒绝不安全变更] 删主键应被拒绝, 实际 %v", err)
	}
	if _, err := tb.RenameColumn(ctxbg, tb.CurrentVersion(), "id", "uid"); ErrorCode(err) != CodeSchemaIsPrimaryKey {
		t.Fatalf("[用例:拒绝不安全变更] 主键改名应被拒绝, 实际 %v", err)
	}
	if _, err := tb.AddColumn(ctxbg, tb.CurrentVersion(), Column{Name: "points", Type: TypeInt}); ErrorCode(err) != CodeSchemaColumnExists {
		t.Fatalf("[用例:拒绝不安全变更] 重名列应被拒绝, 实际 %v", err)
	}
	t.Log("[用例:拒绝不安全变更] 类型修改/删主键/改主键名/重名列均被明确拒绝并带可区分原因")

	oldest, _ := tb.Snapshot(ctxbg, v1)
	orows, _ := oldest.Scan(ctxbg)
	if len(orows) != 2 {
		t.Fatalf("[用例:最老历史] v1 应仍是2行, 实际 %v", orows)
	}
	t.Logf("[用例:最老历史] 多次演进后 v%d 读取依旧: %v", v1, orows)
	t.Log("[业务:结构演进] === TestSchemaEvolution 通过 ===")
}

// TestUpdateDeleteCompact 业务：按主键更新/删除、确定性重复写入、整理与历史读取。
func TestUpdateDeleteCompact(t *testing.T) {
	t.Log("[业务:更新删除与整理] === TestUpdateDeleteCompact 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)

	// 批内重复主键：后者确定性覆盖前者。
	v1, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert, []Row{
		{"id": int64(1), "name": "first", "score": int64(1)},
		{"id": int64(1), "name": "second", "score": int64(2)},
		{"id": int64(2), "name": "bob", "score": int64(20)},
	}, "duplicate in batch")
	if err != nil {
		t.Fatal(err)
	}
	r1, _ := tb.Snapshot(ctxbg, v1)
	row1, _ := r1.Get(ctxbg, Row{"id": int64(1)})
	if row1["name"] != "second" || row1["score"] != int64(2) {
		t.Fatalf("[用例:批内确定性] 同主键最后一条应胜出, 实际 %v", row1)
	}
	t.Logf("[用例:批内确定性] 批内重复主键 id=1 最后写入胜出: %v", row1)

	// 跨快照更新：后提交覆盖先提交。
	v2, err := tb.Commit(ctxbg, v1, OpUpsert, []Row{
		{"id": int64(1), "name": "updated", "score": int64(3)},
	}, "update id=1")
	if err != nil {
		t.Fatal(err)
	}
	r2, _ := tb.Snapshot(ctxbg, v2)
	row1, _ = r2.Get(ctxbg, Row{"id": int64(1)})
	if row1["name"] != "updated" {
		t.Fatalf("[用例:跨快照更新] 期望 updated, 实际 %v", row1)
	}
	row2, _ := r2.Get(ctxbg, Row{"id": int64(2)})
	if row2["name"] != "bob" {
		t.Fatalf("[用例:跨快照更新] 未触及行应保持, 实际 %v", row2)
	}
	t.Logf("[用例:跨快照更新] id=1 更新为 %v；id=2 保持 %v", row1, row2)

	// 删除标记可靠生效。
	v3, err := tb.Commit(ctxbg, v2, OpDelete, []Row{{"id": int64(2)}}, "delete id=2")
	if err != nil {
		t.Fatal(err)
	}
	r3, _ := tb.Snapshot(ctxbg, v3)
	gone, err := r3.Get(ctxbg, Row{"id": int64(2)})
	if err != nil || gone != nil {
		t.Fatalf("[用例:删除生效] id=2 应已删除, 实际 %v err=%v", gone, err)
	}
	all3, _ := r3.Scan(ctxbg)
	if len(all3) != 1 {
		t.Fatalf("[用例:删除生效] 期望仅剩1行, 实际 %v", all3)
	}
	t.Logf("[用例:删除生效] 删除 id=2 后仅剩: %v", all3)

	// 删除后重新插入同主键可复活。
	v4, err := tb.Commit(ctxbg, v3, OpUpsert, []Row{
		{"id": int64(2), "name": "resurrected", "score": int64(21)},
	}, "reinsert id=2")
	if err != nil {
		t.Fatal(err)
	}
	r4, _ := tb.Snapshot(ctxbg, v4)
	back, _ := r4.Get(ctxbg, Row{"id": int64(2)})
	if back == nil || back["name"] != "resurrected" {
		t.Fatalf("[用例:删除后重插] 期望复活为 resurrected, 实际 %v", back)
	}
	t.Logf("[用例:删除后重插] id=2 重新插入后复活: %v", back)

	// 记录整理前每个历史版本的结果，整理后逐一比对。
	before := map[int64][]Row{}
	for _, v := range []int64{1, 2, 3, 4} {
		rr, _ := tb.Snapshot(ctxbg, v)
		before[v], _ = rr.Scan(ctxbg)
	}

	vc, err := tb.Compact(ctxbg, v4)
	if err != nil {
		t.Fatal(err)
	}
	rc, _ := tb.Snapshot(ctxbg, vc)
	compacted, _ := rc.Scan(ctxbg)
	if len(compacted) != 2 || compacted[0]["id"] != int64(1) || compacted[1]["id"] != int64(2) {
		t.Fatalf("[用例:整理结果] 期望按主键排序的2行, 实际 %v", compacted)
	}
	t.Logf("[用例:整理结果] 整理后单文件快照: %v（墓碑行不落盘）", compacted)

	for v, want := range before {
		rr, _ := tb.Snapshot(ctxbg, v)
		got, _ := rr.Scan(ctxbg)
		if !rowsEqual(want, got) {
			t.Fatalf("[用例:整理不改历史] v%d 整理后结果改变: want=%v got=%v", v, want, got)
		}
	}
	t.Log("[用例:整理不改历史] v1~v4 每个历史版本查询结果与整理前完全一致")

	// 整理后仍可更新/删除，删除标记继续可靠生效。
	v5, err := tb.Commit(ctxbg, vc, OpDelete, []Row{{"id": int64(1)}}, "delete after compact")
	if err != nil {
		t.Fatal(err)
	}
	r5, _ := tb.Snapshot(ctxbg, v5)
	if g, _ := r5.Get(ctxbg, Row{"id": int64(1)}); g != nil {
		t.Fatalf("[用例:整理后删除] id=1 应已删除, 实际 %v", g)
	}
	t.Log("[用例:整理后删除] 整理后删除 id=1 依然可靠生效")

	if _, err := tb.Compact(ctxbg, v4); ErrorCode(err) != CodeConflictSnapshotStale {
		t.Fatalf("[用例:整理冲突] 过期基版本整理应冲突, 实际 %v", err)
	}
	t.Log("[用例:整理冲突] 基于过期快照整理被拒绝（CONFLICT_SNAPSHOT_STALE）")
	t.Log("[业务:更新删除与整理] === TestUpdateDeleteCompact 通过 ===")
}

// TestSnapshotIsolationAndTimeTravel 业务：旧 Reader 不受新提交影响；非法版本报错。
func TestSnapshotIsolationAndTimeTravel(t *testing.T) {
	t.Log("[业务:快照隔离与历史读取] === TestSnapshotIsolationAndTimeTravel 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)

	v1, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert, []Row{
		{"id": int64(1), "name": "a", "score": int64(1)},
	}, "v1")
	if err != nil {
		t.Fatal(err)
	}
	r1, err := tb.Snapshot(ctxbg, v1)
	if err != nil {
		t.Fatal(err)
	}
	want1, _ := r1.Scan(ctxbg)

	v2, err := tb.Commit(ctxbg, v1, OpUpsert, []Row{
		{"id": int64(2), "name": "b", "score": int64(2)},
	}, "v2")
	if err != nil {
		t.Fatal(err)
	}
	v3, err := tb.Commit(ctxbg, v2, OpDelete, []Row{{"id": int64(1)}}, "v3 delete")
	if err != nil {
		t.Fatal(err)
	}

	// 旧 reader 看到的是打开时的完整快照，不受 v2/v3 影响。
	got1, _ := r1.Scan(ctxbg)
	if !rowsEqual(want1, got1) || len(got1) != 1 {
		t.Fatalf("[用例:旧视图冻结] 旧 reader 应冻结在 v1, 实际 %v", got1)
	}
	t.Logf("[用例:旧视图冻结] v1 reader 在 v2/v3 提交后仍返回 v1 内容: %v", got1)

	r3, _ := tb.Snapshot(ctxbg, v3)
	all, _ := r3.Scan(ctxbg)
	if len(all) != 1 || all[0]["id"] != int64(2) {
		t.Fatalf("[用例:历史版本] v3 应只剩 id=2, 实际 %v", all)
	}
	t.Logf("[用例:历史版本] 可定点读取任意历史版本 v%d: %v", v3, all)

	if _, err := tb.Snapshot(ctxbg, v3+1); ErrorCode(err) != CodeSnapshotNotFound {
		t.Fatalf("[用例:非法版本] 未来版本应报 SNAPSHOT_NOT_FOUND, 实际 %v", err)
	}
	t.Log("[用例:非法版本] 查询不存在版本返回 SNAPSHOT_NOT_FOUND")
	t.Log("[业务:快照隔离与历史读取] === TestSnapshotIsolationAndTimeTravel 通过 ===")
}
