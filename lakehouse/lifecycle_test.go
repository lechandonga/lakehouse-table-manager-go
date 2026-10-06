package lakehouse

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func timeSleep(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }

// buildHistory 构造一段含 upsert/delete/结构演进/整理的历史，记录各版本期望行。
func buildHistory(t *testing.T, tb *Table) map[int64][]Row {
	t.Helper()
	want := map[int64][]Row{}
	commit := func(op Op, rows []Row, msg string) int64 {
		v, err := tb.Commit(ctxbg, tb.CurrentVersion(), op, rows, msg)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	v := commit(OpUpsert, []Row{
		{"id": int64(1), "name": "a1", "score": int64(10)},
		{"id": int64(2), "name": "b1", "score": int64(20)},
	}, "seed")
	want[v] = snapshotRows(t, tb, v)

	v = commit(OpUpsert, []Row{{"id": int64(1), "name": "a2", "score": int64(11)}}, "update 1")
	want[v] = snapshotRows(t, tb, v)

	v = commit(OpDelete, []Row{{"id": int64(2)}}, "delete 2")
	want[v] = snapshotRows(t, tb, v)

	vc, err := tb.AddColumn(ctxbg, tb.CurrentVersion(),
		Column{Name: "city", Type: TypeString, Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	want[vc] = snapshotRows(t, tb, vc)

	v = commit(OpUpsert, []Row{
		{"id": int64(3), "name": "c1", "score": int64(30), "city": "SH"},
	}, "insert 3")
	want[v] = snapshotRows(t, tb, v)

	vk, err := tb.Compact(ctxbg, tb.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	want[vk] = snapshotRows(t, tb, vk)
	return want
}

func snapshotRows(t *testing.T, tb *Table, v int64) []Row {
	t.Helper()
	r, err := tb.Snapshot(ctxbg, v)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Scan(ctxbg)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func headRows(t *testing.T, tb *Table) []Row {
	t.Helper()
	r, err := tb.Head(ctxbg)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.Scan(ctxbg)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func countDataFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "table", "data"))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func lifecycleChildCmd(dir, op, stage, arg string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run", "TestLifecycleChild")
	cmd.Env = append(os.Environ(),
		"LAKEHOUSE_LC_CHILD=1",
		"LAKEHOUSE_LC_OP="+op,
		"LAKEHOUSE_LC_STAGE="+stage,
		"LAKEHOUSE_LC_ARG="+arg,
		"LAKEHOUSE_TABLE_DIR="+filepath.Join(dir, "table"),
	)
	return cmd
}

// TestLifecycleChild 是恢复/清理崩溃的子进程入口（正常 go test 直接跳过）。
func TestLifecycleChild(t *testing.T) {
	if os.Getenv("LAKEHOUSE_LC_CHILD") != "1" {
		t.Skip("lifecycle child helper")
	}
	stage := os.Getenv("LAKEHOUSE_LC_STAGE")
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
	switch os.Getenv("LAKEHOUSE_LC_OP") {
	case "restore":
		var target int64
		fmt.Sscanf(os.Getenv("LAKEHOUSE_LC_ARG"), "%d", &target)
		_, _ = tb.Restore(ctxbg, tb.CurrentVersion(), target, "child restore")
	case "purge":
		_, _ = tb.PurgeExpired(ctxbg)
	}
	os.Exit(0)
}

// TestRestoreVersion 业务：按历史版本恢复（新提交、不改写历史、schema 一起回退、
// 恢复前最新状态仍可读回、版本号只增不复用、目标缺失/冲突可分辨）。
func TestRestoreVersion(t *testing.T) {
	t.Log("[业务:版本恢复] === TestRestoreVersion 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	want := buildHistory(t, tb)
	headBefore := tb.CurrentVersion()

	// 选加列之前的版本 v2 作为恢复目标：恢复后 city 列必须一起消失。
	var target int64 = 2
	targetRows := want[target]
	targetCols := map[string]bool{}
	for _, c := range testSchema(t).Columns {
		targetCols[c.Name] = true
	}

	vr, err := tb.Restore(ctxbg, headBefore, target, fmt.Sprintf("restore to v%d", target))
	if err != nil {
		t.Fatal(err)
	}
	if vr != headBefore+1 {
		t.Fatalf("[用例:恢复即新提交] 期望新版本 v%d, 实际 v%d", headBefore+1, vr)
	}
	t.Logf("[用例:恢复即新提交] 恢复 v%d -> 新提交 v%d（编号只增不复用）", target, vr)

	// 直接读当前表必须与读目标版本逐行、逐列一致。
	head := headRows(t, tb)
	if !rowsEqual(head, targetRows) {
		t.Fatalf("[用例:逐行一致] 恢复后当前表 %v != 目标 v%d %v", head, target, targetRows)
	}
	for name := range head[0] {
		if !targetCols[name] {
			t.Fatalf("[用例:schema回退] 恢复后不应出现目标版本没有的列 %q: %v", name, head[0])
		}
	}
	hr, _ := tb.Head(ctxbg)
	if _, err := hr.Project(ctxbg, []string{"city"}); ErrorCode(err) != CodeSchemaColumnNotFound {
		t.Fatalf("[用例:schema回退] 恢复后 city 列应 NOT_FOUND, 实际 %v", err)
	}
	t.Logf("[用例:逐行一致] 恢复后读当前表与 v%d 完全一致（含列集合）: %v", target, head)

	// 历史一律未被改写：恢复前最新版本仍能读回当时的样子（含 city）。
	beforeRows := snapshotRows(t, tb, headBefore)
	if rowsEqual(beforeRows, head) {
		t.Fatalf("[用例:历史不改写] v%d（恢复前最新）内容不应等于恢复结果", headBefore)
	}
	if _, hasCity := beforeRows[0]["city"]; !hasCity {
		t.Fatalf("[用例:历史不改写] v%d 仍应有 city 列", headBefore)
	}
	t.Logf("[用例:历史不改写] 恢复前最新 v%d 仍可原样读回: %v", headBefore, beforeRows)

	// 目标版本自身读取结果不变。
	if again := snapshotRows(t, tb, target); !rowsEqual(again, targetRows) {
		t.Fatalf("[用例:历史不改写] 目标 v%d 读取被改写", target)
	}

	// 恢复后可继续正常提交。
	vNext, err := tb.Commit(ctxbg, vr, OpUpsert,
		[]Row{{"id": int64(9), "name": "post-restore", "score": int64(90)}}, "after restore")
	if err != nil {
		t.Fatal(err)
	}
	if vNext != vr+1 {
		t.Fatalf("[用例:恢复后写入] 期望 v%d, 实际 v%d", vr+1, vNext)
	}
	t.Logf("[用例:恢复后写入] 恢复后继续提交 -> v%d，版本号连续", vNext)

	// 过期基版本的恢复必须冲突失败。
	if _, err := tb.Restore(ctxbg, headBefore, target, "stale restore"); ErrorCode(err) != CodeConflictSnapshotStale {
		t.Fatalf("[用例:恢复冲突] 过期基版本恢复应 STALE, 实际 %v", err)
	}
	t.Log("[用例:恢复冲突] 基于过期快照恢复被拒绝（CONFLICT_SNAPSHOT_STALE）")

	// 目标版本不存在 / 未来版本：可分辨原因。
	if _, err := tb.Restore(ctxbg, tb.CurrentVersion(), tb.CurrentVersion()+10, "future"); ErrorCode(err) != CodeSnapshotNotFound {
		t.Fatalf("[用例:恢复目标校验] 不存在版本应 NOT_FOUND, 实际 %v", err)
	}
	t.Log("[用例:恢复目标校验] 恢复不存在版本 -> SNAPSHOT_NOT_FOUND")
	t.Log("[业务:版本恢复] === TestRestoreVersion 通过 ===")
}

// TestRestoreAcrossSchemaChange 业务：恢复跨越结构演进边界——
// 恢复到“加列前”后，新增列消失且写入只能用当时列；再演进加回列仍正常。
func TestRestoreAcrossSchemaChange(t *testing.T) {
	t.Log("[业务:恢复与结构演进交叉] === TestRestoreAcrossSchemaChange 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	v1, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(1), "name": "a", "score": int64(1)}}, "v1")
	if err != nil {
		t.Fatal(err)
	}
	vAdd, err := tb.AddColumn(ctxbg, v1, Column{Name: "city", Type: TypeString, Nullable: true})
	if err != nil {
		t.Fatal(err)
	}
	v2, err := tb.Commit(ctxbg, vAdd, OpUpsert,
		[]Row{{"id": int64(2), "name": "b", "score": int64(2), "city": "BJ"}}, "v2")
	if err != nil {
		t.Fatal(err)
	}

	vr, err := tb.Restore(ctxbg, v2, v1, "back before city")
	if err != nil {
		t.Fatal(err)
	}
	cur := tb.CurrentSchema()
	if _, err := cur.FindByName("city"); ErrorCode(err) != CodeSchemaColumnNotFound {
		t.Fatalf("[用例:schema回退] 恢复后 city 应不存在, 实际 %v", err)
	}
	// 用回退后的 schema（无 city）写入应成功；带 city 的写入应失败。
	if _, err := tb.Commit(ctxbg, vr, OpUpsert,
		[]Row{{"id": int64(3), "name": "c", "score": int64(3)}}, "write old schema"); err != nil {
		t.Fatalf("[用例:回退后写入] %v", err)
	}
	if _, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(4), "name": "d", "score": int64(4), "city": "SH"}}, "with city"); ErrorCode(err) != CodeSchemaColumnNotFound {
		t.Fatalf("[用例:回退后写入] 带已消失列应 NOT_FOUND, 实际 %v", err)
	}
	// 再次加同名列：得到全新列 ID，历史值不串列。
	if _, err := tb.AddColumn(ctxbg, tb.CurrentVersion(),
		Column{Name: "city", Type: TypeString, Nullable: true}); err != nil {
		t.Fatalf("[用例:回退后再演进] %v", err)
	}
	r, _ := tb.Head(ctxbg)
	rows, _ := r.Scan(ctxbg)
	for _, row := range rows {
		if row["city"] != nil {
			t.Fatalf("[用例:回退后再演进] 新 city 列历史值必须为 nil: %v", row)
		}
	}
	t.Logf("[用例:schema回退] 恢复到加列前后 schema/读写一致；再次加列得到全新ID，%d 行 city 全 nil", len(rows))

	// 恢复到“加列后”的版本也能正常前进，旧 schema 文件仍在、可读。
	vr2, err := tb.Restore(ctxbg, tb.CurrentVersion(), v2, "forward to city era")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tb.Snapshot(ctxbg, v2); err != nil {
		t.Fatalf("[用例:schema文件保留] 旧 schema 绑定的 v2 应仍可读: %v", err)
	}
	r2, _ := tb.Snapshot(ctxbg, vr2)
	if _, err := r2.Schema().FindByName("city"); err != nil {
		t.Fatalf("[用例:schema文件保留] 恢复后应回到带 city 的 schema: %v", err)
	}
	t.Log("[用例:schema文件保留] 跨演进恢复后旧 schema 文件完整、版本可读")
	t.Log("[业务:恢复与结构演进交叉] === TestRestoreAcrossSchemaChange 通过 ===")
}

// TestRestoreCrash 业务：恢复过程中被杀，重开要么停在恢复前、要么是完整恢复。
func TestRestoreCrash(t *testing.T) {
	t.Log("[业务:恢复中断恢复] === TestRestoreCrash 开始 ===")
	for _, stage := range []string{"restore-after-snapshot-meta", "restore-after-current-publish"} {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			tb := openCreated(t, dir)
			v1, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
				[]Row{{"id": int64(1), "name": "good", "score": int64(1)}}, "v1 good")
			if err != nil {
				t.Fatal(err)
			}
			v2, err := tb.Commit(ctxbg, v1, OpUpsert,
				[]Row{{"id": int64(1), "name": "bad", "score": int64(2)}}, "v2 bad")
			if err != nil {
				t.Fatal(err)
			}
			_ = tb.Close()

			cmd := lifecycleChildCmd(dir, "restore", stage, fmt.Sprintf("%d", v1))
			out, err := cmd.CombinedOutput()
			t.Logf("[用例:恢复子进程崩溃] stage=%s out=%q err=%v", stage, out, err)

			re, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
			if err != nil {
				t.Fatal(err)
			}
			rows := headRows(t, re)
			name := rows[0]["name"]
			switch stage {
			case "restore-after-snapshot-meta":
				if re.CurrentVersion() != v2 || name != "bad" {
					t.Fatalf("[用例:恢复原子性] 翻转前崩溃应停在 v2/bad, 实际 v%d/%v", re.CurrentVersion(), name)
				}
				t.Logf("[用例:恢复原子性] stage=%s 重启后仍是恢复前 v%d: %v", stage, v2, rows)
			case "restore-after-current-publish":
				if re.CurrentVersion() != v2+1 || name != "good" {
					t.Fatalf("[用例:恢复原子性] 翻转后崩溃应是完整恢复 v3/good, 实际 v%d/%v", re.CurrentVersion(), name)
				}
				t.Logf("[用例:恢复原子性] stage=%s 重启后为完整恢复 v%d: %v", stage, v2+1, rows)
			}

			nv, err := re.Commit(ctxbg, re.CurrentVersion(), OpUpsert,
				[]Row{{"id": int64(7), "name": "after", "score": int64(7)}}, "continue")
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("[用例:恢复后可用] 重开后继续提交成功 -> v%d", nv)
		})
	}
	t.Log("[业务:恢复中断恢复] === TestRestoreCrash 通过 ===")
}

// TestRetentionPurge 业务：保留策略落盘、最近 N + 长期保留、物理释放文件、
// 过期版本读取可分辨、保留版本逐行不变、当前版本永不被动。
func TestRetentionPurge(t *testing.T) {
	t.Log("[业务:保留策略与清理] === TestRetentionPurge 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	want := buildHistory(t, tb)

	// 默认策略：保留全部，Purge 不清理任何版本。
	if res, err := tb.PurgeExpired(ctxbg); err != nil || len(res.ExpiredVersions) != 0 {
		t.Fatalf("[用例:默认全保留] 默认策略不应清理, res=%+v err=%v", res, err)
	}
	t.Log("[用例:默认全保留] 未配置策略时 PurgeExpired 不删除任何版本")

	current := tb.CurrentVersion()
	// 保留最近 3 个版本，另长期保留 v2。
	var pinned int64 = 2
	if err := tb.SetRetentionPolicy(ctxbg, RetentionPolicy{RetainVersions: 3, Pinned: []int64{pinned}}); err != nil {
		t.Fatal(err)
	}

	// 策略落盘：重开后仍生效。
	re, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
	if err != nil {
		t.Fatal(err)
	}
	pol := re.RetentionPolicy()
	if pol.RetainVersions != 3 || len(pol.Pinned) != 1 || pol.Pinned[0] != pinned {
		t.Fatalf("[用例:策略落盘] 重开后策略丢失: %+v", pol)
	}
	t.Logf("[用例:策略落盘] 重开表后策略仍生效: 保留最近%d个 + pin v%d", pol.RetainVersions, pinned)

	res, err := re.PurgeExpired(ctxbg)
	if err != nil {
		t.Fatal(err)
	}
	// 期望过期：current=7，最近3为 5,6,7；pinned=2；故 1(create),3,4 过期。
	wantExpired := map[int64]bool{1: true, 3: true, 4: true}
	if len(res.ExpiredVersions) != len(wantExpired) {
		t.Fatalf("[用例:过期范围] 期望过期 %v, 实际 %v", wantExpired, res.ExpiredVersions)
	}
	for _, v := range res.ExpiredVersions {
		if !wantExpired[v] {
			t.Fatalf("[用例:过期范围] 非预期的过期版本 v%d", v)
		}
	}
	t.Logf("[用例:过期范围] 本次过期 %v（最近3个 %d,%d,%d 与 pin v%d 保留）",
		res.ExpiredVersions, current-2, current-1, current, pinned)

	// 当前版本永远保留：清理只删旧快照，current 指针与内容不变。
	if re.CurrentVersion() != current {
		t.Fatalf("[用例:当前不动] current 被清理改变: %d", re.CurrentVersion())
	}
	if got := snapshotRows(t, re, current); !rowsEqual(got, want[current]) {
		t.Fatalf("[用例:当前不动] 当前 v%d 内容被清理破坏: %v", current, got)
	}

	// 第一轮：过期快照 v1/v3/v4 与保留窗口内 v5/v6 共享追加链/删除 sidecar，
	// 这些文件仍被存活快照引用，必须保留（共享文件保护）。
	t.Logf("[用例:共享文件保护] 首轮过期 %v，但其文件仍被保留窗口快照引用，删除=%d（不误删）",
		res.ExpiredVersions, res.RemovedFiles)

	// 保留范围内的版本清理前后逐行一致；pinned 版本也一致。
	for _, v := range []int64{2, 5, 6, 7} {
		got := snapshotRows(t, re, v)
		if !rowsEqual(got, want[v]) {
			t.Fatalf("[用例:保留版本一致] v%d 清理后结果改变: want=%v got=%v", v, want[v], got)
		}
	}
	t.Log("[用例:保留版本一致] 最近3个版本与 pin v2 清理前后逐行一致，当前版本未动")

	// 过期版本读取：明确 SNAPSHOT_EXPIRED（不是空结果）。
	for _, v := range res.ExpiredVersions {
		if _, err := re.Snapshot(ctxbg, v); ErrorCode(err) != CodeSnapshotExpired {
			t.Fatalf("[用例:过期读取] v%d 应 SNAPSHOT_EXPIRED, 实际 %v", v, err)
		}
		if !re.IsExpired(v) {
			t.Fatalf("[用例:过期读取] IsExpired(v%d) 应为 true", v)
		}
	}
	t.Logf("[用例:过期读取] 读取已清理版本稳定返回 SNAPSHOT_EXPIRED: %v", res.ExpiredVersions)

	// 恢复到已清理版本：可分辨失败。
	if _, err := re.Restore(ctxbg, re.CurrentVersion(), 3, "restore purged"); ErrorCode(err) != CodeSnapshotExpired {
		t.Fatalf("[用例:恢复过期版本] 应 SNAPSHOT_EXPIRED, 实际 %v", err)
	}
	// pin 已清理版本 / 非法策略：可分辨失败。
	if err := re.PinVersions(ctxbg, 3); ErrorCode(err) != CodePinTargetInvalid {
		t.Fatalf("[用例:pin校验] pin 已清理版本应 PIN_TARGET_INVALID, 实际 %v", err)
	}
	if err := re.SetRetentionPolicy(ctxbg, RetentionPolicy{RetainVersions: -1}); ErrorCode(err) != CodeRetentionInvalid {
		t.Fatalf("[用例:策略校验] 负数 N 应 RETENTION_INVALID, 实际 %v", err)
	}
	t.Log("[用例:恢复过期版本] 恢复/pin 已清理版本、非法策略均带可分辨原因失败")

	// 再提交产生新版本后再次清理：已过期版本仍过期，新一轮只清理新掉队的版本。
	v8, err := re.Commit(ctxbg, re.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(8), "name": "new", "score": int64(80)}}, "v8")
	if err != nil {
		t.Fatal(err)
	}
	res2, err := re.PurgeExpired(ctxbg)
	if err != nil {
		t.Fatal(err)
	}
	// 最近3为 6,7,8，pin=2；v5 新掉队。
	if len(res2.ExpiredVersions) != 1 || res2.ExpiredVersions[0] != 5 {
		t.Fatalf("[用例:增量清理] 期望仅 v5 新过期, 实际 %v", res2.ExpiredVersions)
	}
	if _, err := re.Snapshot(ctxbg, v8); err != nil {
		t.Fatalf("[用例:增量清理] 新版本应可读, %v", err)
	}
	t.Logf("[用例:增量清理] 推进到 v%d 后再次清理，仅 v5 新掉队；当前 v%d 正常", v8, re.CurrentVersion())

	// pin 集合中 v2 引用的数据文件仍在，v2 始终可读。
	if got := snapshotRows(t, re, 2); !rowsEqual(got, want[2]) {
		t.Fatalf("[用例:长期保留] pin v2 内容被破坏: %v", got)
	}
	t.Log("[用例:长期保留] pin v2 历经多轮清理依旧逐行可读")

	// 最后：连续推进多轮、每轮清理，验证旧追加文件随最后引用者过期被物理释放，
	// 磁盘不再线性膨胀，且每轮当前版本完整可读。
	totalRemoved := res.RemovedFiles + res2.RemovedFiles
	for i := 0; i < 6; i++ {
		v, cerr := re.Commit(ctxbg, re.CurrentVersion(), OpUpsert,
			[]Row{{"id": int64(400 + i), "name": fmt.Sprintf("stream%d", i), "score": int64(i)}},
			fmt.Sprintf("stream-%d", i))
		if cerr != nil {
			t.Fatal(cerr)
		}
		pr, perr := re.PurgeExpired(ctxbg)
		if perr != nil {
			t.Fatal(perr)
		}
		totalRemoved += pr.RemovedFiles
		if rr, e := re.Snapshot(ctxbg, v); e != nil || rr == nil {
			t.Fatalf("[用例:空间回收可读性] 清理后当前 v%d 不可读: %v", v, e)
		}
	}
	filesAfter := countDataFiles(t, dir)
	if totalRemoved <= 0 {
		t.Fatalf("[用例:空间释放] 整个过程没有任何过期文件被物理释放")
	}
	if filesAfter >= 11 {
		t.Fatalf("[用例:空间持续回收] 磁盘应收敛，实际仍有 %d 个文件（疑似只增不减）", filesAfter)
	}
	t.Logf("[用例:空间持续回收] 多轮提交+清理累计物理释放 %d 个过期文件；磁盘收敛到 %d 个（仅最近N+pin所需），当前每轮可读",
		totalRemoved, filesAfter)
	t.Log("[业务:保留策略与清理] === TestRetentionPurge 通过 ===")
}

// TestPurgeCrash 业务：清理在“登记过期后/删数据后”被杀，重开后续完，
// 期间读取只有完整可读或 SNAPSHOT_EXPIRED 两种结局，保留版本不受损。
func TestPurgeCrash(t *testing.T) {
	t.Log("[业务:清理中断恢复] === TestPurgeCrash 开始 ===")
	for _, stage := range []string{"purge-after-expire-mark", "purge-after-data-delete"} {
		stage := stage
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			tb := openCreated(t, dir)
			want := buildHistory(t, tb)
			current := tb.CurrentVersion()
			if err := tb.SetRetentionPolicy(ctxbg, RetentionPolicy{RetainVersions: 2}); err != nil {
				t.Fatal(err)
			}
			_ = tb.Close()

			cmd := lifecycleChildCmd(dir, "purge", stage, "")
			out, err := cmd.CombinedOutput()
			t.Logf("[用例:清理子进程崩溃] stage=%s out=%q err=%v", stage, out, err)

			re, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if re.CurrentVersion() != current {
				t.Fatalf("[用例:当前不动] current 被清理改变: %d", re.CurrentVersion())
			}

			// 应过期版本（current-2 之前的全部非保留版本）。
			var expiredAny bool
			for v := int64(1); v <= current-2; v++ {
				_, gerr := re.Snapshot(ctxbg, v)
				code := ErrorCode(gerr)
				if code != CodeSnapshotExpired {
					t.Fatalf("[用例:清理结局] stage=%s v%d 期望 EXPIRED, 实际 %v", stage, v, gerr)
				}
				expiredAny = true
			}
			if !expiredAny {
				t.Fatal("没有可过期版本，测试构造有误")
			}
			// 保留版本与当前版本完整、逐行一致。
			for _, v := range []int64{current - 1, current} {
				if got := snapshotRows(t, re, v); !rowsEqual(got, want[v]) {
					t.Fatalf("[用例:清理结局] stage=%s 保留版本 v%d 被破坏: %v", stage, v, got)
				}
			}
			t.Logf("[用例:清理结局] stage=%s 重开后过期版本均 EXPIRED，保留 v%d/v%d 完整",
				stage, current-1, current)

			// 再跑一次清理必须幂等成功（续完不报错、不重复计数为负数）。
			if _, err := re.PurgeExpired(ctxbg); err != nil {
				t.Fatalf("[用例:清理幂等] 续完后再清理失败: %v", err)
			}
			// 当前表可正常继续提交。
			if _, err := re.Commit(ctxbg, re.CurrentVersion(), OpUpsert,
				[]Row{{"id": int64(1), "name": "after", "score": int64(1)}}, "post purge crash"); err != nil {
				t.Fatal(err)
			}
			t.Log("[用例:清理幂等] 崩溃续完后清理可重入、表可继续提交")
		})
	}
	t.Log("[业务:清理中断恢复] === TestPurgeCrash 通过 ===")
}

// TestMultiHandleOneWinner 业务：同一进程内多个独立句柄写同一张表，
// 同一基线上恰好一个成功，其余 STALE；后到者绝不覆盖已确认数据。
func TestMultiHandleOneWinner(t *testing.T) {
	t.Log("[业务:多句柄并发] === TestMultiHandleOneWinner 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	base := tb.CurrentVersion()
	vSeed, err := tb.Commit(ctxbg, base, OpUpsert,
		[]Row{{"id": int64(1), "name": "seed", "score": int64(1)}}, "seed")
	if err != nil {
		t.Fatal(err)
	}

	const n = 6
	handles := make([]*Table, n)
	for i := range handles {
		h, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if h.CurrentVersion() != vSeed {
			t.Fatalf("句柄打开时 current 应为 v%d", vSeed)
		}
		handles[i] = h
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, stale, inProgress := 0, 0, 0
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, cerr := handles[i].Commit(ctxbg, vSeed, OpUpsert,
				[]Row{{"id": int64(100 + i), "name": fmt.Sprintf("h%d", i), "score": int64(i)}},
				fmt.Sprintf("handle-%d", i))
			mu.Lock()
			defer mu.Unlock()
			switch ErrorCode(cerr) {
			case "":
				ok++
			case CodeConflictSnapshotStale:
				stale++
			case CodeConflictInProgress:
				inProgress++
			default:
				t.Errorf("非预期错误: %v", cerr)
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || stale+inProgress != n-1 {
		t.Fatalf("[用例:单赢家] 期望恰好1成功/%d冲突, 实际 ok=%d stale=%d inProgress=%d",
			n-1, ok, stale, inProgress)
	}
	t.Logf("[用例:单赢家] %d 个独立句柄同基线提交: 成功=1, STALE=%d, IN_PROGRESS=%d",
		n, stale, inProgress)

	// 已确认数据绝不被后到者吃掉：刷新任一旧句柄后能看到 seed + 唯一赢家两行。
	any := handles[0]
	if err := any.Refresh(ctxbg); err != nil {
		t.Fatal(err)
	}
	rows := headRows(t, any)
	if len(rows) != 2 {
		t.Fatalf("[用例:无丢写] 期望 seed + 赢家共2行, 实际 %v", rows)
	}
	t.Logf("[用例:无丢写] Refresh 旧句柄后看到完整结果2行: %v", rows)

	// 失败句柄基于最新版本重试可以成功（恢复/清理/整理也共用同一条 CAS 路径）。
	vr, err := handles[1].Restore(ctxbg, any.CurrentVersion(), vSeed, "restore via old handle")
	if err != nil {
		t.Fatalf("[用例:旧句柄恢复] %v", err)
	}
	t.Logf("[用例:旧句柄恢复] Refresh 后旧句柄发起恢复成功 -> v%d", vr)

	// 旧句柄不刷新直接写：STALE，不会悄悄回退版本。
	if _, err := handles[2].Commit(ctxbg, vSeed, OpUpsert,
		[]Row{{"id": int64(200), "name": "stale", "score": 1}}, "x"); ErrorCode(err) != CodeConflictSnapshotStale {
		t.Fatalf("[用例:陈旧句柄] 不刷新的旧句柄应 STALE, 实际 %v", err)
	}
	t.Log("[用例:陈旧句柄] 未刷新句柄的写入被 STALE 拒绝，已确认数据不受影响")
	t.Log("[业务:多句柄并发] === TestMultiHandleOneWinner 通过 ===")
}

// TestMultiProcessOneWinner 业务：多个独立进程同时写同一张表，
// 同一基线上恰好一个提交成功，其余 STALE/IN_PROGRESS，无丢写无覆盖。
func TestMultiProcessOneWinner(t *testing.T) {
	t.Log("[业务:多进程并发] === TestMultiProcessOneWinner 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	vSeed, err := tb.Commit(ctxbg, tb.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(1), "name": "seed", "score": int64(1)}}, "seed")
	if err != nil {
		t.Fatal(err)
	}
	_ = tb.Close()

	const n = 6
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, stale, inProgress, other := 0, 0, 0, 0
	startFile := filepath.Join(dir, "start.flag")
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, err := os.Stat(startFile); err == nil {
					break
				}
				timeSleep(5)
			}
			cmd := exec.Command(os.Args[0], "-test.run", "^TestMultiProcessCommitChildOnly$")
			cmd.Env = append(os.Environ(),
				"LAKEHOUSE_MP_CHILD=1",
				"LAKEHOUSE_TABLE_DIR="+filepath.Join(dir, "table"),
				fmt.Sprintf("LAKEHOUSE_MP_BASE=%d", vSeed),
				fmt.Sprintf("LAKEHOUSE_MP_TAG=%d", i),
			)
			out, _ := cmd.CombinedOutput()
			mu.Lock()
			defer mu.Unlock()
			first := string(out)
			if i := indexByte(first, '\n'); i >= 0 {
				first = first[:i+1]
			}
			switch first {
			case "OK\n":
				ok++
			case "STALE\n":
				stale++
			case "BUSY\n":
				inProgress++
			default:
				other++
				t.Errorf("[用例:子进程结果] worker %d 异常输出 %q", i, string(out))
			}
		}()
	}
	timeSleep(100)
	if err := os.WriteFile(startFile, []byte("go"), 0o644); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if ok != 1 || stale+inProgress != n-1 || other != 0 {
		t.Fatalf("[用例:跨进程单赢家] 期望1成功/%d冲突, 实际 ok=%d stale=%d busy=%d other=%d",
			n-1, ok, stale, inProgress, other)
	}
	t.Logf("[用例:跨进程单赢家] %d 个进程同基线提交: 成功=1, STALE=%d, IN_PROGRESS=%d",
		n, stale, inProgress)

	re, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if re.CurrentVersion() != vSeed+1 {
		t.Fatalf("[用例:跨进程无丢写] 期望仅推进到 v%d, 实际 v%d", vSeed+1, re.CurrentVersion())
	}
	rows := headRows(t, re)
	if len(rows) != 2 {
		t.Fatalf("[用例:跨进程无丢写] 期望 seed + 唯一赢家共2行, 实际 %v", rows)
	}
	t.Logf("[用例:跨进程无丢写] 版本只前进一次、共2行（无覆盖无丢写）: %v", rows)
	t.Log("[业务:多进程并发] === TestMultiProcessOneWinner 通过 ===")
}

// TestMultiProcessCommitChildOnly 是多进程提交的子进程入口（名字唯一，避免误匹配父测试）。
func TestMultiProcessCommitChildOnly(t *testing.T) {
	if os.Getenv("LAKEHOUSE_MP_CHILD") != "1" {
		return
	}
	var base, tag int64
	fmt.Sscanf(os.Getenv("LAKEHOUSE_MP_BASE"), "%d", &base)
	fmt.Sscanf(os.Getenv("LAKEHOUSE_MP_TAG"), "%d", &tag)
	tb, err := Open(ctxbg, os.Getenv("LAKEHOUSE_TABLE_DIR"), nil)
	if err != nil {
		fmt.Print("ERR\n")
		os.Exit(0)
	}
	_, err = tb.Commit(ctxbg, base, OpUpsert,
		[]Row{{"id": int64(200 + tag), "name": fmt.Sprintf("p%d", tag), "score": tag}}, "mp")
	switch ErrorCode(err) {
	case "":
		fmt.Print("OK\n")
	case CodeConflictSnapshotStale:
		fmt.Print("STALE\n")
	case CodeConflictInProgress:
		fmt.Print("BUSY\n")
	default:
		fmt.Print("ERR\n")
	}
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
