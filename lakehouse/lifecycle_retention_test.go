package lakehouse

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRetentionExpire 业务：保留策略配置/落盘、最近N+pin 清理、
// 物理空间释放、清理后过期版本明确报错、保留版本逐行不变。
func TestRetentionExpire(t *testing.T) {
	t.Log("[业务:保留策略与清理] === TestRetentionExpire 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)

	// v1..v6（外加 v1=create，实际版本号 2..7）。
	versions := buildLinearVersions(t, tb, 6)
	snapshot := map[int64][]Row{}
	for _, v := range versions {
		r, _ := tb.Snapshot(ctxbg, v)
		snapshot[v], _ = r.Scan(ctxbg)
	}
	headBefore, _ := tb.Head(ctxbg)
	headRowsBefore, _ := headBefore.Scan(ctxbg)

	// 整理产生单文件快照：此前的 upsert/delete 文件只被旧快照引用，
	// 这样旧版本一旦过期，其独占数据文件才可能被真正释放
	//（被新版本共享的文件受引用保护、永不误删——这正是清理安全性的关键）。
	vc, err := tb.Compact(ctxbg, tb.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	versions = append(versions, vc)
	rvc, _ := tb.Snapshot(ctxbg, vc)
	snapshot[vc], _ = rvc.Scan(ctxbg)
	// 再追加两个普通版本，使最近窗口之外出现“整理前”的老版本。
	vA, err := tb.Commit(ctxbg, vc, OpUpsert,
		[]Row{{"id": int64(7), "name": "r7", "score": int64(7)}}, "after compact a")
	if err != nil {
		t.Fatal(err)
	}
	vB, err := tb.Commit(ctxbg, vA, OpUpsert,
		[]Row{{"id": int64(8), "name": "r8", "score": int64(8)}}, "after compact b")
	if err != nil {
		t.Fatal(err)
	}
	versions = append(versions, vA, vB)
	for _, v := range []int64{vA, vB} {
		rr, _ := tb.Snapshot(ctxbg, v)
		snapshot[v], _ = rr.Scan(ctxbg)
	}
	headBefore, _ = tb.Head(ctxbg)
	headRowsBefore, _ = headBefore.Scan(ctxbg)

	// 默认策略：保留全部，清理是无操作。
	if rep, err := tb.ExpireOldVersions(ctxbg, tb.CurrentVersion()); err != nil || len(rep.ExpiredVersions) != 0 {
		t.Fatalf("[用例:默认全保留] 默认策略不应清理任何版本: %v %v", rep, err)
	}
	t.Log("[用例:默认全保留] 未配置策略时 ExpireOldVersions 不清理任何历史")

	// 非法策略。
	if err := tb.SetRetentionPolicy(ctxbg, tb.CurrentVersion(), RetentionPolicy{KeepVersions: -1}); ErrorCode(err) != CodeRetentionInvalid {
		t.Fatalf("[用例:非法策略] 负数应 RETENTION_POLICY_INVALID, 实际 %v", err)
	}

	// pin 最老的数据版本，保留最近 3 个现存版本。
	pinV := versions[0]
	if err := tb.PinVersions(ctxbg, pinV); err != nil {
		t.Fatal(err)
	}
	if err := tb.SetRetentionPolicy(ctxbg, tb.CurrentVersion(), RetentionPolicy{KeepVersions: 3}); err != nil {
		t.Fatal(err)
	}
	p, _ := tb.RetentionPolicyView(ctxbg)
	if p.KeepVersions != 3 || len(p.Pinned) != 1 || p.Pinned[0] != pinV {
		t.Fatalf("[用例:策略落盘] 策略读取不一致: %+v", p)
	}
	t.Logf("[用例:pin长期保留] pin v%d；保留最近3个版本；策略可回读: %+v", pinV, p)

	// 清理前列出数据文件数量。
	dataFilesBefore := countDataFiles(t, dir)

	rep, err := tb.ExpireOldVersions(ctxbg, tb.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	// 现存版本为 create + 8 个数据/整理版本；最近3保留，pin 保留。
	for _, v := range rep.ExpiredVersions {
		if v == pinV {
			t.Fatalf("[用例:清理范围] pin 版本不得被清理: %v", rep.ExpiredVersions)
		}
	}
	last3 := map[int64]bool{
		versions[len(versions)-1]: true,
		versions[len(versions)-2]: true,
		versions[len(versions)-3]: true,
	}
	for _, v := range rep.ExpiredVersions {
		if last3[v] {
			t.Fatalf("[用例:清理范围] pin/最近3版本不得被清理: %v", rep.ExpiredVersions)
		}
	}
	if len(rep.ExpiredVersions) == 0 {
		t.Fatalf("[用例:清理范围] 应清理超出最近3且未pin的旧版本")
	}
	if len(rep.RemovedFiles) == 0 {
		t.Fatalf("[用例:空间释放] 应实际删除数据文件, 实际 %+v", rep)
	}
	t.Logf("[用例:清理范围] 被清理版本=%v；pin 的 v%d 与最近3版本保留", rep.ExpiredVersions, pinV)

	// 物理文件确实被删除（不只是删记录）。
	dataFilesAfter := countDataFiles(t, dir)
	if dataFilesAfter >= dataFilesBefore {
		t.Fatalf("[用例:空间释放] 数据文件数应下降: before=%d after=%d", dataFilesBefore, dataFilesAfter)
	}
	t.Logf("[用例:空间释放] data/ 文件数 %d -> %d（删除 %d 个物理文件 %v）",
		dataFilesBefore, dataFilesAfter, dataFilesBefore-dataFilesAfter, rep.RemovedFiles)

	// 过期版本读取：明确 SNAPSHOT_EXPIRED，绝不返回空结果。
	for _, v := range rep.ExpiredVersions {
		if _, err := tb.Snapshot(ctxbg, v); ErrorCode(err) != CodeSnapshotExpired {
			t.Fatalf("[用例:过期读取] v%d 应 SNAPSHOT_EXPIRED, 实际 %v", v, err)
		}
	}
	t.Logf("[用例:过期读取] 已清理版本 %v 读取一律 SNAPSHOT_EXPIRED（不会返回空结果）", rep.ExpiredVersions)
	// 压根不存在仍是 NOT_FOUND，与过期可分辨。
	if _, err := tb.Snapshot(ctxbg, 9999); ErrorCode(err) != CodeSnapshotNotFound {
		t.Fatalf("[用例:过期读取] 不存在版本应 NOT_FOUND, 实际 %v", err)
	}
	t.Log("[用例:过期读取] 从未存在的版本 -> SNAPSHOT_NOT_FOUND，两类原因可分辨")

	// 保留范围内的版本：逐行一致；pin 版本与当前版本也一样。
	for _, v := range append([]int64{pinV}, versions[len(versions)-2:]...) {
		r, err := tb.Snapshot(ctxbg, v)
		if err != nil {
			t.Fatalf("[用例:保留版本一致] v%d 应仍可读: %v", v, err)
		}
		got, _ := r.Scan(ctxbg)
		want := snapshot[v]
		if !rowsEqual(want, got) {
			t.Fatalf("[用例:保留版本一致] v%d 清理后结果改变: want=%v got=%v", v, want, got)
		}
	}
	headAfter, _ := tb.Head(ctxbg)
	headRowsAfter, _ := headAfter.Scan(ctxbg)
	if !rowsEqual(headRowsBefore, headRowsAfter) {
		t.Fatalf("[用例:当前不动] 清理不得改变当前版本: before=%v after=%v", headRowsBefore, headRowsAfter)
	}
	t.Log("[用例:保留版本一致] pin 版本、最近3版本、当前版本清理后读取全部逐行一致，当前版本不动")

	// 当前版本号不变；清理不是提交。
	if tb.CurrentVersion() != versions[len(versions)-1] {
		t.Fatalf("[用例:清理不出版本] current 不应变化")
	}

	// 策略重开后仍生效。
	tb2, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
	if err != nil {
		t.Fatal(err)
	}
	p2, _ := tb2.RetentionPolicyView(ctxbg)
	if p2.KeepVersions != 3 || len(p2.Pinned) != 1 {
		t.Fatalf("[用例:策略持久化] 重开后策略丢失: %+v", p2)
	}
	if _, err := tb2.Snapshot(ctxbg, rep.ExpiredVersions[0]); ErrorCode(err) != CodeSnapshotExpired {
		t.Fatalf("[用例:策略持久化] 重开后过期状态丢失: %v", err)
	}
	t.Logf("[用例:策略持久化] 重开后策略仍生效 %+v，过期版本仍报 EXPIRED", p2)

	// 再追加提交并再次清理：老 pin 版本仍留着；当前版本永远安全。
	vNew, err := tb2.Commit(ctxbg, tb2.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(7), "name": "r7", "score": int64(7)}}, "after expire")
	if err != nil {
		t.Fatal(err)
	}
	rep2, err := tb2.ExpireOldVersions(ctxbg, vNew)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range rep2.ExpiredVersions {
		if v == pinV {
			t.Fatalf("[用例:pin长期有效] pin 版本第二次清理仍不得被删: %v", rep2.ExpiredVersions)
		}
	}
	rp, _ := tb2.Snapshot(ctxbg, pinV)
	pinRows, _ := rp.Scan(ctxbg)
	if !rowsEqual(snapshot[pinV], pinRows) {
		t.Fatalf("[用例:pin长期有效] pin 版本读取改变: %v vs %v", snapshot[pinV], pinRows)
	}
	t.Logf("[用例:pin长期有效] 二次清理=%v，pin 的 v%d 依旧可读 %d 行，当前 v%d 不动",
		rep2.ExpiredVersions, pinV, len(pinRows), vNew)

	// unpin 后它就能被清掉。
	if err := tb2.UnpinVersions(ctxbg, pinV); err != nil {
		t.Fatal(err)
	}
	// keep 3 下 pinV 已远超出最近3，立刻可清。
	rep3, err := tb2.ExpireOldVersions(ctxbg, tb2.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	foundPinExpired := false
	for _, v := range rep3.ExpiredVersions {
		if v == pinV {
			foundPinExpired = true
		}
	}
	if !foundPinExpired {
		t.Fatalf("[用例:unpin生效] unpin 后 v%d 应可清理, 实际 %v", pinV, rep3.ExpiredVersions)
	}
	t.Logf("[用例:unpin生效] 取消 pin 后 v%d 在下一次清理中被回收: %v", pinV, rep3.ExpiredVersions)
	t.Log("[业务:保留策略与清理] === TestRetentionExpire 通过 ===")
}

// TestExpireSharedFiles 业务：恢复产生的新快照复用旧文件时，
// 清理旧版本绝不能删掉新版本仍依赖的共享文件。
func TestExpireSharedFiles(t *testing.T) {
	t.Log("[业务:保留策略与清理] === TestExpireSharedFiles 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	versions := buildLinearVersions(t, tb, 4)
	keep := versions[1]

	// 恢复到较早版本：新快照复用旧快照文件清单。
	vR, err := tb.RestoreToVersion(ctxbg, tb.CurrentVersion(), keep, "restore for sharing")
	if err != nil {
		t.Fatal(err)
	}
	wantRows, _ := func() ([]Row, error) {
		r, e := tb.Snapshot(ctxbg, keep)
		if e != nil {
			return nil, e
		}
		return r.Scan(ctxbg)
	}()

	if err := tb.SetRetentionPolicy(ctxbg, tb.CurrentVersion(), RetentionPolicy{KeepVersions: 1}); err != nil {
		t.Fatal(err)
	}
	rep, err := tb.ExpireOldVersions(ctxbg, tb.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[用例:共享文件保护] 恢复到 v%d 得到 v%d；清理旧版本 %v", keep, vR, rep.ExpiredVersions)

	// 被恢复依赖的共享数据文件必须还在：恢复快照（当前）读取必须完整。
	head, _ := tb.Head(ctxbg)
	got, err := head.Scan(ctxbg)
	if err != nil {
		t.Fatalf("[用例:共享文件保护] 清理后当前版本读失败（共享文件被误删）: %v", err)
	}
	if !rowsEqual(wantRows, got) {
		t.Fatalf("[用例:共享文件保护] 当前读取与恢复目标不一致: want=%v got=%v", wantRows, got)
	}
	t.Logf("[用例:共享文件保护] 即便来源版本已过期，恢复快照 v%d 仍完整可读: %v", vR, got)

	// keep 本身（若已过期）读取报 EXPIRED，但恢复出来的新快照不受影响。
	if _, err := tb.Snapshot(ctxbg, keep); err != nil && ErrorCode(err) != CodeSnapshotExpired {
		t.Fatalf("[用例:共享文件保护] 来源版本应过期可读EXPIRED, 实际 %v", err)
	}
	t.Log("[业务:保留策略与清理] === TestExpireSharedFiles 通过 ===")
}

func countDataFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "table", "data"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			n++
		}
	}
	return n
}
