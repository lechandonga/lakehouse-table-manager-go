package lakehouse

import (
	"path/filepath"
	"testing"
)

// TestRestoreCrashRecovery 业务：恢复中断后重开，要么恢复前、要么完整恢复。
func TestRestoreCrashRecovery(t *testing.T) {
	t.Log("[业务:生命周期中断恢复] === TestRestoreCrashRecovery 开始 ===")
	for _, stage := range []string{"after-restore-schema", "after-restore-snapshot"} {
		stage := stage
		t.Run("crash_before_visibility/"+stage, func(t *testing.T) {
			dir := t.TempDir()
			tb := openCreated(t, dir)
			versions := buildLinearVersions(t, tb, 4)
			target := versions[0]
			preHead := tb.CurrentVersion()
			tr, _ := tb.Snapshot(ctxbg, preHead)
			preRows, _ := tr.Scan(ctxbg)
			_ = tb.Close()

			cmd := restoreCrashChildCmd(dir, stage, target)
			out, err := cmd.CombinedOutput()
			t.Logf("[用例:恢复中断] stage=%s err=%v out=%q", stage, err, string(out))

			reopened, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if cv := reopened.CurrentVersion(); cv != preHead {
				t.Fatalf("[用例:原子恢复] 指针翻转前崩溃，current 应保持 v%d，实际 v%d", preHead, cv)
			}
			r, _ := reopened.Head(ctxbg)
			rows, _ := r.Scan(ctxbg)
			if !rowsEqual(preRows, rows) {
				t.Fatalf("[用例:原子恢复] 恢复前状态应完整, want=%v got=%v", preRows, rows)
			}
			t.Logf("[用例:原子恢复] stage=%s 崩溃后重开 current=v%d（恢复前完整状态），无半截恢复",
				stage, preHead)

			// 重试恢复必须成功（版本号不受崩溃影响、不重用、无空洞）。
			vR, err := reopened.RestoreToVersion(ctxbg, reopened.CurrentVersion(), target, "retry restore")
			if err != nil {
				t.Fatal(err)
			}
			if vR <= preHead {
				t.Fatalf("[用例:恢复重试] 新版本必须 > 崩溃前 v%d, 实际 v%d", preHead, vR)
			}
			tr2, _ := reopened.Snapshot(ctxbg, target)
			want, _ := tr2.Scan(ctxbg)
			hr, _ := reopened.Head(ctxbg)
			got, _ := hr.Scan(ctxbg)
			if !rowsEqual(want, got) {
				t.Fatalf("[用例:恢复重试] 重试恢复后当前应等于 v%d, want=%v got=%v", target, want, got)
			}
			t.Logf("[用例:恢复重试] 重开后恢复重试成功 -> v%d，与目标 v%d 逐行一致", vR, target)
		})
	}
	t.Log("[业务:生命周期中断恢复] === TestRestoreCrashRecovery 通过 ===")
}

// TestExpireCrashRecovery 业务：清理在“清单翻转后、文件删除中”崩溃，
// 重开后续删完成；保留版本与当前版本始终可读，过期版本始终明确报错。
func TestExpireCrashRecovery(t *testing.T) {
	t.Log("[业务:生命周期中断恢复] === TestExpireCrashRecovery 开始 ===")
	dir := t.TempDir()
	tb := openCreated(t, dir)
	versions := buildLinearVersions(t, tb, 8)
	if err := tb.SetRetentionPolicy(ctxbg, tb.CurrentVersion(), RetentionPolicy{KeepVersions: 2}); err != nil {
		t.Fatal(err)
	}
	preCurrent := tb.CurrentVersion()
	survivor := versions[len(versions)-1]
	_ = tb.Close()

	cmd := expireCrashChildCmd(dir, "after-expire-manifest")
	out, err := cmd.CombinedOutput()
	t.Logf("[用例:清理中断] 清单翻转后被杀 err=%v out=%q", err, string(out))

	reopened, err := Open(ctxbg, filepath.Join(dir, "table"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.CurrentVersion() != preCurrent {
		t.Fatalf("[用例:原子清理] 清理不改变 current, 期望 v%d 实际 v%d", preCurrent, reopened.CurrentVersion())
	}

	// 清单已翻转：过期版本立刻 EXPIRED（即使文件还没删完，也绝不返回半截内容）。
	// 现存最近2版本 + v1 是保留的（1 + 8 个数据版本 = 9 个现存；最近2 => v8,v9）。
	expiredNow := []int64{}
	for v := int64(2); v <= 7; v++ {
		expiredNow = append(expiredNow, v)
	}
	for _, v := range expiredNow {
		if _, err := reopened.Snapshot(ctxbg, v); ErrorCode(err) != CodeSnapshotExpired {
			t.Fatalf("[用例:清单先翻转] 中断后 v%d 应 EXPIRED, 实际 %v", v, err)
		}
	}
	sr, err := reopened.Snapshot(ctxbg, survivor)
	if err != nil {
		t.Fatalf("[用例:保留版本完整] 最近版本 v%d 中断后必须可读: %v", survivor, err)
	}
	srows, _ := sr.Scan(ctxbg)
	if len(srows) != len(versions) {
		t.Fatalf("[用例:保留版本完整] v%d 应有 %d 行, 实际 %d", survivor, len(versions), len(srows))
	}
	hr, _ := reopened.Head(ctxbg)
	hrows, _ := hr.Scan(ctxbg)
	if len(hrows) != len(versions) {
		t.Fatalf("[用例:当前不动] 当前版本中断后必须完整, 实际 %d 行", len(hrows))
	}
	t.Logf("[用例:清单先翻转] 中断后过期版本立刻 EXPIRED；当前 v%d 与保留版本完整可读（%d行）",
		survivor, len(hrows))

	// 重新打开再清理：续删把残留物理文件释放，结果幂等。
	filesBefore := countDataFiles(t, dir)
	rep, err := reopened.ExpireOldVersions(ctxbg, reopened.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	filesAfter := countDataFiles(t, dir)
	t.Logf("[用例:中断续删] 续删文件=%v（data 文件 %d -> %d）", rep.RemovedFiles, filesBefore, filesAfter)

	// 再调一次应完全幂等（没有新受害者、没有残留文件）。
	rep2, err := reopened.ExpireOldVersions(ctxbg, reopened.CurrentVersion())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep2.ExpiredVersions) != 0 || len(rep2.RemovedFiles) != 0 {
		t.Fatalf("[用例:清理幂等] 重复清理应为空操作, 实际 %+v", rep2)
	}

	// 续删后保留版本依旧逐行完整。
	hr2, _ := reopened.Head(ctxbg)
	hrows2, _ := hr2.Scan(ctxbg)
	if len(hrows2) != len(versions) {
		t.Fatalf("[用例:续删不伤人] 续删后当前版本行数变化: %d", len(hrows2))
	}
	t.Logf("[用例:续删不伤人] 续删后当前仍 %d 行；重复清理幂等无操作", len(hrows2))

	// 续删后仍可正常提交。
	nv, err := reopened.Commit(ctxbg, reopened.CurrentVersion(), OpUpsert,
		[]Row{{"id": int64(99), "name": "post-expire", "score": int64(99)}}, "after resumed expire")
	if err != nil {
		t.Fatal(err)
	}
	if nv != preCurrent+1 {
		t.Fatalf("[用例:清理后提交] 期望 v%d, 实际 v%d", preCurrent+1, nv)
	}
	t.Logf("[用例:清理后提交] 中断续删后新提交 -> v%d，表可正常继续写入", nv)
	t.Log("[业务:生命周期中断恢复] === TestExpireCrashRecovery 通过 ===")
}
