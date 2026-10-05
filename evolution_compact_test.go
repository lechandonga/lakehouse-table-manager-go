package lakehouse

import (
	"errors"
	"fmt"
	"sort"
	"testing"
)

// TestSchemaEvolution 结构演进：加列缺省值、删列隔离、改名不串列、非法变更拒绝。
func TestSchemaEvolution(t *testing.T) {
	log := newBizLog(t, "结构演进（加列/删列/改名 + 历史读取）")

	t.Run("加列后历史行得到确定缺省值", func(t *testing.T) {
		tb, _ := openTable(t)
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "alice"}); err != nil {
			t.Fatal(err)
		}
		v2, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		log.casef("加列后历史行得到确定缺省值", "v%d 写入历史行，随后加非空 int 列 age=默认18", v2.Version())

		ev := tb.Begin()
		if err := ev.AddColumn("age", TypeInt, false, int64(18)); err != nil {
			t.Fatal(err)
		}
		v3, err := ev.Commit()
		if err != nil {
			t.Fatal(err)
		}
		row, found, err := tb.Snapshot(v2.Version()).Get(int64(1))
		if err != nil || !found {
			t.Fatalf("v2 read: found=%v err=%v", found, err)
		}
		if _, hasAge := row["age"]; hasAge {
			t.Fatal("v2 schema must not contain age")
		}
		row3, found, _ := tb.Snapshot(v3.Version()).Get(int64(1))
		if !found || row3["age"] != int64(18) {
			t.Fatalf("history row under new schema must get default 18, got %#v", row3)
		}
		row3b, _, _ := tb.Latest().Get(int64(1))
		if row3b["age"] != int64(18) {
			t.Fatalf("default not stable: %#v", row3b)
		}
		log.pass("加列后历史行得到确定缺省值")
	})

	t.Run("可空加列缺省NULL且默认值校验", func(t *testing.T) {
		tb, _ := openTable(t)
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(7), "name": "g"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		log.casef("可空加列缺省NULL且默认值校验", "加可空 string 列 remark 历史行为 NULL；非空无默认/默认类型错均拒绝")
		ev := tb.Begin()
		if err := ev.AddColumn("remark", TypeString, true, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := ev.Commit(); err != nil {
			t.Fatal(err)
		}
		row, found, _ := tb.Latest().Get(int64(7))
		if !found || row["remark"] != nil {
			t.Fatalf("want NULL default, got %#v", row)
		}
		bad := tb.Begin()
		if err := bad.AddColumn("score", TypeInt, false, nil); !errors.Is(err, ErrUnsupportedEvolution) {
			t.Fatalf("non-nullable add without default must be rejected, got %v", err)
		}
		bad2 := tb.Begin()
		if err := bad2.AddColumn("x", TypeInt, false, "not-int"); !errors.Is(err, ErrTypeMismatch) {
			t.Fatalf("wrong-typed default must be ErrTypeMismatch, got %v", err)
		}
		log.pass("可空加列缺省NULL且默认值校验")
	})

	t.Run("删列后残留同名列不影响结果", func(t *testing.T) {
		tb, _ := openTable(t)
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "alice"}); err != nil {
			t.Fatal(err)
		}
		v2, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		log.casef("删列后残留同名列不影响结果", "删 name 后重加同名 int 列，物理按列 ID 隔离不串列")
		ev := tb.Begin()
		if err := ev.DropColumn("name"); err != nil {
			t.Fatal(err)
		}
		if _, err := ev.Commit(); err != nil {
			t.Fatal(err)
		}
		ev2 := tb.Begin()
		if err := ev2.AddColumn("name", TypeInt, true, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := ev2.Commit(); err != nil {
			t.Fatal(err)
		}
		tx2 := tb.Begin()
		if err := tx2.Upsert(map[string]any{"id": int64(2), "name": int64(99)}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx2.Commit(); err != nil {
			t.Fatal(err)
		}
		row, found, _ := tb.Latest().Get(int64(1))
		if !found || row["name"] != nil {
			t.Fatalf("old physical residual must not leak into new same-named column: %#v", row)
		}
		row2, found, _ := tb.Latest().Get(int64(2))
		if !found || row2["name"] != int64(99) {
			t.Fatalf("new column value wrong: %#v", row2)
		}
		old, found, _ := tb.Snapshot(v2.Version()).Get(int64(1))
		if !found || old["name"] != "alice" {
			t.Fatalf("historical read changed after drop/readd: %#v", old)
		}
		log.pass("删列后残留同名列不影响结果")
	})

	t.Run("改名后旧名报错不静默错列", func(t *testing.T) {
		tb, _ := openTable(t)
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "alice"}); err != nil {
			t.Fatal(err)
		}
		v2, err := tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
		log.casef("改名后旧名报错不静默错列", "name 改名为 full_name（列 ID 不变），按旧名写入返回 ErrColumnNotFound")
		ev := tb.Begin()
		if err := ev.RenameColumn("name", "full_name"); err != nil {
			t.Fatal(err)
		}
		if _, err := ev.Commit(); err != nil {
			t.Fatal(err)
		}
		bad := tb.Begin()
		err = bad.Upsert(map[string]any{"id": int64(2), "name": "bob"})
		if !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("write by old name must fail with ErrColumnNotFound, got %v", err)
		}
		row, found, _ := tb.Latest().Get(int64(1))
		if !found || row["full_name"] != "alice" {
			t.Fatalf("renamed column must keep value by same id: %#v", row)
		}
		if _, ok := row["name"]; ok {
			t.Fatal("old name must not be resolvable after rename")
		}
		old, _, _ := tb.Snapshot(v2.Version()).Get(int64(1))
		if old["name"] != "alice" {
			t.Fatalf("historical snapshot must still read by old name: %#v", old)
		}
		log.pass("改名后旧名报错不静默错列")
	})

	t.Run("不支持的变更被明确拒绝", func(t *testing.T) {
		tb, _ := openTable(t)
		log.casef("不支持的变更被明确拒绝", "删主键列、加重复列、改成已存在名、改不存在列均返回可区分错误")
		ev := tb.Begin()
		if err := ev.DropColumn("id"); !errors.Is(err, ErrUnsupportedEvolution) {
			t.Fatalf("drop PK must be ErrUnsupportedEvolution, got %v", err)
		}
		if err := ev.AddColumn("name", TypeString, true, nil); !errors.Is(err, ErrColumnExists) {
			t.Fatalf("duplicate add must be ErrColumnExists, got %v", err)
		}
		if err := ev.RenameColumn("name", "id"); !errors.Is(err, ErrColumnExists) {
			t.Fatalf("rename to existing name must be ErrColumnExists, got %v", err)
		}
		if err := ev.RenameColumn("ghost", "x"); !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("rename missing column must be ErrColumnNotFound, got %v", err)
		}
		log.pass("不支持的变更被明确拒绝")
	})
}

// TestUpsertDelete 按主键更新/删除、重复写确定性、墓碑可靠生效。
func TestUpsertDelete(t *testing.T) {
	log := newBizLog(t, "按主键更新/删除（重复写确定性 + 墓碑生效）")

	t.Run("同主键重复写最后一次为准且跨文件归并确定", func(t *testing.T) {
		tb, _ := openTable(t)
		for i, name := range []string{"v1", "v2", "v3"} {
			tx := tb.Begin()
			if err := tx.Upsert(map[string]any{"id": int64(1), "name": name}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			log.casef("同主键重复写最后一次为准且跨文件归并确定", "第%d次写 id=1 name=%s", i+1, name)
		}
		row, found, _ := tb.Latest().Get(int64(1))
		if !found || row["name"] != "v3" {
			t.Fatalf("last write must win deterministically: %#v", row)
		}
		if rows, _ := tb.Latest().Scan(); len(rows) != 1 {
			t.Fatalf("duplicate pk must collapse to one row: %d", len(rows))
		}
		log.pass("同主键重复写最后一次为准且跨文件归并确定")
	})

	t.Run("删除墓碑可靠生效且可重新插入", func(t *testing.T) {
		tb, _ := openTable(t)
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "a"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		log.casef("删除墓碑可靠生效且可重新插入", "Delete id=1 后查不到；删不存在 id=9 幂等；再插入可见")
		dt := tb.Begin()
		if err := dt.Delete(int64(1)); err != nil {
			t.Fatal(err)
		}
		if _, err := dt.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, found, _ := tb.Latest().Get(int64(1)); found {
			t.Fatal("tombstone must hide the row")
		}
		dt2 := tb.Begin()
		if err := dt2.Delete(int64(9)); err != nil {
			t.Fatal(err)
		}
		if _, err := dt2.Commit(); err != nil {
			t.Fatal(err)
		}
		rt := tb.Begin()
		if err := rt.Upsert(map[string]any{"id": int64(1), "name": "resurrected"}); err != nil {
			t.Fatal(err)
		}
		if _, err := rt.Commit(); err != nil {
			t.Fatal(err)
		}
		row, found, _ := tb.Latest().Get(int64(1))
		if !found || row["name"] != "resurrected" {
			t.Fatalf("reinsert after delete must win over tombstone: %#v", row)
		}
		log.pass("删除墓碑可靠生效且可重新插入")
	})

	t.Run("事务内重复写同键最后一次为准", func(t *testing.T) {
		tb, _ := openTable(t)
		log.casef("事务内重复写同键最后一次为准", "同一事务两次 Upsert(id=1) 只产生一条最终记录")
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "first"}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "second"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		rows, err := tb.Latest().Scan()
		if err != nil || len(rows) != 1 || rows[0]["name"] != "second" {
			t.Fatalf("want single final row, got %#v err=%v", rows, err)
		}
		log.pass("事务内重复写同键最后一次为准")
	})
}

func snapshotDigest(t *testing.T, tb *Table, version int64) string {
	t.Helper()
	rows, err := tb.Snapshot(version).Scan()
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(rows))
	body := map[string]Row{}
	for _, r := range rows {
		k := primaryKeyString(r["id"])
		keys = append(keys, k)
		body[k] = r
	}
	sort.Strings(keys)
	out := fmt.Sprintf("v%d[", version)
	for i, k := range keys {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%s=%v", k, body[k])
	}
	return out + "]"
}

// TestCompactHistory 小文件合并不改变任何历史快照读取结果。
func TestCompactHistory(t *testing.T) {
	log := newBizLog(t, "小文件合并（compaction 不改变历史读取）")

	t.Run("合并后各历史版本结果逐行一致", func(t *testing.T) {
		tb, _ := openTable(t)
		log.casef("合并后各历史版本结果逐行一致", "制造更新/删除共5次提交后 compact，逐版本对比合并前后结果")

		commitUpsert := func(id int64, name string) {
			t.Helper()
			tx := tb.Begin()
			if err := tx.Upsert(map[string]any{"id": id, "name": name}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		commitDelete := func(id int64) {
			t.Helper()
			tx := tb.Begin()
			if err := tx.Delete(id); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}

		commitUpsert(1, "alice")  // v2
		commitUpsert(2, "bob")    // v3
		commitUpsert(1, "alice2") // v4
		commitDelete(2)           // v5 墓碑
		commitUpsert(3, "carol")  // v6

		before := map[int64]string{}
		for v := int64(1); v <= 6; v++ {
			before[v] = snapshotDigest(t, tb, v)
		}

		cv, err := tb.Compact()
		if err != nil {
			t.Fatal(err)
		}
		if cv.Version() != 7 || len(cv.snap.Files) != 1 {
			t.Fatalf("compact snapshot wrong: version=%d files=%d", cv.Version(), len(cv.snap.Files))
		}

		rows, err := cv.Scan()
		if err != nil || len(rows) != 2 {
			t.Fatalf("compact result should contain 2 live rows: %#v err=%v", rows, err)
		}
		byID := map[int64]string{}
		for _, r := range rows {
			byID[r["id"].(int64)] = r["name"].(string)
		}
		if byID[1] != "alice2" || byID[3] != "carol" {
			t.Fatalf("compacted rows wrong: %#v", byID)
		}

		for v := int64(1); v <= 6; v++ {
			after := snapshotDigest(t, tb, v)
			if after != before[v] {
				t.Fatalf("history changed after compact for v%d:\nbefore=%s\nafter =%s", v, before[v], after)
			}
		}
		t.Logf("    合并前后 v1..v6 摘要完全一致；v7 活行=%d，墓碑已随重写丢弃", len(rows))
		log.pass("合并后各历史版本结果逐行一致")
	})

	t.Run("合并后演进加列的历史缺省仍然确定", func(t *testing.T) {
		tb, _ := openTable(t)
		log.casef("合并后演进加列的历史缺省仍然确定", "compact 后加列 score=默认0，活行按 ID 投影得到缺省")
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "a"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tb.Compact(); err != nil {
			t.Fatal(err)
		}
		ev := tb.Begin()
		if err := ev.AddColumn("score", TypeInt, false, int64(0)); err != nil {
			t.Fatal(err)
		}
		if _, err := ev.Commit(); err != nil {
			t.Fatal(err)
		}
		row, found, _ := tb.Latest().Get(int64(1))
		if !found || row["score"] != int64(0) {
			t.Fatalf("default after compact+evolve wrong: %#v", row)
		}
		log.pass("合并后演进加列的历史缺省仍然确定")
	})

	t.Run("合并只重写活行且可再次整理", func(t *testing.T) {
		tb, dir := openTable(t)
		log.casef("合并只重写活行且可再次整理", "空表/全删 compact 产生空清单；重复 compact 幂等")
		tx := tb.Begin()
		if err := tx.Upsert(map[string]any{"id": int64(1), "name": "a"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		dt := tb.Begin()
		if err := dt.Delete(int64(1)); err != nil {
			t.Fatal(err)
		}
		if _, err := dt.Commit(); err != nil {
			t.Fatal(err)
		}
		cv, err := tb.Compact()
		if err != nil {
			t.Fatal(err)
		}
		if rows, _ := cv.Scan(); len(rows) != 0 {
			t.Fatalf("all-deleted table compact must be empty, got %d", len(rows))
		}
		if len(cv.snap.Files) != 0 {
			t.Fatalf("empty compact must have empty manifest, got %d", len(cv.snap.Files))
		}
		cv2, err := tb.Compact()
		if err != nil {
			t.Fatal(err)
		}
		if rows, _ := cv2.Scan(); len(rows) != 0 {
			t.Fatalf("repeated compact not idempotent: %d", len(rows))
		}
		tb.Close()
		tb2, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if rows, _ := tb2.Latest().Scan(); len(rows) != 0 {
			t.Fatalf("state after reopen wrong: %d", len(rows))
		}
		log.pass("合并只重写活行且可再次整理")
	})
}
