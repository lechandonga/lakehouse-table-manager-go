package lakehouse_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/lakehouse-table-manager-go/lakehouse"
)

func Example() {
	ctx := context.Background()
	root, err := os.MkdirTemp("", "lakehouse-demo-")
	must(err)
	defer os.RemoveAll(root)
	dir := filepath.Join(root, "demo-table")

	sch, err := lakehouse.NewSchema([]string{"id"},
		lakehouse.Column{Name: "id", Type: lakehouse.TypeInt, Nullable: false},
		lakehouse.Column{Name: "name", Type: lakehouse.TypeString, Nullable: true},
	)
	must(err)
	tb, err := lakehouse.Create(ctx, dir, "demo", sch, nil)
	must(err)

	v1, err := tb.Commit(ctx, tb.CurrentVersion(), lakehouse.OpUpsert,
		[]lakehouse.Row{{"id": int64(1), "name": "alice"}}, "insert")
	must(err)

	// 两个基于同一快照 v1 的并发提交：恰好一个 committed，另一个 stale。
	var wg sync.WaitGroup
	results := make([]string, 2)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, cerr := tb.Commit(ctx, v1, lakehouse.OpUpsert,
				[]lakehouse.Row{{"id": int64(10 + i), "name": fmt.Sprintf("w%d", i)}}, "race")
			if cerr == nil {
				results[i] = "committed"
			} else if lakehouse.ErrorCode(cerr) == lakehouse.CodeConflictSnapshotStale {
				results[i] = "stale"
			} else {
				results[i] = cerr.Error()
			}
		}()
	}
	wg.Wait()
	sort.Strings(results)
	fmt.Println("concurrent:", results[0], results[1])

	// 时间旅行：v1 永远只有 alice。
	r1, err := tb.Snapshot(ctx, v1)
	must(err)
	rows, err := r1.Scan(ctx)
	must(err)
	fmt.Println("v1 rows:", len(rows))

	// 结构演进：加列后历史行缺省为 nil。
	v2, err := tb.AddColumn(ctx, tb.CurrentVersion(),
		lakehouse.Column{Name: "city", Type: lakehouse.TypeString, Nullable: true})
	must(err)
	r2, err := tb.Snapshot(ctx, v2)
	must(err)
	head, _ := r2.Scan(ctx)
	fmt.Println("alice city after add column:", head[0]["city"])

	// 删除 + 整理：删除标记生效；历史快照读取不变。
	v3, err := tb.Commit(ctx, v2, lakehouse.OpDelete,
		[]lakehouse.Row{{"id": int64(1)}}, "delete alice")
	must(err)
	vc, err := tb.Compact(ctx, v3)
	must(err)
	r3, _ := tb.Snapshot(ctx, v3)
	rowsNow, _ := r3.Scan(ctx)
	fmt.Println("rows after delete:", len(rowsNow))
	r1Again, _ := tb.Snapshot(ctx, v1)
	rowsAgain, _ := r1Again.Scan(ctx)
	fmt.Println("v1 rows after compact:", len(rowsAgain), "current version:", vc)

	// Output:
	// concurrent: committed stale
	// v1 rows: 1
	// alice city after add column: <nil>
	// rows after delete: 1
	// v1 rows after compact: 1 current version: 6
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
