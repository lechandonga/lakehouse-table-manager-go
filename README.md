# 本地湖仓表管理器（Go）

用本地文件系统模拟湖仓表存储，提供**快照隔离事务、乐观并发提交、崩溃恢复、
结构演进（加列/删列/改名）、按主键更新删除、小文件合并与任意历史版本读取**。
所有规则都是确定且可解释的：无第三方依赖，数据格式为 JSON/JSONL，可直接打开检查。

## 快速开始

```bash
go test -v ./...          # 运行业务单测，日志按业务分组
CGO_ENABLED=1 go test -race ./...
```

典型用法：

```go
schema, _ := lakehouse.NewSchema(
    lakehouse.Column{Name: "id", Type: lakehouse.TypeInt},      // 第一列即主键（非空）
    lakehouse.Column{Name: "name", Type: lakehouse.TypeString, Nullable: true},
)
tb, _ := lakehouse.Create("/data/tbl", schema)
defer tb.Close()

tx := tb.Begin()                              // 基于当前最新快照 v1
tx.Upsert(map[string]any{"id": 1, "name": "alice"})
view, err := tx.Commit()                      // 生成不可变快照 v2；并发冲突时
                                              // 返回 ErrConflictConcurrentCommit
rows, _ := tb.Snapshot(2).Scan()              // 任意历史版本，按提交当时结构读取
tb.Compact()                                  // 小文件合并，不改变历史读取
```

重开已存在的表：`tb, err := lakehouse.Open("/data/tbl")`，Open 会自动执行崩溃恢复。

## 存储布局与元数据模型

```
<table-root>/
  metadata/
    latest.json            # 最新快照指针：{version, path, checksum(sha256)}
    snap-0000000001.json   # 不可变快照（每个版本一个，永不原地修改）
    ...
  data/
    0000000002-0000000000.jsonl   # 不可变数据文件（一个普通提交一个）
    0000000007-compact-000000.jsonl
  txn/                       # 提交暂存区（中断后遗留内容在 Open 时清除）
  .lock                      # flock 排他提交锁（同时覆盖跨进程并发）
```

**快照 `Snapshot`（自包含、不可变）：**

| 字段 | 含义 |
| --- | --- |
| `version` / `parent` | 单调递增版本号；parent 串成历史链，也是 GC 的可达性依据 |
| `schema` | 提交当时的**全量结构**，历史快照永远自带结构，不依赖最新定义 |
| `files[]` | 该快照可见数据文件的完整清单（manifest）：路径、主键范围、记录条数 |
| `operation` | `create` / `upsert` / `delete` / `evolve` / `compact` |
| `commit_time` | UTC 提交时间 |

**数据文件（JSONL，每行一条记录）：**

```json
{"v":{"1":1,"2":"alice"}}
{"v":{"1":2},"t":true}
```

- 值按**列 ID**（整数）存储而非列名：`{"v": {<列ID>: <值>}, "t": <墓碑>}`。
- 读取时以快照结构把列 ID 投影成列名，因此改名/删列/重加同名列都不会串列。

## 结构（Schema）模型与演进规则

列的物理身份是 **列 ID**（建表时从 1 递增分配），逻辑身份是列名。

- **加列** `AddColumn(name, type, nullable, default)`
  - 必须给出类型匹配的缺省值，或者声明 `nullable=true` + `nil`（缺省填 NULL）；
    不允许“非空且无默认”的加列（历史行无法安全填充）。
  - 读取加列前写入的历史行时，按列定义中的默认值投影——结果确定且稳定。
  - 旧版本快照不含该列，读旧版本永远看不到新列。
- **删列** `DropColumn(name)`：从结构中移除 ID；历史数据文件中的残留值仍在，
  但投影时被丢弃，**永不影响结果**。主键列禁止删除（`ErrUnsupportedEvolution`）。
- **改名** `RenameColumn(old, new)`：只改名字、ID 不变、数据零搬迁。
  改名后按旧名写入立即返回 `ErrColumnNotFound`，不会静默错列；
  历史快照仍按当时的旧名读取。删列后允许重加同名列（分配全新 ID，互不干扰）。
- **明确不支持并拒绝的变更**（返回可区分错误）：列类型变更、主键变更/删除主键列、
  非空无默认加列、默认值类型不符、重名列、对不存在列操作。
- 支持类型：`int`（int64）、`float`（float64）、`string`、`bool`。

错误均可通过 `errors.Is` 区分：`ErrConflictConcurrentCommit`、`ErrUnsupportedEvolution`、
`ErrColumnExists`、`ErrColumnNotFound`、`ErrTypeMismatch`、`ErrNullViolation`、
`ErrPrimaryKeyMissing`、`ErrSnapshotNotFound`、`ErrTableCorrupted`、`ErrTxnClosed`。

## 事务、并发与提交协议

- `Begin()` 固定一个**基线快照版本**；`Upsert/Delete/AddColumn/...` 只改内存暂存，
  未提交数据对任何查询不可见（快照隔离，无中间态）。
- 提交时（进程内 mutex + `flock` 双重串行化发布）重读 `latest.json`：
  - 基线已被其他确认提交推进 → 本次提交失败，返回
    `ErrConflictConcurrentCommit: base=<旧> current=<新>`（原因可区分、可解释）；
    事务以最新快照重新 `Begin()` 后可重试。
  - 基线相同 → 生成新版本。
- **发布顺序（崩溃安全）**：
  1. 记录按主键排序写入 `txn/<ver>.jsonl`，`fsync` 后 `rename` 到 `data/` 并 fsync 目录；
  2. 写不可变快照 `snap-<ver>.json`（父版本清单 + 新文件 + 新结构）；
  3. 原子写 `latest.json`（临时文件 + rename，带快照 sha256）并 fsync 目录。
  只有第 3 步完成提交才算“已确认”。

**崩溃恢复（`Open` 时执行）**：

1. 清空 `txn/`，删除 `*.tmp`；
2. 从 `latest.json` 沿 `parent` 链求可达快照集合，以及这些快照清单引用的数据文件集合；
3. 删除任何不可达快照与数据文件。

因此：已确认提交（latest 已切换）完整可见；`data-renamed`、`snapshot-written`
两个中断点留下的孤儿文件/孤儿快照被回收，未完成改动彻底不可见。
`latest.json` 与快照均为“临时文件 + 原子 rename”，指针带校验和，损坏可被发现。

## 更新、删除与归并语义

- `Upsert(row)`：整行按主键写入，必须带主键；同事务重复写同键只保留最后一次。
- `Delete(pk)`：写墓碑记录（即使主键原本不存在也幂等生效）。
- 跨多个数据文件的同一主键，按**文件路径升序、文件内靠后者胜**的全序归并，
  墓碑覆盖任何先前值；墓碑之后的再次写入会“复活”该行。重复写入结果因此完全确定。
- `Scan()` 按主键升序返回全部存活行；`Get(pk)` 返回单行。

## 小文件合并（Compaction）规则

`Compact()` 在最新快照上：

1. 按上述全序规则归并全部文件，只保留每个主键的最终**活行**；
2. 墓碑随重写丢弃（被删行不再出现，查询效果不变）；
3. 活行按主键排序写入单个新文件，生成 `op=compact` 的新快照，新清单只含新文件；
4. 旧文件不会被主动删除——它们仍被历史快照引用；下次任何 `Open` 的可达性 GC
   会自动回收不再被任何快照引用的文件。

**保证：compaction 不修改任何旧快照与旧数据文件，对任意历史版本的查询结果
与提交当时逐行一致**（单测对 v1..vN 做合并前后摘要比对）。空表/全删整理产生空清单，
重复整理幂等。

## 本地复现与验证

```bash
# 1) 运行业务单测（日志按“业务 -> 用例 -> 结果”分组，覆盖情况一目了然）
go test -v ./...

# 2) 带竞态检测
CGO_ENABLED=1 go test -race ./...

# 3) 只看某一业务
go test -v -run TestCrashRecovery    # 提交中断恢复（子进程 os.Exit 模拟掉电）
go test -v -run TestConcurrentCommit # 8 事务竞争：1 成功 7 冲突
go test -v -run TestSchemaEvolution  # 加列/删列/改名/拒绝项
go test -v -run TestCompactHistory   # 整理后历史读取逐行一致
```

测试结束后可直接检查 `$TMPDIR/Test*/.../tbl` 风格目录（崩溃恢复用例由子进程构造）：
`metadata/latest.json`、各 `snap-*.json` 与 `data/*.jsonl` 均为可读 JSON。

### 业务覆盖一览

| 业务 | 测试 | 关键断言 |
| --- | --- | --- |
| 并发提交 | `TestConcurrentCommit` | 8 个同基线事务仅 1 个成功，其余 `ErrConflictConcurrentCommit`；冲突重试成功；未提交不可见 |
| 中断恢复 | `TestCrashRecovery` | `data-renamed` / `snapshot-written` 中断后未完成提交不可见、孤儿被回收；已确认提交完整可见 |
| 结构演进 | `TestSchemaEvolution` | 加列历史默认值确定；删列残留隔离；同名重加不串列；改名旧名报错；非法变更可区分拒绝 |
| 更新删除 | `TestUpsertDelete` | 重复写最后者胜（跨文件全序）；墓碑可靠、幂等、可复活；事务内重复写确定 |
| 小文件合并 | `TestCompactHistory` | 合并后 v1..vN 逐行一致；墓碑丢弃；整理后加列默认值仍确定；空表/重复整理幂等 |
