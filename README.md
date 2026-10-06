# Lakehouse Table Manager（本地文件系统版）

用本地文件系统模拟湖仓表存储，提供事务提交、崩溃恢复、结构演进、小文件合并、
快照历史读取、按版本恢复与历史版本保留清理。零第三方依赖，标准库实现。

## 目录与元数据模型

```
<table-dir>/
├── metadata/
│   ├── table.json          # 表级指针：current_version / schema_version
│   ├── history.json        # 版本生命周期清单：现存/已过期版本、高水位、保留策略与 pin
│   ├── snap-<v>.json       # 每个版本一份不可变快照清单 SnapshotMeta
│   └── schema-<v>.json     # 每个 schema 版本一份不可变 Schema
├── data/
│   ├── v-upsert-<v>-<rand>.jsonl       # 不可变数据文件（物理行=列ID->值）
│   ├── v-delete-<v>-<rand>.jsonl       # 删除提交的清单占位
│   ├── v-delete-<v>-<rand>.deletes.json# 删除标记 sidecar（物理主键集合）
│   └── v-compact-<v>-<rand>.jsonl      # 整理产生的单一数据文件
└── txns/
    └── commit.lock         # flock 跨进程提交互斥锁
```

### 快照模型

- `table.json.current_version` 是唯一的“已发布”指针；所有查询从它出发。
- `SnapshotMeta` 不可变：含父版本、绑定的 `schema_id`、有序文件清单、操作类型。
- 查询某版本时读取该版本清单与其绑定 schema；后续提交**不可改变**任何旧快照。
- 读取按文件清单顺序回放：upsert 后写覆盖前写；delete 以 sidecar 标记删除；
  删除后再插入同主键可复活；最终行按物理主键排序，输出确定。
- 主键物理编码是规范化标量拼接（内存行与 JSONL 读回行编码一致），
  批内同主键最后一条胜出，跨快照后者覆盖前者。

### 列身份模型（结构演进的基础）

- 列以 **列 ID** 为稳定身份；数据文件按 `列ID -> 值` 存储，不按列名位置存储。
- 加列：`NextColID` 单调递增，**删除过的 ID 永不复用**；历史行缺该 ID 时读出 `nil`，
  这就是加列后对历史数据确定且稳定的缺省值。
- 删列：列从当前 schema 移除；历史文件里残留的同 ID 数据在读取投影时直接忽略；
  旧名字被登记，按旧名访问报 `SCHEMA_COLUMN_NOT_FOUND`。
- 改名：**只改名字、ID 不变**，改名历史永久保留；按旧名读取/写入/查询报
  `SCHEMA_COLUMN_RENAMED`，绝不会静默错列。
- 每个历史快照绑定其提交当时的 schema，因此旧名读取旧版本永远与当时一致。
- 明确拒绝的不安全变更（带可区分错误码）：
  - 类型修改：`SCHEMA_TYPE_CHANGE_UNSUPPORTED`（需加新列并显式回填）；
  - 删除/改名主键列：`SCHEMA_IS_PRIMARY_KEY`；
  - 与现有列重名：`SCHEMA_COLUMN_EXISTS`。

## 提交协议（事务 + 并发 + 崩溃恢复）

1. 获取 `txns/commit.lock`（flock）。普通提交/演进/整理/恢复/清理会阻塞等待
   锁并串行执行；锁只负责把改表操作排成确定顺序，真正的冲突判定在下一步 CAS。
2. **乐观并发 CAS**：`baseVersion == current_version` 才继续，否则返回
   `CONFLICT_SNAPSHOT_STALE`——基于同一快照的并发提交恰好一个成功，
   其余以该明确原因失败，调用方可基于最新版本重试。
   每个句柄在持锁后都以**磁盘上的 table.json/history.json 重新装载状态**，
   因此同一进程多个独立句柄、多个进程同时打开同一张表时，后到者绝不会用
   陈旧内存缓存覆盖或吃掉别人已经确认成功的数据。
3. 写不可变数据文件与删除 sidecar（文件名含随机后缀，崩溃孤儿文件永不与
   后续提交重名）。
4. 写新快照元数据 `snap-<v>.json`（此刻仍不可见）。
5. **原子翻转** `table.json`（临时文件 fsync + rename）——唯一可见性切换点：
   查询要么看到上一个完整快照，要么看到新的完整快照，从无中间状态。
6. 更新内存状态。

崩溃恢复（重新 `Open`）：

- 崩溃发生在步骤 3/4（current 未翻转）：提交完全不可见；遗留文件成为孤儿，
  可由 GC 回收；旧快照完整可读。
- 崩溃发生在步骤 5 之后：current 已发布，提交完整可见。
- 进程死亡后内核自动释放 flock；`Open` 清理 `txns/*.lock` 残留并以 current 为准装载。

## 小文件合并（Compact）规则

- 回放当前快照得到全部存活行（删除标记可靠生效，墓碑不写入新文件），
  合并为**一个**新数据文件并生成新快照；文件内按主键排序。
- 旧数据文件与旧快照清单原样保留，因此 **v1..vn 任一历史版本的读取结果
  与整理前逐行一致**。
- Compact 与普通提交共用同一把锁和 CAS，基版本过期返回 `CONFLICT_SNAPSHOT_STALE`。
- `GCUnreferencedFiles` 只删除“不被任何已发布快照引用”的文件，
  历史快照引用的文件一律保留。

## 按历史版本恢复（Restore）

`RestoreToVersion(baseVersion, targetVersion, message)` 把表拉回 targetVersion
当时的样子，规则如下：

- **恢复是一次新的提交**：产生一个 `operation=restore` 的新快照，版本号在
  高水位 `history.next_version` 上继续递增，**永不回收重用任何旧版本号**。
- **当前表逐行回到目标状态**：恢复后 `Head().Scan()` 与 `Snapshot(targetVersion)`
  的结果（包括可见列——加列/删列/改名都随之回到当时 schema）逐行一致。
  实现上恢复快照**复用目标版本的不可变文件清单**，并把目标 schema 的内容
  克隆为一个新的 schema 版本（列 ID 完全沿用，物理文件按列 ID 对齐）。
- **绝不改写历史**：targetVersion、恢复前的最新版本以及其他任何历史版本的
  读取结果都不变；恢复前的最新状态之后仍可直接读回，也可以再恢复回去。
- **失败原因可分辨**：目标版本已被清理返回 `SNAPSHOT_EXPIRED`；
  压根不存在返回 `SNAPSHOT_NOT_FOUND`；baseVersion 过期返回
  `CONFLICT_SNAPSHOT_STALE`。
- **崩溃安全**：恢复不产生新数据文件，发布顺序仍是
  `新 schema -> 新快照 -> table.json 原子翻转`。在指针翻转前的任何时刻
  崩溃/被杀，重开后表还是恢复前的完整版本；翻转后即为完整恢复结果，
  不存在“恢复了一半”的可见状态。
- `RestoredFrom(version)` 可查询某快照的恢复来源版本。

## 历史版本保留与清理（Retention / Expire）

策略结构 `RetentionPolicy{KeepVersions int; Pinned []int64}` 落盘在
`metadata/history.json`，重新打开表后继续生效：

- **默认行为不变**：`KeepVersions <= 0`（默认）时保留全部历史，
  `ExpireOldVersions` 是无操作。
- **最近 N 个版本**：`KeepVersions = N > 0` 后，只保留“当前仍存在版本里
  最新的 N 个”；超出窗口的旧版本可被清理。
- **长期保留 pin**：`PinVersions(v...)` 标记的版本无论多老都不清理，
  `UnpinVersions` 取消标记；**当前版本永远不被动**。
- 配置入口：`SetRetentionPolicy(baseVersion, policy)`（只改窗口大小，
  已有 pin 标记原样保留；策略本身是元数据，不产生新版本号）；
  查询入口：`RetentionPolicyView()`。
- 执行入口：`ExpireOldVersions(baseVersion) (*ExpireReport, error)`，
  返回被清理版本与被删除物理文件的相对路径列表。

清理协议（崩溃安全、不误伤）：

1. 持提交锁、CAS 校验基版本；计算受害者集合（超窗、未 pin、非当前）。
2. 汇总**清理后仍保留版本**引用的全部数据文件与删除 sidecar，作为删除保护集合。
   append 模型下旧 upsert 文件会被后续快照共享，因此受保护文件绝不删除；
   只有旧版本独占的文件（典型为 compact 之前的小文件、旧删除 sidecar）才释放。
   恢复产生的新快照复用旧文件时，被恢复依赖的文件同样受保护。
3. **先原子翻转 `history.json` 清单，再删物理文件**。清单翻转后被清理版本
   立刻表现为 `SNAPSHOT_EXPIRED`——即使此时物理文件还没删完，也绝不会
   返回空结果或前后对不上的内容。
4. 删除受害者的快照/schema 元数据与独占数据文件，释放磁盘空间。
   中断后重新 `Open` 或再次调用 `ExpireOldVersions` 会**续删**剩余文件，
   重复调用完全幂等。

被清理之后的兼容范围：

| 操作 | 结果 |
| --- | --- |
| 读现存版本（最近 N 内 / pin / 当前） | 与清理前逐行一致 |
| 读已被清理的版本 | `SNAPSHOT_EXPIRED`，明确报错，绝不返回空结果 |
| 读从未存在的版本 | `SNAPSHOT_NOT_FOUND` |
| 恢复到已清理版本 | `SNAPSHOT_EXPIRED` |
| pin / unpin 已清理版本 | `SNAPSHOT_EXPIRED`（pin）/ 成功（unpin 幂等） |
| 对已清理版本之后的新版本做提交/恢复 | 正常，版本号继续递增 |

## 错误码（可区分原因）

| 错误码 | 含义 |
| --- | --- |
| `CONFLICT_SNAPSHOT_STALE` | 基快照过期，乐观并发冲突（可重试） |
| `CONFLICT_IN_PROGRESS` | 另一提交正持有提交锁（非阻塞抢锁场景） |
| `SCHEMA_COLUMN_NOT_FOUND` | 列不存在或已删除 |
| `SCHEMA_COLUMN_RENAMED` | 按已改名列的旧名访问 |
| `SCHEMA_COLUMN_EXISTS` | 列名冲突 |
| `SCHEMA_IS_PRIMARY_KEY` | 试图删除/改名主键列 |
| `SCHEMA_TYPE_CHANGE_UNSUPPORTED` | 类型变更被拒绝 |
| `VALIDATION_FAILED` | 行数据违反 schema（类型/非空/缺列） |
| `SNAPSHOT_NOT_FOUND` | 查询/恢复了一个从未存在的版本 |
| `SNAPSHOT_EXPIRED` | 版本曾存在但已被保留策略清理 |
| `RETENTION_POLICY_INVALID` | 保留策略参数非法（如负数） |

判定方式：`lakehouse.ErrorCode(err)` 或 `lakehouse.IsConflict(err)`。

## 最小用法

```go
sch, _ := lakehouse.NewSchema([]string{"id"},
    lakehouse.Column{Name: "id", Type: lakehouse.TypeInt, Nullable: false},
    lakehouse.Column{Name: "name", Type: lakehouse.TypeString, Nullable: true},
)
tb, _ := lakehouse.Create(ctx, "/tmp/t", "users", sch, nil)

v1, err := tb.Commit(ctx, tb.CurrentVersion(), lakehouse.OpUpsert,
    []lakehouse.Row{{"id": int64(1), "name": "alice"}}, "insert")
_, err = tb.Commit(ctx, v1, lakehouse.OpDelete,
    []lakehouse.Row{{"id": int64(1)}}, "delete")

r, _ := tb.Snapshot(ctx, v1)     // 历史版本只读视图
rows, _ := r.Scan(ctx)           // 时间旅行读取
row, _ := r.Get(ctx, lakehouse.Row{"id": int64(1)})

_, _ = tb.AddColumn(ctx, tb.CurrentVersion(),
    lakehouse.Column{Name: "city", Type: lakehouse.TypeString, Nullable: true})
_, _ = tb.RenameColumn(ctx, tb.CurrentVersion(), "name", "full_name")
_, _ = tb.DropColumn(ctx, tb.CurrentVersion(), "city")
_, _ = tb.Compact(ctx, tb.CurrentVersion())

// 版本生命周期：恢复 + 保留策略与清理
vr, _ := tb.RestoreToVersion(ctx, tb.CurrentVersion(), v1, "rollback")
_ = tb.SetRetentionPolicy(ctx, tb.CurrentVersion(),
    lakehouse.RetentionPolicy{KeepVersions: 10})
_ = tb.PinVersions(ctx, vr) // 长期保留
report, _ := tb.ExpireOldVersions(ctx, tb.CurrentVersion())
// report.ExpiredVersions / report.RemovedFiles
```

## 本地复现验证

要求 Go 1.25+。

```bash
# 全部单测（带业务/用例中文日志，可直接看到覆盖了哪些业务）
go test -count=1 -v ./...

# 竞态检测（需 cgo）
CGO_ENABLED=1 go test -count=1 -race ./...

# 可运行的端到端示例（并发冲突/加列缺省/删除/整理/历史读取）
go test -count=1 -v -run Example ./...
```

测试日志中用 `[业务:xxx]` 标识业务域、`[用例:yyy]` 标识具体用例与结果：

- `[业务:并发提交]`：8 个同基快照并发提交，恰好 1 成功，7 个
  `CONFLICT_SNAPSHOT_STALE`；无中间状态可见；失败方可重试成功。
- `[业务:重启恢复]`：子进程分别在“数据文件写后/快照元数据写后”崩溃，
  重启后崩溃提交不可见、已确认提交完整可见、版本连续、孤儿文件可 GC。
- `[业务:结构演进]`：加列历史缺省 `nil`、改名按 ID 对齐且旧名报 RENAMED、
  删列残留忽略、同名加列不复用 ID、不安全变更被拒绝、最老快照读取不变。
- `[业务:更新删除与整理]`：批内/跨快照重复主键确定性合并、删除生效、
  删除后重插复活、整理后 v1..v4 逐版本结果不变、整理后删除仍生效、过期整理冲突。
- `[业务:快照隔离与历史读取]`：旧 Reader 视图冻结、可时间旅行、非法版本报错。
- `[业务:版本恢复]`：恢复后当前表（列+数据）与目标版本逐行一致、
  恢复前状态与历史版本原样可读、版本号只增不重用、不存在/已清理目标可分辨、
  恢复与普通提交同基线竞争恰好一个成功。
- `[业务:保留策略与清理]`：默认全保留、最近 N + pin 清理、物理文件实际释放、
  过期读报 `SNAPSHOT_EXPIRED`、保留版本逐行不变、策略重开仍生效、
  恢复快照共享文件不被误删、unpin 后可回收。
- `[业务:生命周期中断恢复]`：恢复在指针翻转前崩溃重开仍是恢复前完整状态；
  清理在清单翻转后崩溃，重开后续删、过期状态即时生效、保留版本与当前版本不受伤。
- `[业务:多句柄并发]`：同一进程多个独立 Open 句柄同基线写，恰好 1 个成功、
  其余 stale；陈旧句柄的提交/演进/整理/恢复全部被拒，刷新后可续写且
  别人已确认的数据不丢。
- `[业务:多进程并发]`：6 个独立进程同时打开同表同基线写，flock + 磁盘 CAS
  保证跨进程恰好 1 个提交成功、5 个 stale，重开后数据完整，失败进程可重试。
