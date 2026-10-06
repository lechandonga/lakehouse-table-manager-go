# Lakehouse Table Manager（本地文件系统版）

用本地文件系统模拟湖仓表存储，提供事务提交、崩溃恢复、结构演进、小文件合并、
快照历史读取、**按历史版本恢复（Rollback）** 与 **版本保留策略 + 旧版本清理
（Expire/Purge）**。零第三方依赖，标准库实现。

## 目录与元数据模型

```
<table-dir>/
├── metadata/
│   ├── table.json          # 表级指针：current_version / schema_version
│   ├── snap-<v>.json       # 每个版本一份不可变快照清单 SnapshotMeta
│   └── schema-<v>.json     # 每个 schema 版本一份不可变 Schema
│   └── retention.json      # 保留策略 + 已清理（过期）版本登记表
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

1. 获取 `txns/commit.lock`（flock，非阻塞）；被占用返回 `CONFLICT_IN_PROGRESS`。
2. **乐观并发 CAS**：`baseVersion == current_version` 才继续，否则返回
   `CONFLICT_SNAPSHOT_STALE`——基于同一快照的并发提交恰好一个成功，
   其余以该明确原因失败，调用方可基于最新版本重试。
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

## 按历史版本恢复（Restore / Rollback）

`tb.Restore(ctx, baseVersion, targetVersion, message) (newVersion, error)`：

- 把表恢复到某个**仍然存在**的历史版本，并把这次恢复作为**一次新提交**发布：
  - 恢复后直接读当前表（`Head`/`Scan`/`Get`）与读 `targetVersion` **逐行一致**，
    包括当时能看到的**列集合**——schema 视图也回到目标版本绑定的 schema；
  - 新版本号 = 当前版本 + 1，**只增、不复用、不回收**；旧快照一律不改写，
    因此恢复前的最新状态之后仍可按其版本号读回（恢复不等于删除历史）；
  - 恢复**不复制数据文件**：新快照直接引用目标版本的文件清单与 schema，
    不产生额外数据占用，也绝不改动旧版本依赖的任何文件。
- 崩溃安全：沿用“快照元数据 -> current 原子翻转”协议。current 翻转前进程被杀，
  恢复完全不可见（仍是恢复前版本）；翻转后即为完整恢复结果。**不存在恢复一半**。
- 冲突语义与普通提交一致：`baseVersion` 过期返回 `CONFLICT_SNAPSHOT_STALE`，
  锁被占用返回 `CONFLICT_IN_PROGRESS`。
- 目标版本可分辨的失败原因：
  - 目标从未存在 / 超过当前版本：`SNAPSHOT_NOT_FOUND`；
  - 目标已被保留策略清理：`SNAPSHOT_EXPIRED`。

恢复跨结构演进边界时的列身份规则不变（见上）：回到加列前，该列即消失，
写入只能用当时存在的列；之后再次加同名列仍分配**全新列 ID**，旧值不串列。

## 保留策略与旧版本清理（Retention / Purge）

### 配置策略（落盘持久化）

`tb.SetRetentionPolicy(ctx, RetentionPolicy{RetainVersions: N, Pinned: []int64{...}})`

- `RetainVersions`：保留**最近 N 个**已发布版本（始终包含当前版本）。
  `0` 表示不按数量裁剪；**零值策略（默认）= 历史全部保留**，与旧行为完全一致。
- `Pinned`：**长期保留**版本集合，即使超出最近 N 个窗口也不会被清理。
  便捷方法：`PinVersions` / `UnpinVersions`。
- 策略写入 `metadata/retention.json`，**表重新打开后仍生效**。
- 参数非法（负数 N、pin 未来版本、pin 已清理版本）分别返回
  `RETENTION_INVALID` / `PIN_TARGET_INVALID`。

设置策略本身不删任何数据；空间回收由显式清理完成。

### 执行清理

`res, err := tb.PurgeExpired(ctx)`，返回 `PurgeResult{ExpiredVersions,
RemovedFiles, RemovedSnapshots, RemovedSchemas}`。

- **当前版本永远保留、永不被动**；清理只作用于更旧的版本。
- 过期判定：既不在“最近 N 个”窗口内、也不在 `Pinned` 中。
- 清理内容（真正释放空间，不只是删记录）：
  - 删除过期版本的快照清单 `snap-<v>.json`；
  - 物理删除**仅被过期快照引用**的数据文件与删除 sidecar；
  - 删除不再被任何存活快照引用的旧 schema 文件；
  - **任何仍被存活版本引用的共享文件一律保留**（多个快照共享同一条追加文件链，
    只有当最后一个引用者也过期时才释放），因此清理绝不会伤到别的版本。
- 崩溃安全（两阶段）：先把过期版本**原子登记**进 `retention.json`，再物理删文件。
  - 登记后、删除中途被杀：重新 `Open` 会在提交锁保护下**自动续完**清理；
  - 无论崩在哪一步，读一个版本只有两种结局：**完整可读** 或
    **明确 `SNAPSHOT_EXPIRED`**，不会读到空结果或前后对不上的半截内容。
- 清理与提交/结构变更/恢复/整理共用同一把提交锁和同一套 CAS，
  要么按确定顺序串行，要么过期方以 `CONFLICT_SNAPSHOT_STALE` /
  `CONFLICT_IN_PROGRESS` 失败，互不拆台。

### 清理之后还能读什么

- 保留窗口内版本、`Pinned` 版本、当前版本：**逐行可读，结果与清理前完全一致**；
- 已清理版本：`Snapshot` / `Restore` 返回 **`SNAPSHOT_EXPIRED`**（可用
  `IsExpired(v)` 预判），不会返回空表，也不会与“版本从未存在”混淆；
- 从未存在 / 超过当前的版本号：仍是 `SNAPSHOT_NOT_FOUND`。

> 注意：若先 `Restore` 到很老的版本再配置激进清理，老版本所依赖的数据文件
> 仍由“当前恢复快照”引用而被保留；只有当它再次滑出保留窗口、且无任何存活
> 快照引用时，文件才会被释放。

`GCUnreferencedFiles` 语义不变：只回收“不被任何**已发布且未清理**快照引用”的
孤儿文件（如崩溃提交遗留），并与其它写操作共用提交锁，避免误删在途提交文件。

## 多句柄 / 多进程并发

- 同一张表可被同一进程内多个 `Open` 句柄、或多个进程同时打开。
- 所有改表操作（提交 / 结构演进 / 整理 / 恢复 / 清理 / 设策略）共用
  `txns/commit.lock`（flock）串行化发布，并用“基版本 == 当前版本”做 CAS。
  **同一基线上恰好一个写入成功**；其余得到 `CONFLICT_SNAPSHOT_STALE`
  （基版本过期）或 `CONFLICT_IN_PROGRESS`（锁正被持有），都是可分辨、可重试的失败。
- 已确认成功的数据**绝不会**被后到的写入悄悄覆盖或吃掉：版本号单调递增，
  current 指针只做原子前进。
- 一个句柄提交后，另一个旧句柄可调 `tb.Refresh(ctx)` 重新装载最新 current/快照/
  保留策略后再写；不刷新直接写只会得到 STALE，不会回退表状态。
- 进程死亡后内核自动释放 flock；锁文件本身不会被 `Open` 删除，以免破坏另一
  存活进程正在进行的提交。

## 错误码（可区分原因）

| 错误码 | 含义 |
| --- | --- |
| `CONFLICT_SNAPSHOT_STALE` | 基快照过期，乐观并发冲突（可重试） |
| `CONFLICT_IN_PROGRESS` | 另一提交正持有提交锁 |
| `SCHEMA_COLUMN_NOT_FOUND` | 列不存在或已删除 |
| `SCHEMA_COLUMN_RENAMED` | 按已改名列的旧名访问 |
| `SCHEMA_COLUMN_EXISTS` | 列名冲突 |
| `SCHEMA_IS_PRIMARY_KEY` | 试图删除/改名主键列 |
| `SCHEMA_TYPE_CHANGE_UNSUPPORTED` | 类型变更被拒绝 |
| `VALIDATION_FAILED` | 行数据违反 schema（类型/非空/缺列） |
| `SNAPSHOT_NOT_FOUND` | 查询了不存在的版本 |
| `SNAPSHOT_EXPIRED` | 版本曾存在但已被保留策略清理（不可读/不可恢复） |
| `PIN_TARGET_INVALID` | 长期保留标记的版本不存在、未来或已被清理 |
| `RETENTION_INVALID` | 保留策略参数非法（如负数保留版本数） |

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

// 恢复到历史版本（恢复本身是新版本；旧历史不改写）
restoredV, _ := tb.Restore(ctx, tb.CurrentVersion(), v1, "rollback to v1")

// 保留最近 5 个版本 + 长期保留 v2；显式清理并释放旧版本占用的空间
_ = tb.SetRetentionPolicy(ctx, lakehouse.RetentionPolicy{
    RetainVersions: 5,
    Pinned:         []int64{2},
})
res, _ := tb.PurgeExpired(ctx)           // res.RemovedFiles > 0 即真正回收的文件数
_, err = tb.Snapshot(ctx, 1)             // 已被清理 -> SNAPSHOT_EXPIRED
_ = restoredV
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
- `[业务:版本恢复]`：恢复即新提交、编号只增不复用、逐行+列集合一致、
  旧历史不改写可读回、恢复冲突与目标 NOT_FOUND 可分辨。
- `[业务:恢复中断恢复]`：子进程在恢复“快照写后/current 翻转后”被杀，
  重开要么停在恢复前、要么是完整恢复，无半截状态，之后可继续提交。
- `[业务:恢复与结构演进交叉]`：跨加列恢复后列消失、按旧 schema 读写、
  再加同名列得新 ID 不串值、旧 schema 文件保留可读。
- `[业务:保留策略与清理]`：默认全保留；最近 N + pin 的过期判定；
  当前版本永不动；共享文件不误删、独占文件物理释放、磁盘收敛；
  过期版本读/恢复/pin 报 EXPIRED/INVALID；策略落盘；多轮增量清理。
- `[业务:清理中断恢复]`：子进程在“登记过期后/删数据后”被杀，重开自动续完，
  只有完整可读或 EXPIRED 两种结局，清理可重入。
- `[业务:多句柄并发]`：同进程 6 个独立句柄同基线提交恰好 1 成功，
  其余 STALE/IN_PROGRESS；Refresh 后看到完整结果、无丢写；陈旧句柄被拒。
- `[业务:多进程并发]`：6 个独立进程同基线提交恰好 1 成功，
  版本只前进一次、无覆盖无丢写。
