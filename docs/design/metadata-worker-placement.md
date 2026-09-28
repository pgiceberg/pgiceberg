<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# 元数据 worker 部署粒度：core 与扩展对照

状态：设计依据；基础进程模型已见 [第 1 阶段](../metadata-workers.md)。查阅日期为 2026-09-28；PostgreSQL 对照 `REL_18_STABLE`，
扩展对照各官方仓库当日可见的 `main` 源码。链接指向会继续演进的分支，不表示所有
历史版本都采用相同实现。本文补充 [架构设计](metadata-backend.md) 的部署选择。

结论：当前 pgiceberg 采用实例 launcher + 每个显式启用库一个 metadata worker。
这是结合本项目配置、绑定和恢复方式作出的工程选择；PostgreSQL 并未要求元数据服务
必须按库，也不能从知名扩展的进程数量直接推导出集中 RPC 一定更快。

## 1. 两种方案的实际边界

按库方案让各库拥有自己的 catalog 客户端、metadata cache、私有事务和发布队列；
实例 launcher 负责进程管理与资源准入。用户请求直接访问本库 worker，不经过 launcher
转发。各库 worker 仍可能操作同一张外部 Iceberg 表。

实例方案由同一服务承接各库请求，内部仍须按 database、授权身份、事务和外部表身份
隔离状态。它可以是无数据库绑定的服务，也可以连接一个集中管理库；必要时另外通过
libpq 或配置解析进程访问各用户库。它不等于所有 I/O 必须串行，也不要求数据文件都
经过这个进程。

## 2. core 的实际模式

| 机制 | 已核对的进程结构 | 对 pgiceberg 的启示 |
| --- | --- | --- |
| Background writer | 实例级进程循环调用 `BgBufferSync`，处理共享 buffer 的脏页。[源码](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/bgwriter.c) | 管理对象本来就属于实例共享设施时，实例级服务合理；不等于所有表级元数据也应该集中 |
| Autovacuum | 常驻 launcher 选择数据库，postmaster 启动 worker；worker 连接一个库执行 vacuum，完成后退出。[源码](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/autovacuum.c) | 全局调度与库内执行可以分开；它不是为每个库永久保留一个 vacuum worker |
| Logical replication | 实例 launcher 发现 subscription，按 subscription 启动 apply worker，还可启动 tablesync/parallel apply worker。[源码](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/launcher.c) | 工作单元由业务状态决定；不能把逻辑复制误写成“每库一个 worker” |
| contrib pg_prewarm | autoprewarm leader 按 database 分组，依次启动连接相应库的 worker 加载 blocks。[源码](https://github.com/postgres/postgres/blob/REL_18_STABLE/contrib/pg_prewarm/autoprewarm.c) | 全局缓存管理和按库执行并不矛盾；按库组织也不必让所有执行进程同时常驻 |

Background writer 和 autovacuum 包含 core 专用辅助进程，不全是通过扩展
`RegisterBackgroundWorker` API 注册；不能照搬其私有启动接口和资源槽配置。

Logical launcher 能不连接某个用户库而枚举 subscription，是因为 `pg_subscription`
是实例共享系统目录。普通扩展创建的配置表没有这个属性，不能直接照抄该发现方式。
[pg_subscription 文档](https://www.postgresql.org/docs/18/catalog-pg-subscription.html)

## 3. 知名扩展的实际模式

| 扩展 | 已核对的实现 | 对 pgiceberg 的适用范围 |
| --- | --- | --- |
| TimescaleDB | cluster launcher 管理数据库 scheduler；scheduler 连接目标库，读取扩展版本，再启动具体 job workers。[launcher](https://github.com/timescale/timescaledb/blob/main/src/loader/bgw_launcher.c)、[scheduler](https://github.com/timescale/timescaledb/blob/main/src/bgw/scheduler.c) | 本地扩展配置与工作状态按库组织时，按库管理直接；可借鉴安装/升级事务完成后的重启与版本检查 |
| Citus | maintenance daemon 用 database OID 做共享注册表键，backend 可按需启动本库 daemon；另有配置 MainDb 的启动入口。[源码](https://github.com/citusdata/citus/blob/main/src/backend/distributed/utils/maintenanced.c) | 可借鉴按库认领、去重和生命周期；不能归纳为 Citus 必须有一个独立的全局 launcher |
| pg_cron | scheduler 连接配置的管理库；扩展仅安装在一个库。其他库的任务通过 libpq 连接或 job background worker 执行。[README](https://github.com/citusdata/pg_cron/blob/main/README.md)、[执行代码](https://github.com/citusdata/pg_cron/blob/main/src/pg_cron.c) | 集中持久配置后，全实例调度是成立的方案；集中 scheduler 并没有在自身 PG 上下文中反复切库 |

这些大多是调度或维护进程，不能直接当作“所有查询都向元数据进程 RPC”的既有证明。
例如 Citus 的 `metadata_cache.c` 在 backend 自己的 MemoryContext 中建立 metadata
hash caches，并注册失效回调；maintenance daemon 并不承接全部查询元数据读取。
[Citus metadata cache](https://github.com/citusdata/citus/blob/main/src/backend/distributed/metadata/metadata_cache.c)

## 4. 两条容易过度推导的限制

一个 background worker 的内部数据库连接只初始化一次，不能切换 database；它也可以
不绑定用户库，仅访问共享系统目录，或者通过 libpq 建立外部连接。所以该限制意味着
“一个进程不能用同一套 SPI 上下文逐个查询各库”，不意味着“一个服务不能服务各库”。
[Background worker API](https://www.postgresql.org/docs/18/bgworker.html)

同样，按库 worker 只是让正常错误处理、任务排队和资源配额更容易分开。PostgreSQL
对 background worker 的异常崩溃可能执行实例范围的 crash recovery；不能承诺某个
worker SIGSEGV 只影响一个库。[postmaster 的 CleanupBackend / HandleChildCrash](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/postmaster.c)

## 5. 对 pgiceberg 的比较

下表是根据上述机制和本项目代码作出的设计判断，不是性能测试结果。

| 维度 | 每个启用库一个 worker | 实例一个 worker |
| --- | --- | --- |
| 已提交配置与绑定 | 直接读取本库表，沿用现有安装模型 | 需要管理库、前端转发或跨库配置解析通道 |
| 用户未提交配置 | 仍需用户 backend 按自身快照解析后传入 | 同样需要，集中进程不能自动加入用户事务 |
| 重启恢复 | 可自行重读本库已提交配置 | 需确保重启后能独立恢复各库绑定和凭证 |
| 缓存与连接池 | 同一外部表跨库访问可能重复缓存和建连接 | 授权与身份兼容时可跨库复用，池和预算容易统一 |
| 发布排队 | 本库同表串行；跨库由 catalog 解决冲突 | 可以对实例内同一物理表统一排队 |
| 外部并发正确性 | 仍需 catalog 条件提交 | 同样需要，单进程不能约束 Spark/Flink |
| 进程与基础内存 | 启用 D 个库约有 D 个服务加 1 个 launcher | 基础进程数少；若需跨库解析连接，仍有额外 backend 成本 |
| 排队与 CPU | 各库独立调度；需要实例总预算避免资源相乘 | 需要租户公平性；有界线程池可并发，不能简单说只用一个 CPU |
| 扩展升级和删库 | 服务生命周期可随本库安装状态处理 | 共享服务要单独隔离各库协议、身份失效与未决工作 |
| zero-copy | 可在同库客户端之间共享不可变 buffers | 可做跨库共享，但还需授权与完整缓存键；进程粒度不自动带来零复制 |

当前本地配置表为 `pgiceberg.catalogs`、`table_bindings` 和 `column_bindings`，均在
各 database 的扩展中创建；`LoadCatalogOptions` 用调用 backend 的 SPI 读取。
[SQL 定义](../../sql/pgiceberg--0.1.0.sql)、[catalog 实现](../../src/common/catalog.cc)
因此按库方案保留本地状态的归属，减少了一个新的跨库配置系统。需要牢记：按库 worker
仍是独立 PG 会话，不能看见用户未提交行，也不能等待正在等它完成 PRE_COMMIT 的用户
事务释放配置锁。

Iceberg table 的实际身份却不属于 PG database：两个库可以注册同一个外部 table UUID。
这正是实例方案的主要优势，也是按库方案的明确成本。两种方案都必须防止仅按本地
catalog 名称或 relation OID 判断外部对象身份。权限不同的请求不能无条件共享全部缓存。

## 6. 由比较得到的设计选择

第一版继续采用按库方案，但遵守以下边界：

1. 实例 launcher 只做发现、启停、退避和 worker/内存准入；普通 RPC 直接连库内服务，
   不让 launcher 成为第二个请求转发点。只启动显式启用的库。
2. 动态 metadata worker 由 launcher 统一决定重启，使用 `BGW_NEVER_RESTART`，避免
   postmaster 自动重启与 launcher 同时创建竞争实例。静态 launcher 自己可由 postmaster
   重启。该单一调度方式可参考 logical launcher 的动态 worker 注册流程。
3. 全局共享注册表保存 database/安装身份、generation 和服务状态；worker 自身启动时
   再确认所有权。launcher 重启后的存活对账不能依赖已经丢失的进程私有句柄。
4. 对启用库总数、所有库的保留内存及 I/O job 设置实例总预算；不能只设每库上限后
   假定实例总占用仍然有界。容量不足要显式拒绝启用或请求。
5. 活跃 read handle、私有写事务、未决发布或恢复工作存在时维持服务。第一版启用库
   保持常驻；若后来加入空闲退出，只能在上述状态全部结清后进行，并定义重新唤醒。
6. 保留独立的 client/protocol 层，用户 backend 不依赖 worker 的部署位置。将来有证据
   证明跨库复用值得做时，可调整服务归属，而不重写 FDW/Table AM 的读写接口。

TimescaleDB 当前 launcher 重启会终止旧 scheduler 再重建；pgiceberg 持有用户未提交
私有状态，不能未经评估照搬这种策略。应优先对账健康 worker；若必须终止，应使对应
用户事务明确失败，并保留发布日志，不能假装只是缓存重载。

选择实例级服务的合理条件是：大量 database 访问相同的外部 catalog/tables；配置与
凭证已经集中且可独立恢复；愿意由服务负责多租户公平性、授权缓存和跨库生命周期。
例如将 pgiceberg 明确定位为整个实例共用的 Iceberg gateway 时，pg_cron 的管理库模式
就是值得借鉴的方向。它是另一种完整的配置与状态归属设计，不只是少启动几个进程。

## 7. 决策验证

实现原型至少比较：单库；多库访问不同表；多库访问同一表；一个热点库与多个轻载库。
同时测冷/热 metadata 的 RPC 等待、catalog 调用次数、重复解析字节数、连接池数量、
真实私有/共享内存占用和发布冲突率。还需验证无用户连接时的恢复，以及扩展升级/删库
对其他库的影响。

这些数据分别回答集中服务是否减少重复工作、按库部署的基础成本是否值得、是否需要
共享不可变 metadata cache。zero-copy 数据缓存另行测量，不能用它替代此次部署粒度
的比较，也不能以这些调度器案例代替 pgiceberg 自己的性能证据。
