# 腾讯文档处理生命周期、原生适配与可审计恢复

上游：WeKnora **v0.7.2**。目标：`mwe-support/weknora-dispatch-mwe` / `weknora-v0.7.2`。基线：`dab6feaee7f62292ddbd2c5b3d9a897ba569e626`。

状态：隔离环境实现、全量回归、实际 HTTP/MCP/页面验收和最终复审已通过，**生产发布尚未执行**。生产放行以本记录后续的发布结果为准。设计依据：[生命周期契约](../design/2026-09-10-tencent-sync-lifecycle.md)、[历史复审](../design/2026-09-10-tencent-sync-review.md)。

## 变更与提交分组

| 分组 | 范围与动机 | 使用及部署影响 |
|---|---|---|
| D1 设计契约 | 保存现状复审、T1–T7 设计、43 项状态模型检查；文档措辞同步说明真实样本的能力边界 | 历史设计结论保留，实际交付状态以本发布记录为准 |
| B1 生命周期执行与存储（T1–T6） | 原子代次、阶段计划、事务 outbox、租约、事件和重试；腾讯原生分页、导出不确定结果、加密产物；版本化索引、FAQ、图谱、Wiki、回滚及精确回收 | PostgreSQL 新增 080–089、SQLite 新增 003–012 迁移；`WEKNORA_PROCESSING_SOURCES` 按来源切换；旧知识保留兼容检索。后台写入、读取过滤与共享投影必须协调升级 |
| U1 生命周期页面（T7） | 六类历史视图、冻结结果分页、阶段详情、预算与截止时间；带版本及幂等标识的恢复操作；文档预览接入已校验产物 | 工作空间管理员操作；普通成员只读受授权 KB；预览及下载保持原文件权限边界。失去响应后重试复用同一操作标识 |
| O1 观测与只读权限（T7） | 同一 SQL 生成器提供应用和 Grafana 投影；首页统计、新详细看板、低基数指标、受限 observer 视图 | Grafana 完整列表关闭自动刷新并前端分页；超过 10,000 行明确要求缩小筛选。observer 不再持有原始业务表 SELECT 权限 |
| D2 验收与发布记录 | 记录测试与实测范围、镜像、回滚限制、残余风险及复审 | 本文件与索引文字修正均属于本推送批次 |
| P1 管理权限与工作池 | 来源工作空间管理员校验，模型任务进入模型工作池 | 共享接收方管理员不能修改来源执行 |
| A1 原生附件 | 资源目录身份和 PDF 持久导出 | 附件正文可完整读取、预览及检索 |
| A2 空内容策略 | 空白 DOCX 和首次为空策略 | 现有发布版不会被新空内容替换 |
| C1 配置与产物复用 | 真实阶段配置摘要、跨代确认产物、租户解析设置锁；模型变化只失效依赖阶段 | 原模型身份未证明时重新计算已核验快照，不盲目复制向量 |
| R1 旧数据恢复 | H13 原记录证明/接管、H14 明确清单恢复、原索引目标核验及回收 | 需要当前成员范围、可信旧产物和人工排空记录；不重放整个旧任务 |
| H1 历史保护与调度 | 启动、兼容写入、保留期统一保护原记录；历史 running 与实际执行区分 | 原 running 仍可审计，但已证明排空后不再永久阻断新调度 |
| S1 设置保存 | 空 cron、false 删除设置持久化；过期设置快照和其他副本旧 cron 拒绝执行 | 关闭计划不会被运行状态或并发改名恢复；冲突需重读设置 |
| S2 MinIO 流式产物 | 无知识 ID 的 SaveFile 使用既有 exports 子目录，避免 MinIO 拒绝双斜线对象键 | 仅调整匿名上传的物理路径；外层仍不绑定虚构知识，保留租约、配额、资源登记和退役约束 |
| U2 恢复证据展示 | 原错误序号/摘要、独立恢复记录、关联作业链接、受限 observer 投影 | 保留当时结果并显示后续事实，正文与凭据不进入发布记录 |

B1 是同一执行协议的协调变更：仅更新其中的写入端、读取端或清理端会破坏版本隔离，因此作为一个功能提交。对外 WeKnora MCP Server 本批没有源码变更，不能将本批提交推入 MCP 专属仓库，也不向 Tencent 官方仓库推送。

## 根因与最终行为

原流程把扫描等待、异步解析计数、补偿队列和 spans 分别当作状态依据；后处理失败会触发文件级重做，迟到回调与清理缺少完整代次约束。新流程由数据库记录扫描、文件、阶段和尝试的事实。重试只恢复失败单元；current 与 published 独立，候选失败保留旧可用版；完成要求所有已启用阶段及旧版退役通过。

源读取覆盖 DOC、Sheet、SmartCanvas、SmartSheet 的已验证契约。完整性未知、缺页、Unsupported、版本混合和超限进入明确阻塞；导出开始结果不确定时保留意图，禁止自动重发。48 张图片逐项保存及处理，不用旧的 30 张数量上限截断整份文档。

外部索引和图谱写入按版本、尝试或贡献隔离，检索只接受已确认 manifest。Wiki 页面更新使用 revision 与 publication epoch 校验；FAQ 手动编辑在索引确认前保留已发布答案及媒体。候选文件、临时产物与索引估算预占配额，确认清理后只释放一次。取消、删除、回滚 pin、配置变化、迟到写入和回收均保留对应保护。

真实页面验收额外发现并修复：新协议没有旧式 FilePath，导致文件预览失败。预览和下载现从已发布版本的确认产物读取并校验加密身份与 digest；读取后再次检查 publication epoch，拒绝读取中已撤回的版本。Markdown 图片复用已有受保护文件代理；DOCX 返回原始导出字节。

其他验收修正：非法来源选择器也留下可操作的阻塞账本；腾讯 DOC 导出的加粗 FAQ 表头可识别而不修改问答正文；保留供回滚的文档删除返回 HTTP 409；已取消或取代的扫描产物按同一七天回收门槛处理。

## 验收环境与证据

所有写入、故障与回滚测试针对独立数据库、Redis、文件卷和测试账号。生产业务数据库及存储未复制到测试环境。真实腾讯文档样本均为本次建立的合成验收资料，限定在测试来源范围；模型调用使用已运行的模型服务，未重启或修改生产模型服务。

工作区证据目录：`results/lifecycle-implementation-20260910/`。编号日志保留每次重要失败与修复结果；其 JSON 记录测试源码归档 SHA-256。测试归档明确排除了无关的 `internal/datasource/tmp_execute_datasource_recovery_test.go`。

| 设计验收组 | 已执行验证与证据 |
|---|---|
| 1 提交与 ACK 边界 | 实际子进程在状态提交后投递前、Redis 投递后 receipt 前、业务完成后 ACK 前被强制终止；重新投递后 effect=1、completion=1、attempt=1。`188-process-crash-green.log`；早期 `03`、`05`、`06` 验证持久恢复与真实 Redis 丢 ACK |
| 2 并发及旧回调 | PostgreSQL 并发首代创建、publication、lease、重复确认；旧协议各工作池回调无法进入新协议对象。`03`、`17`、`56`、`57`、`59`、`66`、`153`、`155` |
| 3 观测与活动执行 | 真实 Redis 预算、依赖错误、过期 lease、队列查询失败与旧计数器隔离；实际应用升级中失联的两个图片阶段由 lease 接管完成。`06`、`11`、`55`、`57`、`59`，模型验收日志 |
| 4 表格完整性 | Sheet 坐标、占位、0/false、空范围及多子表；SmartSheet 字段/记录分页、计算值、重名字段、附件和总数变化。`40`、`41`、`42`、`47`，`native-live-result.json` |
| 5 DOC / MDX | 真 DOC 导出及 DocReader：长段落、超过 150 结构节点、表格、代码文本、脚注、图片；真实 MDX 续页与 48 图。多顶层页及 Unsupported 为独立契约测试。`30`、`36`、`37`、`38`、`42` |
| 6 导出不确定性 | 外部意图、同一 task 续查、一次性完成地址、地址过期、源版本变化及人工核实出口。`29`、`30-export-oneshot`、`35`、`157` |
| 7 阶段故障与复用 | embedding 批次、图片、摘要、问题、Wiki、发布与回收故障；真实模型鉴权恢复后仅重试失败阶段，原有 56 / 14 / 7 个成功输出未变化。`28`、`33`、`51`、`61`、`96`、`98`、`108`、`133`、`140`、`143`，`app-api-enrichment-verify.log` |
| 8 限额与配额 | 主体、limit+1、原生累计、媒体、展开大小、流式加密、并发配额及不确定写入；索引估算与重复释放保护。`76`–`83`、`173`、`174` |
| 9 调度和历史口径 | 持久 publish 单元、丢投递自动补送、真实 run 当时结论冻结、当前恢复另行显示；死信按精确尝试呈现。`17`、`55`、`159`–`162`。无法映射身份的旧历史保留为未核实，未按标题伪造消解 |
| 10 取消、版本及共享内容 | 来源暂停/恢复、范围、KB 删除、回滚 epoch、产物验证、pin 竞争、精确物理回收及迟到图谱/index 清理；FAQ 手动并发保留新答案，Wiki 保留手动内容和其他来源。`58`、`65`–`75`、`86`–`112`、`114`–`155`、`168`、`174`、`186` |
| 11 视图、分页与 RBAC | 六视图使用同一投影；SQLite 与实际 PostgreSQL 冻结数据/计数、10,001 行上限、撤权、跨租户/调用者隔离。真实 HTTP 17 页不漏不重；普通成员及 KB scoped key 禁止全局越权。实际 observer 新登录无法 SELECT 原表。`159`–`166`，`app-api-history-rbac.log`，Grafana 截图 |
| 12 实际检索及媒体 | 真实 API 与独立 WeKnora MCP 检索正文尾标记、FAQ、问题/摘要索引；48 OCR、48 caption、真实 embedding、Neo4j、9 Wiki 页面、8 链接阶段。49 张原图接口字节 SHA 校验；浏览器 48/48 图片实际加载；Wiki 引用回源 DOCX 正常。`app-api-mcp.log`、`app-api-media.log`、`app-api-enrichment-search.log`、`app-api-preview.log`、`app-live-media-preview.json` 与截图 |

整体验证命令与结果：

```text
python tmp/lifecycle-20260910/run-tests.py ./... -count=1 --postgres --neo4j
  PASS — 187-full-go-green.log
  source SHA-256: 539dbcdbdc36817d88774dfd887162032ab05a87eca4ebf8e5a978b73a68e188
python tmp/lifecycle-20260910/run-tests.py ./internal/application/service -run '^TestProcessingProcessCrashRecovery$' -count=1 -v --postgres
  PASS — 三个实际进程终止边界
npm test
  PASS — 361 tests（阶段 UI 修改后的完整测试）
npx vue-tsc --noEmit
  PASS — 包含最新预览接入
npm run build
  PASS — 包含最新预览接入，dist 747 files / 2 HTML / 594 JS / 64 CSS
```

后续测试若只新增验收代码，不改变运行时源码，应单独记录，不把旧运行时镜像误记为包含新测试源码。Vite 仍提示现有大 vendor chunk，不属于构建失败。外部服务接口会随版本和权限变化，测试成功不扩大到未验证的内容类型。

## 镜像与生产放行

隔离运行基线 app：`local/weknora-app:lifecycle-34074cd9bb92bf19`；二进制 SHA-256 `8d3a7e4ccd6d9ed3e917db1ddc8963282defb250cea81919f302af1c0c54cf1d`。最终设置并发修正的替换镜像另在本记录末尾登记，不能把此基线当作最终镜像。隔离前端 bundle：`ui-observer-e69048bad17871ad`。隔离 MCP 沿用 `local/weknora-mcp-dispatch:v0.7.2-mwe-1b16874`，通过真实 HTTP MCP 初始化、受限短期 API key 和检索验收；测试结束即撤销该 key。

生产发布标识、Git 提交、镜像 digest、迁移结果与 smoke 结果：**待完成最终复审后填写**。尚未启动生产重同步、迁移或新来源调度，也未修改原本暂停的自动化。

放行顺序：确认复审问题关闭 → 固定源码与镜像 → 核实生产主机/Compose/worker、保存受控回滚材料 → 升级所有理解新协议的执行者并检查迁移 → 对明确范围启用 → 验证 published 检索及观测 → 再决定扩大范围。切换前要核实旧任务已排空或取消生效；不能仅凭停止 enqueue 推断在途执行已结束。

## 回滚与残余边界

新账本已产生数据后，回滚必须使用理解新协议、fence、published 与索引 manifest 的兼容镜像。**不能直接回退旧的 2026-09-07 二进制并让它消费新任务**。按来源停用新调度，等待或撤销 lease；保留账本、已发布产物和索引，使用受控兼容版本处理。降级 SQL 会丢失新审计语义，不能作为普通故障回退动作。

文档回滚需要目标已 pin、完整产物核验和新 publication epoch；正在 deleting 或已删除的版本不能直接切指针。实际隔离环境完成 V1→V2→V1 发布/检索及后续物理退役验收。

- SmartCanvas Unsupported 代码块没有可靠补读契约时明确阻塞；真实多次续页样本中的字面 `<Page>` 不冒充多个原生顶层页。
- before/after 修订核对不是腾讯侧 MVCC；不稳定或权限不足的源保持阻塞。
- 跨 PostgreSQL、模型、腾讯导出和外部索引不承诺全局 exactly-once；承诺版本隔离、幂等确认和可核查的补偿。
- 历史未归因记录保留兼容可见性，不自动推断旧 job 或清除不确定导出。取代后明确取消的义务记录取消消解，不伪称后继成功。
- 事件目前保守保留；设计中的 90/365 天是可选初始归档策略，未执行删除。受引用、pin、不确定操作保护的产物不按普通 TTL 删除。
- 阶段详情一次读取该作业完整步骤，页面每页显示 50 条；未新增远端步骤分页 API。完整历史查询超过 10,000 行需缩小筛选。
- `charged_bytes` 包含索引估算，页面另列估算字节，不能当作物理磁盘实测值。
- PostgreSQL/Neo4j 已完成处理链验收；Qdrant 已补真实索引读取、写入完成确认、重放、精确删除及旧候选接管执行器验收。旧候选执行器测试使用合成旧记录和可计数模型替身，数据库、Qdrant 和文件读写为真实执行；另外完成独立应用的 H13/H14 HTTP、MCP 与页面验证，见下文。其他索引后端未声称逐个实测。

## 独立复审与发布结果

两名只读审查者分别检查代码规范和设计契约，固定基线为 `57945a8bfc05b3c5e89f304d445a9d17260fcf62`，后续按不可变源码归档增量复核。下表是已执行修正；最终设置并发修正的回归与复核结果见末节：

| 问题 | 修正与验证状态 |
|---|---|
| 共享接收方管理员可写来源工作空间 | 管理动作要求来源工作空间管理员；实际注册路由验证共享读取仍允许、七种写入均拒绝。`190-review-rbac-pools-green` |
| embedding 进入轻量工作池 | 两种 embedding 单元改用既有模型工作池；发布仍用轻量池。`190-review-rbac-pools-green` |
| 附件元信息不支持导致入口阻塞 | 记录并新鲜核实来源目录身份，复用持久导出链路。真实两页 PDF 经 API/MCP 检索和浏览器预览；预览/下载 SHA-256 与原件一致。`194-resource-green`、`app-api-resource-verify.log`、`app-live-resource-pdf-preview.png` |
| 缺少按阶段配置失效及跨代复用 | 独立阶段指纹，原身份校验后复制确认产物；重新生成当前版本的块和索引身份。引用在复制提交后留下审计事件并释放，防止版本链长期占用。修改摘要/embedding 模型的实际 PostgreSQL 执行器计数、问题/图谱和 FAQ 回归通过。`196-cross-generation-reuse-green`、`197-reuse-pipeline-faq-green` |
| 缺少验证为空的策略 | 首次验证为空跳过；已有发布版本时新空源阻塞并保留旧版。真实空白 DOC/MDX 同轮成功、均跳过且未发布；DOCX 完整包严格为空时避开解析器的空文件错误。真实 MDX 从空白→有内容发布→再次空白后，新版本 SOURCE_EMPTY，旧内容仍可检索。原失败轮次结论保持不变，恢复结果另有证据。`201-empty-docx-green`、`app-api-empty-retry-verified.log`、`app-api-empty-verify.log`、`app-api-empty-retained-verify.log` |
| H13/H14 的可信旧数据桥接缺失 | 当前成员范围与模型/旧索引目标边界已补齐。H13 实际应用登记 4 条完成证据并接管 1 条候选；H14 实际恢复限定 8 项（1 项发布、7 项验证为空跳过），原 run 与死信完整摘要不变。六视图显示原错误及独立恢复证据；最终设置并发复审见后续记录 |

补充全量检查：`198-full-go-green.log` 在真实 PostgreSQL/Neo4j 测试环境执行 `go test ./... -count=1` 全部通过，源码 SHA-256 `5ca4fde8b683872617989248bf9be7d6a4a280e0ffa09750ce95dd07cf215959`；其后无版本 DOC 修正通过 `199` 针对性检查。当前仍需完成上述迁移、最终复审、固定发布产物及生产验收。

H13/H14 准备中新增两项发现：

- 只读复核生产原记录，待发布候选已被旧 housekeeping 标为 failed，仍保留 index_ready、完成的核心阶段和 337 个可核验索引；4 份 completed 也保留原版本和分块。核验遵循原父子分块规则及索引内容的标题前缀，不能把未参与 embedding 的正文节点误报为缺失点。旧图片索引的 enabled=false 单独记录，尚未修改生产数据。
- 新增限定知识与 SourceID 的旧索引读取能力，仅支持已验证的 PostgreSQL/Qdrant，缺项、内容变化、重复身份、维度变化或非法向量均拒绝。真实 Qdrant 首次测试复现写入返回后立即读取为空；Save/BatchSave 及删除改为等待应用完成，新协议与 staged FAQ 使用确定的 point ID，重放不重复新增。`203-qdrant-ack-visibility-red` → `204-qdrant-wait-replay-green`；后者源码 SHA-256 `b344a3b832003e8e94b944f7721132c9e55f3e7b4fd4b53e4bda495c657ee82b`，实际 PostgreSQL/Qdrant 检查通过。这些是迁移所需的底层能力，尚不等于旧记录已迁移或已消解。

H13 的实现与验收补充（生产原记录尚未改写）：

- `processing_legacy_evidence` 按原轮次、错误序号及原错误完整摘要登记；数据库触发器禁止改写和删除。`late_completion` 要求原记录已具备精确知识、版本和处理尝试；缺失时只能明确记为 `manual_confirmed`，且仍须完整产物验证。顶层 done、零计数、后来的时间戳和成功批次均不能替代证据。传统图谱/Wiki 缺少实际贡献证明时保持不可核实。
- 源文件读取核对完整 MD5/长度并计算 SHA-256；正文、父子块、图片处理、摘要及问题按旧计划核对，并读取实际向量。资源引用核对租户；旧 MinIO/local 文件要求明确的知识或 exports 路径和相应快照引用。未知布局不自动读取。
- 候选接管要求来源已启用新流程及限时的人工 worker/queue 核验记录。原实例退出、新实例保护版本、各队列库存和具体投递状态均须记录；`active=0` 不构成证明。该记录属于操作人声明，不冒充应用自行验证了宿主机。接管事务一并持久化知识所有权、旧资源绑定、原索引目标、处理计划和 outbox，原文件的配额基线保留，旧文本索引配额转入账本并在确认删除后释放。
- 新计划从 `legacy_snapshot` 开始；旧向量只有在模型身份、维度、原索引目标及内容全部证明匹配时才复用，否则从已核验旧快照计算当前向量。不会再次导出、读取、解析或 OCR，也不会把旧 parse/embedding 补写成成功。新索引发布后精确清理原目标中的 SourceID，当前要求的摘要/问题等投影完成后另追加 `recovered`。旧同步日志和旧 span 原样保留。已核验图片块的新索引遵循块的启用状态。
- 通用全量保存、列更新和批量保存补齐接管写入检查；另修正了死信回调使用独立 context 导致的竞态。真实回调测试证明旧检查通过后即使发生接管，也无法把新状态改回 failed 或关闭旧 span。
- 原失败看板改用显式证据，先消解再选最新未解决项；无稳定标识的错误以原轮次/序号独立展示。受限 observer 先匹配原错误摘要再投影到脱敏摘要，不能因两条错误脱敏后相同而错误复用证据。`legacy-query-tests.log` 的实际 PostgreSQL TEMP 表测试及受限账号查询通过。
- 新增 KB 来源管理员管理接口：`processing/legacy/sources/:source_id` 下的范围读取、原错误快照、worker 核验、完成登记和候选接管。实际注册路由测试拒绝普通读取角色、共享接收方管理员和仅检索 API Key；输出不含原错误正文或凭据。
- `214-legacy-adopt-real-qdrant-green`、`216-legacy-early-delete-and-accounting-green`、`217-legacy-boundaries-green` 已通过；217 源码 SHA-256 为 `115eb649700172ade703598dacde90624ab46776db4f7ada44b1432ffd9804f9`。执行命令为隔离 PostgreSQL/Qdrant 环境中的 `go test ./internal/application/service ./internal/application/repository ./internal/router ./internal/application/service/retriever -run 'TestProcessingLegacy|TestProcessingPostgresConcurrent|TestProcessingHistoryRoutes|TestProcessingPlanDeliveryLease' -count=1`。
- 扩展带图片的 Wiki 场景复现 `WIKI_SOURCE_CHANGED`：原查询只返回正文块，却被用来校验 OCR/描述块。已改为按完整计划分块 ID 分批读取并核对知识、KB、内容和修订号；`219-wiki-image-source-coverage-red` 为失败证据。`221-legacy-graph-wiki-pipeline-green` 在实际 PostgreSQL/Qdrant 下通过 3 个旧候选场景（完成、执行前删除、带图谱/Wiki）及原有 14 个处理链场景；源码 SHA-256 `772fa3fea6c26378755b756981c0f61220fe03172af73b60712581b866d17c50`。模型和该组合场景的图谱后端使用可计数替身，Wiki 实际写入并核对页面及来源引用。

部署时必须依次应用 088/011 和 089/012，再更新 observer 投影和看板。089/012 仅替换视图，保留既有冻结分页快照；其降级只恢复前版视图。回滚应用或取消接管不等同于撤销知识所有权；已接管行继续由新流程保护，保留原文件及旧错误记录，按明确的版本操作处理。不得让旧二进制重新写入已接管知识，也不得在仍有新流程数据时回滚掉证据表。

H14 与旧历史补充：

- `POST processing/legacy/sources/:source_id/runs/:run_id/retry` 接受最多 100 条明确的原错误身份、原死信编号、队列 task ID、语义载荷摘要及有效 drain。读取归档的原 payload 校验租户、来源和原 run；逐项确认清单中错误仍为 scheduled、原摘要未变、原选择器仍有效，拒绝导出不确定状态。该接口不会重放原任务。
- 新 scan、每条 `retry_requested` 意图及初次 outbox 原子提交。既有分页枚举继续验证成员范围，只对清单内文件建立文档作业；实际准入追加 `retry_admitted` 并关联新文档作业。最终 coverage 单元检查全部请求是否准入；缺项明确阻塞。新文档完成并发布才追加 `recovered`；完整核验的新空源追加独立的 `policy_skipped`，不冒称发布成功。原 running 日志、scheduled 字段和 dead letter 不改写。
- `222-legacy-retry-revision-red` 复现追加事件时使用过期 revision 导致事务回滚；修正后 `223-legacy-eight-retry-green` 通过 SQLite 和 PostgreSQL 的 8 文件准入、全事务回滚、重复请求、错误关联、清单外文件以及缺项覆盖测试，源码 SHA-256 `d121e80637f255993065b41f17f2a74be1b92cd1f2f7baa83e7c7452dde416c9`。`224-legacy-retry-filters-green` 验证保留目录和下一页的同时只生成清单内文件步骤，源码 SHA-256 `8318fd94701fa28e9f992706962ad81a06cfd8739f4eca6f2cee113c4907fa6f`。
- 原始错误、恢复证据和原 run 使用独立行 ID；legacy 行的 job ID 始终为空，恢复作业通过 `linked_job_id` 单独关联。视图提供原错误序号/摘要、记录人、恢复动作和原死信编号。事件不以标题合并，不按后来的时间戳消解。受限 observer 无权读取包含原始错误的辅助视图。
- SQLite 使用已安装驱动的 ConnectHook 为每条连接注册标准 SHA-256 函数，应用连接和迁移连接统一使用该驱动。`225-legacy-history-dual-db-green` 验证两数据库的同名错误独立、原内容变化令证据失效、分页冻结、旧 running 与 dead letter 并存，以及两条 SQLite 连接上的摘要一致性；源码 SHA-256 `6a5dddc07ad029f59eace6b120e82222bee6e07360492c6fa8ab241e6ff6313e`。
- 本轮前端 `npm run type-check`、`npm run build` 及分页/i18n 12 项检查通过，dist 完整性为 828 文件 / 2 HTML / 672 JS / 67 CSS。该构建结果尚未替换运行服务，不代表新页面的浏览器验收已完成。

配置边界补充：租户实际解析器覆盖配置进入整代配置摘要及 parse 阶段指纹。PostgreSQL 在处理事务中取配置共享 advisory 锁，设置写入在更新租户行前取独占锁；配额单列更新不持有该配置锁，避免在租户行上 SHARE→UPDATE 升级造成死锁。配置变动后旧 lease 的 heartbeat/提交被拒绝，下一代采用新配置。Wiki 候选提取另按正文、语言、提取提示词和 chat 配置确定输入，embedding 变化不重新调用这一纯提取步骤；共享概念匹配及后继仍按实际依赖执行。此补充的扩大回归与最终运行服务验收见后续记录。

## 最终复审修正与运行服务复验（2026-09-11）

`232`、`233`、`237`、`238` 补齐来源成员校验、模型变动、旧索引目标与发布工作池边界。旧 Qdrant 目标变化时保存原目标证明并精确回收；无证明则拒绝删除。真实双目标测试发现 JSON 对象浅拷贝导致 A/B 地址共用指针，`236-migrated-pointer-alias-red` 保留原失败；改为重新解码后，`243` 使用四条数据库连接验证真实源行锁下的并发行为。keyword-only 接管复制原 keyword 索引，不要求不存在的向量；模型类阶段仍进入对应模型工作池。

实际应用启动还发现旧 reset 会改写超过 30 分钟的 running 原记录。`238a-live-startup-history-red.log` 和诊断记录保留了被改写的合成旧记录，未还原数据来伪造“从未改变”。修正后，启动 reset、旧 Update/UpdateResult、来源取消和历史清理共用原记录保护；旧写入口与接管使用同一来源锁。`BeginScan` 不能把含原错误结果的历史 run 认作空白新 run。取消记录含错误、失败计数或错误消息时不做普通 TTL 删除。

保留的旧 running 由不可变排空记录区分是否仍有有效旧执行；排空证明仅覆盖它之前的旧记录，新投递和新账本活动仍阻止重叠调度。聚合 job 已 blocked/failed 时，如果其他非退役阶段仍排队或运行，仍视作活动执行。查询失败时不创建新 run。`240`、`241`、`243` 实际 PostgreSQL 与对应单元检查通过。

真实定时复验复现 GORM 跳过空值，`243a-live-cron-empty-setting-red.log` 保留失败；当时只清除本次测试来源的精确临时计划。现在创建显式 false、更新空 cron/false 均在来源锁内保存；设置读取携带服务端指纹，持锁时拒绝被并发暂停、修改计划或凭据取代的旧快照，运行游标变化不引发该冲突。旧副本的 cron 回调同时核对持久计划。两实例测试证明旧回调不再创建 run，当前计划仍可派发。`247-schedule-settings-concurrency-green.log` 四包通过，源码 SHA-256 `27d0d506690d8466b34c9785d7f8562286fafbcdb2bb8c2674bc817fb4817e36`。两位审查者对该不可变快照的剩余 P1/P2 均为 0；审查是静态复核，实测由主执行流程单独记录。

测试环境的 088/089 曾在开发中提前应用。只对隔离数据库对齐尚未发布的 `policy_skipped` 约束及十个视图，保留原历史与分页快照；生产仍为旧迁移版本，不需要这些测试对齐操作。`244`、`245`、`246` 是测试建表缺失或重复的 fixture 失败，已分别修复，不记为运行时故障。旧取消接口第一次验收错误地预期 409，实际契约是 400 并要求使用处理历史控件；修正断言后验证知识与账本完全未变，保留 `248a-cancel-alias-expectation-red.log`。

| 实际服务场景 | 验证结果与证据 |
|---|---|
| H13 完成证明与候选接管 | 4 条精确完成证据；1 个候选已发布且原索引已清除；仅 6 个接管阶段，无重新读取、导出、解析、OCR。原同步行完整摘要不变。`app-api-legacy-live-verify.log` |
| H14 明确 8 项恢复 | 8 个独立文档作业；1 个实际发布，7 个完整验证为空后策略跳过；原 8 条错误消解，原 running 与死信完整摘要不变，一致性视图仍保留旧 running/死信异常。`app-api-legacy-retry-verify.log` |
| 腾讯来源状态恢复 | 两个新建空 MDX 在腾讯侧为 hidden_until_modify，正常阻塞；仅对本次合成文档插入并删除一个验收段落，确认 normal 且空后，只重试两项 metadata 阶段；12 个成功步骤的 attempt/digest 未变。provider 状态与操作 trace 单独保存 |
| 实际重启 | 新镜像启动后，两条已超过 30 分钟的原 running 行完整摘要仍与创建时一致。`app-api-legacy-restart-verify.log` |
| 定时同步与关闭 | 原 running 保留时创建一个新的 scheduled run；API 清空临时计划后，数据库确实为空，原记录仍不变。`app-api-legacy-retry-cron.log` |
| 旧取消路径 | HTTP 400 明确提示使用新控件；知识行和作业详情完全未变。`app-api-legacy-cancel-alias.log` |
| MCP | 实际 12 个只读工具，7 个限定测试 KB 的正文、尾标记、媒体/共享投影及 H13/H14 内容可检索；临时只读 key 最终撤销。`app-api-mcp.log` |
| 页面与监控 | H14 0/0 未解决项、32/32 独立历史事件、关联恢复任务可打开；Grafana 六视图读取新 observer 投影，原 running 与成功新轮次并存。页面 PNG/DOM 均保存并实际查看。`app-live-h14-*`、`app-live-grafana-h14.*` |

最终设置修正的已运行隔离镜像为 `local/weknora-app:lifecycle-27d0d506690d8466`，二进制 SHA-256 `3109f9500928921b00c88124209af19571c5dee6f65bc41724cb5ca3a0c81bcb`，镜像 manifest list `sha256:1d6d217e4df668fb8db5ba9ed17e62e247834aa20950f7982e0bcfebb4e11985`。前端最终 type-check/build 通过（880 文件 / 2 HTML / 722 JS / 69 CSS；多次构建保留旧静态资源，因此文件总数不代表新增代码量）。测试监控重新配置后 frontend、Grafana、Alloy、Prometheus 均返回 200，未修改生产监控。

生产只读预检于 2026-09-11 11:25（Asia/Shanghai）确认：9 个队列的 active/pending/scheduled/retry 均为 0，独立 outbox 为空，MinerU 无排队/执行；2 条旧 running 仍保留，不能当作当前 worker 存活证明，也不能删除来获得“全绿”。归档任务仍保留（default 334、summary 31、sync 23）。这里只记录预检，不代表已部署或已恢复生产历史记录。

最终全量检查 `248-full-go-final-green.log`：隔离环境执行 `python tmp/lifecycle-20260910/run-tests.py --postgres --neo4j --qdrant ./... -count=1` 全部通过，源码 SHA-256 `84874a708e7ad124555c2f6c521bf4699b4fe30eeab6103d5091753a275944be`；repository 64.859 秒、service 394.279 秒。该归档与已运行 247 镜像的运行时代码由发布准备阶段逐文件核对，文档变化另计。提交按已通过验证的功能检查点整理为 P1、A1、C1、A2、R1、H1、S1，再提交页面/观测和最终记录；整理不改写当前工作区，不包含无关恢复测试。

## 待切换发布产物

已逐文件核对 1,796 个运行时源码文件，247 运行镜像与 248 全量回归一致。发布整理只规范生成 SQL 的结尾空行；生成器 `--check` 及监控查询生成一致性检查通过，SQL 语义不变。生产前端镜像另在隔离容器启动，HTML 与已验收 bundle 字节一致，nginx 配置和真实 API 代理均通过；该临时容器随后移除。

| 产物 | 标识 |
|---|---|
| App | `marvel/weknora-app:v0.7.2-processing-lifecycle-20260911`；镜像 ID `sha256:1d6d217e4df668fb8db5ba9ed17e62e247834aa20950f7982e0bcfebb4e11985` |
| 前端 | `marvel/weknora-ui:v0.7.2-processing-lifecycle-20260911`；镜像 ID `sha256:581426bfbbc1b6b781d4bcb4aafe48812eb684273836998bc2668717c0bd221b` |
| 前端入口 | SHA-256 `114540947c925887abf148d77d7637affe0ccba4ee77b762a2b7fe5c84dbf583` |
| P1 / A1 / C1 / A2 | `3d85afb9` / `0ab1e3b3` / `10767eff` / `e97bd6d1` |
| R1 / H1 / S1 | `406891a6` / `07c924a8` / `4894ed6c` |
| U2 / O2 | `64370652` / `1b930144` |

11:38 的更新预检确认迁移为 79、dirty=false，数据库约 809 MB；6 个 Asynq server 注册均属于同一当前 app 容器，没有第二个同库应用执行者。切换时必须停妥该旧容器并重核队列与注册，保留两条原 running 及全部归档投递，不以删除历史获得排空证明。新协议按现有腾讯来源范围启用，不更改来源选择器、同步日程、模型或凭据；此次部署不自动接管生产旧候选，也不重放原 8 项 scheduled 清单。

## 发布前 MinIO 根因修正（S2）

生产使用 MinIO，先前实际应用验收使用本地文件存储。补充的隔离 MinIO + PostgreSQL + Qdrant 验收发现：`ProcessingArtifacts.saveStream` 以空知识 ID 调用 `SaveFile`，原 MinIO 驱动产生 `1//文件名.enc`，存储端实际返回 `Object name contains unsupported characters`。`249`、`250` 保存阶段阻塞，`251-minio-root-cause-red.log` 用直接加密产物上传确认根因。开始前删除场景在修正前已通过，不能以此替代写入验证。

修复在 MinIO 驱动内把空 ID 对应的目录设为已有 `exports` 命名空间；外层资源包装器的参数仍为空，不会建立虚构知识绑定。常规知识上传路径不变。其他存储驱动没有本批源码变更。本节与最终发布状态修订均归 S2 推送批次。

`252-minio-green.log`：真实 MinIO 加密保存/解密读回、完整旧候选接管、丢索引确认重试、原文件预览、旧 Qdrant 索引回收、开始前删除及配额释放全部通过；原始运行记录保持不变。每次使用独立随机 bucket，并在测试结束清理。命令：

```text
python tmp/lifecycle-20260910/run-tests.py --postgres --qdrant --minio ./internal/application/service ./internal/application/service/file -run '^TestProcessing(LegacyAdoptionWithMinIO|Artifacts)' -count=1 -v
PASS — source SHA-256 392586ed50800c9f5ae5a5463408b95d2a00a581eb482daf69d2887474e5e215
```

新隔离应用镜像 `local/weknora-app:lifecycle-392586ed50800c9f` 的二进制 SHA-256 为 `4adde29cdb830a92545788264954247026ab0a80893f8c25bf98eeada1b5431a`，镜像 ID 为 `sha256:1a0c73fb3b6c8af31c603f462f200eb0924ae6c844c6af99bf3d0161cae30555`。实际 HTTP 创建专用 MinIO backend 与新测试 KB，通过真实存储解析器完成合成腾讯 PDF 的 15 个文档阶段；所有活动产物均归该 backend。预览与下载原始字节摘要一致，两页及末尾标记均可检索。独立 MCP 再次验证 8 个测试 KB，含该 MinIO 样本；临时只读 key 已撤销。证据：`app-api-minio-verify.log`、`app-api-mcp.log`。应用再次重启后，两条合成旧 running 行仍保持完整摘要不变。

S2 的 MinIO/Qdrant 自动用例采用合成模型和来源成员响应；真实 HTTP 样本覆盖腾讯附件导出及存储链路，未调用模型。真实模型、48 图与 Wiki 等场景的实测边界仍以前文记录为准。此修正使上表首轮 App 候选作废；最终 App 使用独立 `v0.7.2-processing-lifecycle-20260911-minio` 标识，前端保持已验收镜像。新增底层对象目录可由资源中保存的确切路径读回和回收；回滚仍受前述协议版本限制。

最终补充全量命令 `python tmp/lifecycle-20260910/run-tests.py --postgres --qdrant --neo4j --minio ./... -count=1` 全部通过，证据 `253-full-go-minio-green.log`，源码 SHA-256 `abe9d09ca803827a00a8e01ab4c8a7b66affc3d61c37a0d7da7a4e4bd7730c7c`；repository 62.075 秒、service 439.269 秒。发布准备再次核对全部 1,796 个运行时源码文件与实际运行镜像一致，并验证原前端镜像的 nginx、入口字节及对新应用的 API 代理。S2 的 Standards 和 Spec 两项静态补审均为 0 P1/P2。浏览器实际打开 MinIO 中的双页 PDF，`app-live-minio-preview.png` 与 DOM 已保存并查看；未用接口成功替代浏览器渲染结果。
