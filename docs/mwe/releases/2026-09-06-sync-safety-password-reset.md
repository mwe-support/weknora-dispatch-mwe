# 腾讯文档安全替换、同步恢复与邮箱密码重置

- 上游：WeKnora v0.7.2；仓库：`mwe-support/weknora-dispatch-mwe`；目标分支：`weknora-v0.7.2`。
- 当前基点：`66bd6afd8bd6aaa78ce4b04c915167b25a748d45`，包含此前尚未提交的目录保留/FAQ 数据源改动。
- 发布标识：`v0.7.2-sync-safe-reset-20260906`。
- 生产状态：已切换生产，并完成用户授权的单一数据源全量同步验收；结果为 **8 份更新、1 份失败，部分通过**，失败文件三轮自动补偿耗尽后转人工处理。
- 本记录第 1–7 节的生产证据对应部署镜像 `v0.7.2-sync-safe-reset-20260906`。随后提交前复审追加的 P1 修复尚未部署，代码提交清单与最终复审见 [提交前复审与推送记录](2026-09-06-prepush-review-and-publish.md)，不能用本页此前的生产验收替代新增修复的运行验证。
- 本批只有主项目代码；未修改 WeKnora MCP Server。无数据库表结构迁移。

## 1. 文档更新与失败恢复

原实现对同 external_id 先删除旧文档，再保存和异步解析新文档。新版存储失败、任务入队失败或解析失败时，旧文档可能已经不可用；入队失败还可能向同步入口返回成功。另一个缺陷是导出补偿复用旧任务后，将旧快照确认成源端最新修改时间。

当前实现：

- 以数据源、文件身份、修改时间、内容和路径生成候选版本标识，使用现有 knowledge 元数据持久化；进程重启后可复用同一候选。
- 创建候选时保留旧版。候选尚未发布时，从检索主结果、上下文补充、Agent 直读和文档目录排除；复用已有 ExcludeKnowledgeIDs 并补齐 PostgreSQL/SQLite 驱动支持，避免候选占据召回名额。
- 必要解析、索引和多模态处理结束后，原 post-process 任务只记录 readiness，不将未发布内容融合进共享 Wiki/图谱。同步处理器验证范围后发布候选，再重新入队现有 post-process；最终完成后才清理旧版。
- 先发布可用新版，再清理旧版。清理失败时保留新版并允许重入，避免两版都不可用；可能短暂保留两个已可用版本。
- 存储、入队、处理等失败不确认该文件成功游标。可补偿的入库故障复用现有按文件最多 3 次、约 2/4/8 分钟加随机延迟的持久化补偿；明确配置、配额和格式拒绝不盲目循环。
- 原处理器的 completed 可能包含子任务终失败。本批在终失败任务及 Wiki 重试耗尽时先持久化源版本失败标记，再释放完成计数；该标记阻止清理旧版和确认同步。重新处理候选时重置标记。readiness 使用条件元数据更新，重复旧任务不能把已发布版本重新隐藏。
- 慢摘要等普通全行保存不能覆盖生命周期标记：数据源版本保存时在事务内锁行读取最新标记；只有明确发布/重解析路径修改这些标记。回归覆盖“读取旧对象 → 另一任务记录失败 → 保存旧对象”，失败证据仍保留。
- 导出任务记录所属源修改时间，版本改变时不复用旧任务去确认新版本。
- HTTP 429、502/503/504、连接重置/拒绝、超时、EOF 等暂时性读取故障采用有界重试；创建导出结果不确定仍保守处理。`11607` 明确归为 `INVALID_REQUEST`，不当成普通网络错误无限重试。
- 按用户后续要求，MCP 调用内失败退避改为约 2/4/8/16 分钟（加 0–25% 抖动）。429/503 的共享冷却至少两分钟，更长的 Retry-After 仍优先；腾讯文档外层 Asynq 失败重试也至少两分钟，避免绕回默认秒级退避。正常请求节流与正常导出进度轮询不属于失败重试。
- 为容纳分钟退避，导出查询预算为 45 分钟，文件补偿任务预算与普通同步一致为两小时；单个网络请求仍保留短超时和取消支持。
- 保持原有 **100 MiB** 导出文件限制和有界读取。
- 继续复用手工文件夹上传的路径契约：选取目录时保留该目录及下级目录的真实相对层级，整空间同步保留空间下的目录；不补造所选范围之外的祖先路径。候选入库、补偿重试和目录改名后的更新均保留 folder_path/source_path。
- 检查发布前及清理前的源范围/凭证变化。数据库 checkpoint 使用配置比较，并保留用户暂停状态；旧 worker 不能覆盖新范围游标或撤销暂停。
- 补偿类别、次数和下次执行时间回写同一条错误记录；Grafana 不把未发布候选当作恢复成功证据。

候选完成等待复用 sync worker，每次最多十分钟。超时保留旧版与候选，以文件补偿继续；源同步整体仍受现有任务上限约束。没有新增队列类型、数据库表或依赖。

## 2. FAQ 更新一致性

旧合并逻辑先保存新答案再索引，索引失败可能已经改变正式答案。本次先索引候选值，再保存正式答案；索引故障时旧答案仍保留，下一次恢复后可发布新答案。

审查更正：最初报告推断“同哈希永久跳过修复”，但真实仓库的 FAQ 查询只加载 id/metadata，没有加载 ContentHash，该推断不成立，已更正审查报告。本次修复的是实际存在的发布顺序问题。

数据库与外部索引不构成跨系统事务。索引部分写入后失败，可能暂时保留部分新索引，但正式答案不提前更新，后续重试重新索引。既有 KB 范围标准问合并与源删除不删 FAQ 的行为保留。

## 3. 邮箱验证码密码重置

从隔离开发树移植邮箱验证码功能，保留已有管理员重置入口。登录页可请求六位验证码，十分钟有效、单邮箱一分钟发送冷却、每个验证码最多五次校验。

- 请求接口对已存在/未知邮箱使用相同响应；验证码只保存 bcrypt 哈希。
- Lua 将并发尝试预算绑定到具体验证码版本，旧请求不能影响新码；比较并删除实现一次消费。
- 改密与撤销全部会话在同一数据库事务中完成。数据库结果异常时不恢复已消费验证码，用户可重新申请，避免不确定提交后复用旧码。
- 精确开放两个匿名 POST 路由，继续使用公共认证接口限流；新增真实认证中间件测试，防止“只有单测路由可用、正式未登录请求被挡住”。
- SMTP 支持 465 隐式 TLS 和强制 STARTTLS；校验证书、主机名，支持上下文取消。DATA 被服务器接受后，QUIT 失败不会使已投递验证码失效。
- 腾讯企业邮箱配置为 `smtp.exmail.qq.com:465`。优先读取专用 SMTP 密码变量，支持 `SYSTEM_EMAIL_PASSWORD` 回退；模板见 `deploy/mwe-password-reset/tencent-exmail.override.example.yml`。
- 使用服务器已有 `/public/knowledgebase/secrets/service-secrets.env`，没有把密码复制进仓库、测试产物或发布记录。
- 修复登录页弹窗被变换容器裁切：挂载到 body、居中并限制为视口宽度。补齐韩语/俄语文件重试状态文案。

参考：[腾讯企业邮客户端配置](https://main.qcloudimg.com/raw/document/product/pdf/613_46019_cn.pdf)。

## 4. 验证与证据

隔离目录：`/public/knowledgebase/upgrade-tests/sync-safe-20260906/`。测试只挂载候选源码和公共 Go 缓存，未挂载生产数据库、Redis或业务数据卷；无关 `internal/datasource/tmp_execute_datasource_recovery_test.go` 在隔离副本中排除，原文件未修改、未执行。

```sh
go test ./... -count=1
go test -race ./internal/application/service ./internal/application/repository ./internal/datasource/connector/tencentdocs ./internal/handler ./internal/middleware ./internal/config -run 'TestTencentCandidate|TestTencentFAQIndex|TestTencentRetryDoesNot|TestTencentIngestFailure|TestTencentMCPRead|TestDataSourceCheckpoint|TestPasswordReset' -count=3
npm --prefix frontend run type-check
npm --prefix frontend run test
npm --prefix frontend run build
python scripts/render-observability-failure-queries.py
python scripts/test-observability-failure-queries.py --emit <isolated-fixture.sql>
```

- 全量 Go：68 个有测试的包通过。容器禁网、2 CPU/4 GiB；为依赖公共 DNS 的 SSRF 单测提供固定公共地址映射（记录在 `final-test-environment.json`），不是关闭 SSRF 检查或访问外部服务。最初禁网导致的 DNS 失败和基线复现日志保留。
- 关键并发回归：6 个包、连续 3 轮通过。
- 分钟级重试与路径需求追加回归：腾讯连接器、service、router 三包完整测试及相关用例三轮 race 通过。测试注入等待函数核对 2/4/8/16 分钟及抖动，并检查限流冷却、导出预算和真实 Asynq 任务超时；未在生产制造限流或长时间故障。
- 全量回归之后追加的完成状态/旧快照保护，另行完成相关 6 包完整回归及 service/repository 关键用例 3 轮 race；最终二进制已重新构建。
- 前端：类型检查、360 个测试、生产构建通过；583 个静态资源完整性检查通过。
- Grafana：无网络、无主机端口、tmpfs 数据库的 PostgreSQL 断言通过，包含“未发布候选不清除失败；正式发布完成后才清除”的正反例。
- 独立代码复核：已处理暂停覆盖、共享 Wiki 提前写入和补偿记录不一致问题，最后复核未发现剩余明确阻断。

真实浏览器验收使用当前认证中间件、handler、service、真实独立 SQLite/Redis 和真实腾讯企业邮箱 SMTP，接入本地当前前端。仅创建隔离测试账户，不改变生产同名邮箱账户：

1. SMTP TLS 1.3、AUTH 返回 235。
2. 浏览器请求验证码，一封邮件投递到配置邮箱；通过只读 IMAP 确认本次新邮件，验证码不落盘。
3. 错误验证码被拒绝，密码未改变。
4. 正确验证码改密成功；真实数据库及令牌校验确认旧会话已撤销。
5. 重复使用已消费验证码被拒绝。
6. 新密码登录成功，进入隔离账户的“等待加入工作空间”页面；随后正常退出。

密码重置测试账户/数据库/Redis容器与隧道已清理，该隔离测试没有改变生产账户。验收 JSON 和截图位于本地 `results/sync-code-review-20260906/`；不包含验证码、密码或令牌。后续生产部署与同步验收单独记录如下。

## 5. 生产部署与回滚

- 已部署镜像：`marvel/weknora-app:v0.7.2-sync-safe-reset-20260906`、`marvel/weknora-ui:v0.7.2-sync-safe-reset-20260906`；镜像 ID 和二进制哈希记录在隔离目录 `candidate-images.json`。
- App 镜像 ID：`sha256:9782cf0b32a936a15279a7f811b8d35888c247eb4fbed83b5c91ed04ce3ab160`；UI 镜像 ID：`sha256:10d43db5a5debbbfc59b80dadc65aac100c79fa15c523351a78b76cb57bbdce5`。
- 最终二进制 SHA256：`228d30c917694aab135078a747dca4bc15cf5b23e44a9dd89e835f561d3b9c71`，已从运行容器核对一致。App/UI 健康，公开认证配置返回密码重置已启用。
- 切换前确认九个队列无活动/待执行/定时/重试任务，解析及同步空闲；已备份 Compose 覆盖配置和 Grafana，只重建 app/frontend。其他容器 ID 和启动时间均未改变。
- SMTP 密码从既有规范密钥文件读取，仅写入服务器专用、权限 0600 的运行时 env 文件，并由 Compose `format: raw` 引用；未写入仓库或验收产物。
- Grafana 实际加载版本 26，候选及处理失败排除条件存在，数据源查询 HTTP 200。部署记录：`/public/knowledgebase/results/sync-safe-release-20260906/`。
- 用户授权对其账号拥有的“腾讯文档同步演示”执行一次全量同步，源 ID `2a3f5162-0ca2-4ec2-ba47-b1264babba26`。Chrome 中临时改为 full，原 resource_ids、每天 02:00 计划、覆盖策略及删除同步开关均已核对保留；2026-09-06 19:58:09（Asia/Shanghai）启动，20:16:53 结束，日志 ID `f4eee70a-b728-46c6-a014-db77af7da301`。其他既有源未手动重跑。已经通过 Chrome 恢复 incremental，并从数据库回读确认原范围、每天 02:00、overwrite 及 sync_deletions 均保持原值。
- 历史缺少稳定 ID 的失败不自动迁移或清除。
- 回滚前先暂停腾讯文档同步并核对候选。旧版不理解候选可见性协议，不能带着未处理候选直接降级；先完成或明确撤销候选，保留最后可用版，再恢复旧 app/UI 镜像和监控备份。无数据库表结构回滚。
- 密码重置可单独通过 `WEKNORA_AUTH_PASSWORD_RESET_ENABLED=false` 关闭；已经成功重置的密码和会话撤销不会因镜像回滚自动撤销。

## 6. 剩余边界

- 当前进程内同步锁/凭证预算适用于单 app；多副本需要共享协调后再扩展。
- 安全替换期间需要容纳新旧两份文件，清理失败可能暂时保留两份已可用版本；优先保证内容可用。
- 源端没有修改时间的资源无法精确绑定上游快照版本，保留后续正常同步的重新导出/指纹比较；不宣称上游 exactly-once。
- 腾讯参数错误、不支持的文件类型、鉴权失败或大小超限仍需要更正配置/源内容或人工处理；有界重试不保证永久故障自动恢复。
- 本批源码按邮件重置、分钟重试、目录保留、FAQ 同步、安全替换及发布记录拆分提交，目标为主项目定制仓库，不能混入 MCP 专属仓库。

## 7. 生产全量同步验收（2026-09-06）

通过用户 Chrome 中已登录的 Michael 账号，在其拥有的 AIT组知识库操作。一次全量遍历共处理 9 个源条目：更新 8、失败 1，创建/删除/跳过均为 0，耗时 18 分 44 秒。浏览器同步历史显示“部分成功”，容器 `streaming sync completed` 日志与数据库计数一致。自动补偿为同一次任务的按文件恢复，没有再次触发全量遍历。

| 验收项 | 生产证据 | 结论 |
| --- | --- | --- |
| 安全替换 | 8 组新旧记录逐一比较，新版 processed_at 均早于旧版 deleted_at；首次为 20:00:24.515 → 20:00:26.308 | 通过 |
| 候选隔离 | 连续状态采样捕捉到 candidate=true，匹配旧版仍 completed 且未删除；最终无未发布候选 | 通过 |
| 真实目录 | 8 份新版均有 folder_path/source_path；Chrome 可进入“AI教程与培训”目录，并看到已完成及腾讯文档来源 | 通过，当前选区只有一级目录；更深层级由隔离回归覆盖 |
| 失败不确认 | 失败文件没有新 knowledge、成功游标不包含该 external_id；旧历史记录仍 completed、未删除 | 通过 |
| 分钟级文件补偿 | 首次失败 20:14:27，三轮实际开始时间为 20:16:53、20:21:35、20:30:10，分别符合约 2/4/8 分钟加抖动；最后一轮 20:30:17 结束 | 三轮实测通过；未恢复，按上限转人工 |
| MCP 传输重试 | 本次没有制造/观察到 MCP 429 或网络中断 | 已部署并通过测试，不宣称生产故障恢复已验证 |
| 文件大小上限 | 本次条目未触及 100 MiB | 限制保持，非本次生产实测项 |
| 页面与监控 | Chrome 错误日志为空；Grafana 实际查询成功，并包含候选/处理失败排除条件 | 通过 |

失败项“Codex紫鸟操作教程”（file_id `XebKaZZFhmKk`）是智能文档。只读交叉核查：腾讯 `get_content` 成功响应但 content 长度为 0；`smartcanvas.read` 返回 3 个 Image、4 个空 Paragraph，分页已结束，纯文本长度为 0。因此普通正文读取入口未提供可入库内容，新版报入库失败并保留错误，未像旧路径一样转抓网页外壳并把它确认成正文。纯图片智能文档需要完善图片读取/OCR 接入，**本次不能宣布全部源文档同步成功**。

三轮补偿仍返回同一空正文，未自动恢复。第三轮于 20:30:17（Asia/Shanghai）结束，日志标记 `needs_manual`，文件游标为 `exhausted`、attempt=3，未再安排下一次补偿；只重试了该失败文件。没有修改源文档、缩短退避或手动确认成功。`sync_logs.started_at` 对等待补偿的行是建行时间，不能用它当作实际执行时间；实际执行以容器 `processing data source sync` 日志为准。最终九个队列 active/pending/scheduled/retry 均为 0，未完成解析和运行中同步均为 0。

最终有效记录仍为 10：8 份新版及 2 份原样保留的历史记录。失败文档的旧记录没有 datasource_id/external_id，不能按标题猜测绑定；另一份历史记录“闺蜜机用户常见问题”不在本次 9 个遍历条目中，本次没有删除或移动它。因此“全部历史记录均已更新、全部记录均已归入目录”的检查明确为 false，不能与“本次 8 份成功新版目录完整”混淆。

证据目录：服务器 `/public/knowledgebase/results/sync-safe-release-20260906/`，本地项目 `results/sync-safe-release-20260906/`。主要文件为 `deployment.json`、`grafana-live.json`、`live-acceptance.json`（8 组时间顺序、候选状态及保留记录）、`lifecycle-events.txt`（仅事件/计数，无正文）、`retry-chain.json`、`failed-cursor-proof.json`、`empty-image-document.json`、`source-restored.json` 及 Chrome 同步日志截图。配置备份及运行时密钥文件不复制到本地产物。
