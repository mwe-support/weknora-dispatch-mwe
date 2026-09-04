# Sheet 空占位修复与空行过滤消融实验

状态：本地消融及服务器整包测试通过，已部署生产；4份目标Sheet后处理全部完成，1086个分块全部ready。研发超大文件为独立遗留问题。纳入2026-09-04提交批次，推送状态见批次发布记录。
上游：WeKnora v0.7.2。目标仓库：mwe-support/weknora-dispatch-mwe；目标分支：weknora-v0.7.2。

## 结论与根因

采用“精确空占位过滤 + 现有输出空行过滤 + 完整范围扫描 + 严格坐标校验”。

腾讯 Sheet MCP 的实际响应包含所有字段为零值的条目：row=0、col=0、value_type为空，且字符串、公式为空、数值0、布尔false。原实现把它们当作真实A1：第一页可能覆盖表头，后续页触发越界。此前整页相对坐标兼容不能解决混合在绝对坐标响应中的空占位。

修改仅限共享的fetchSheetMarkdown路径：

- 在坐标模式判断及赋值前跳过 `cell == (SheetCell{})`，不改动客户端返回切片。
- 合法NUMBER=0、BOOL=false、公式和有类型的空单元格不属于这个零值结构体，不按占位删除。
- 保留既有空行过滤、源行号、全部分页扫描及越界报错；空白页不能成为提前结束条件。
- 增加 `sheet_cell_entry_count`、`sheet_empty_placeholder_count`、`omitted_empty_row_count` 元数据。
- 越界错误补充请求行列范围，使用0-based坐标，不记录正文。

## 固定数据与实验方法

从两份已授权源表抓取所有8个页面：4张子表，共791个声明行、4509个返回条目。抓取后固定响应，所有方案使用同一份输入，不反复调用腾讯接口比较不同版本。

测试数据位于 `internal/datasource/connector/tencentdocs/testdata/sheet_empty_ablation.json`。正文、公式及非零数值已替换为测试标记；保留坐标、类型、零/非零、空/空白字符串等过滤相关性质。没有凭证、签名URL、真实业务单元格内容。

| 样本 | 声明扫描行 | 有效输出行 | 空行 | 返回条目 | 全零占位 |
|---|---:|---:|---:|---:|---:|
| supplier | 191 | 156 | 35 | 1404 | 165 |
| overseas，3张子表 | 600 | 391 | 209 | 3105 | 369 |
| 合计 | 791 | 547 | 244 | 4509 | 534 |

这里“有效输出行”是当前捕获响应中的非空内容行，不是业务记录数，也不独立证明腾讯服务没有漏返回内容。声明行数只证明扫描范围；实验另外逐单元格对照捕获响应与输出，并比较各子表源行号。

## 六组消融结果

每组含2个真实脱敏样本、3个正向边界场景、8个必须拒绝的坐标负例。通过数包含正确拒绝，不能理解为导入成功率。

| 方案 | 通过用例 | supplier输出行 | overseas输出行 | 结果 |
|---|---:|---:|---:|---|
| A 不过滤占位或空行 | 8/13 | 越界失败 | 越界失败 | 未修复 |
| B 只过滤输出空行，即原实现 | 8/13 | 越界失败 | 越界失败 | 过滤发生得太晚 |
| C 只过滤精确空占位 | 8/13 | 191 | 600 | 能导出，但多输出244个空行 |
| D 精确占位过滤 + 输出空行过滤 | **13/13** | **156** | **391** | 选用；完整扫描、内容及源行号保留、负例拒绝 |
| E D基础上直接丢弃所有越界条目 | 5/13 | 156 | 391 | 8个真实坐标异常被掩盖，不可采用 |
| F 按文本/公式为空过滤单元格 | 6/13 | 151 | 391 | 供应商少5行，数字0/布尔false等值丢失，不可采用 |

另有100行/191行最小回归及100次固定种子的条目顺序扰动。D组全部通过；首行、数字0、false没有被占位覆盖。正向边界还覆盖连续空白页后的尾行、公式、错误文本、富文本、时间文本和既有相对行坐标兼容。

## 可重复验证

从仓库根目录，使用Go 1.26+、Node.js，GO_EXE可指定已有Go路径：

```text
node scripts/mwe-sheet-empty-ablation.mjs /path/to/sheet-empty-ablation-results.json
```

脚本直接复制真实sheet.go、types.go、客户端接口和本次测试到临时目录。只剔除不参与Sheet算法的网络客户端编译期类型断言，并从原connector.go提取firstNonEmpty；不复制或另写分页算法。各组仅开关指定过滤步骤，结束时校验并删除自身临时目录。非预期编译错误、测试数量变化或D组失败将使脚本失败。

实际执行：Windows便携Go 1.26.4，GOPROXY=off、GOMAXPROCS=2；各组结果已重复获得一致结论。`git diff --check`通过。

最小回归在修改前的远程Go 1.26隔离克隆上明确失败：

```text
TestSheetEmptyPlaceholderRegression/100: empty placeholder overwrote A1
TestSheetEmptyPlaceholderRegression/191: returned out-of-range cell (0,0)
```

标准整包命令（部署前仍需运行）：

```text
go test ./internal/datasource/connector/tencentdocs -count=1
```

本机整包编译受无关CGO依赖阻碍：internal/utils/inject.go引用的pg_query.Parse/Deparse不可用。远程修复源码传输被安全审查阻止，未尝试绕过。因此隔离实验不是整包集成测试或生产验收。

## 发布影响、回滚与剩余边界

- 预期影响：修复空占位导致的同步失败和首格覆盖，避免空白行制造冗余内容；不修改数据源范围、游标、同步周期或数据库。
- 生产仍为 `marvel/weknora-app:v0.7.2-datasource-recovery-20260902`，未构建或切换新生产镜像。
- 部署前需授权的隔离整包测试、构建及真实定向同步验收；本次不重新入队任何源。
- 未来回滚应恢复部署前记录的镜像与override备份。本地补丁可单独撤销，不应覆盖其他未提交工作。
- 人事两份表格的本机MCP权限不足，没有纳入真实样本，不能宣称逐份验收通过。
- 相对/绝对坐标兼容沿用现有逻辑。纯落在相对索引范围内的异常响应存在协议歧义，本次没有放宽它，也未证明所有腾讯响应形态均已覆盖。
- 精确零值判定依赖协议为合法0和false保留value_type；缺少类型与其他值的零坐标条目无法与空占位区分。新增协议形态应提供原始脱敏样本再扩展规则，不能改为任意空文本或越界丢弃。
- code=111、超100 MiB文件及权限问题不属于此修复。
- 本记录为中文运维发布记录，无对应多语言文档变更。

## 生产部署追加记录

在用户明确要求生产部署及失败文档重试后，已获准向自有marvel-kb传输修复源码，并完成以下步骤：

- 腾讯连接器、internal/datasource、application/service、application/repository整包测试通过。通用数据源测试首次因禁网导致公共域名DNS校验失败，正常DNS条件下重跑通过。
- 切换前9个Asynq队列的active/pending/scheduled/retry均为0；无非终态知识，无运行中的同步，MinerU processing/queued均为0。
- 使用已有完整Go缓存和golang:1.26-bookworm编译生产二进制，以旧生产镜像为基础只替换/app/WeKnora。
- 新镜像：`marvel/weknora-app:v0.7.2-sheet-empty-placeholder-20260904`。
- 镜像ID：`sha256:5e6f9c3de2a962449d866c4e76ba5a1b943bcdc89c9da0a9144dc482e78ff384`。
- 二进制SHA256：`d7eedd7b4f8129996ef3773c0157174112142a6360c4054310b2bc91937992cb`。
- 仅app被重建并healthy，其他运行容器的ID、镜像、启动时间逐项未变化。
- override备份：`/public/knowledgebase/weknora/override.yml.bak.20260904-sheet-empty`。
- 部署审计：`/public/knowledgebase/results/sheet-empty-20260904/deployment.json`。
- 如需回滚，恢复上述override备份，在`/public/knowledgebase/weknora/src`执行`docker compose --profile minio -f docker-compose.yml -f ../override.yml up -d --no-deps app`，确认旧镜像healthy；本次部署脚本已包含启动失败回滚。

标准队列真实增量验收批次：`20260904T031711Z`。4个有效数据源各入队一次，ForceFull=false；未修改范围、游标、同步频率。一次性marker防止重复执行。

| 数据源前缀 | Sync log |
|---|---|
| ebd62ec1 | a18ddbdd-d69a-48e6-90ac-7cfbd37b0706 |
| 8f2eae36 | 9390a287-9fcb-41d3-b701-caec7d6f3684 |
| a956661d | e2239c53-34cd-452f-bde5-0ca66d0dffd3 |
| f3dd9d85 | 5e86cfec-c093-4548-8a76-845b3509ec95 |

入队审计：`/public/knowledgebase/results/sheet-empty-20260904/execution.json`。测试凭证文件仅存在服务器临时目录，执行后已删除。

首个Sheet真实结果：扫描191行、非空156、过滤占位165、过滤空行35；新元数据已进入知识记录。此时仍处于finalizing，不能把同步创建/更新成功等同于后处理验收完成。最终结果待更新。

### 四份目标Sheet生产抓取结果

| 目标 | source/scanned行 | 非空行 | 空占位过滤数 | 空行过滤数 | 最后观察 |
|---|---:|---:|---:|---:|---|
| 采购目标表 | 191/191 | 156 | 165 | 35 | finalizing，8个子任务 |
| 海外目标表，3张子表 | 600/600 | 391 | 369 | 209 | finalizing，24个子任务 |
| 人事目标表1 | 693/693 | 306 | 4721 | 387 | finalizing，17个子任务 |
| 人事目标表2 | 600/600 | 219 | 3538 | 381 | finalizing，7个子任务 |

这次人事两份由用户授权的生产数据源凭证走标准同步取得，与此前本机MCP无权读取是不同的权限路径。4份全部出现新版本排除统计，未再报空占位越界。海外sync已success，采购、人事和研发sync仍running；当前本批失败数均为0。此表是抓取成功证据，不是最终后处理验收。

原持久心跳 `marvel-kb-datasource-deadletter-recovery-20260902` 已更新为每5分钟只读跟踪本轮sync log与目标知识，禁止重复入队；异常暂停，终态验收后更新记录并删除自动化。
