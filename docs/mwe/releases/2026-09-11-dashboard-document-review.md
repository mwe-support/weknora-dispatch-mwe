# 2026-09-11 文档复查看板与 llama.cpp 监控鉴权

## 基线与提交范围

- 上游版本：WeKnora v0.7.2；目标仓库：`mwe-support/weknora-dispatch-mwe`，分支：`weknora-v0.7.2`。
- 本批基线：`ed0c9645`。按两个独立改动提交：文档复查看板；llama.cpp 指标采集鉴权。
- 仅修改 Grafana、只读观测投影、生成器及其测试，以及 Alloy 采集配置和安装说明。应用、前端、模型镜像及业务数据库迁移版本保持不变；没有 MCP 专属修改。

## A. 文档处理与失败复查

原看板同时展示新旧流程，主表重复显示批次 ID、查询状态和事件行，缺少源路径及对应文件大小。同一文档的问题生成子任务可以产生大量同类失败，挤占复查列表。

- 保留原有两个 dashboard UID。总览突出新流程统计、复查入口和组件状态；硬件趋势与旧流程记录折叠保留。
- 生命周期页按失败复查、文档进度、批次概览排列，阶段重试、事件时间线和一致性明细折叠保留。默认范围为新流程，可切换历史或全部。
- 工作空间、知识库及来源使用名称筛选，后两项随上级筛选联动。支持文件名、腾讯源路径和稳定 ID 搜索。
- 文件表展示名称、腾讯路径、文件或响应大小、工作空间/知识库、阶段、状态、原因、时间、原文链接和详情。
- 失败表按租户、知识库、来源、文档身份、阶段及错误合并，保留异常次数与全部事件 ID。扫描阶段使用源文件 ID/步骤 ID 区分尚未准入的文档；历史错误仍按原始错误身份区分。完整时间线不合并。
- 总览异常数仍按事件计数，标题明确为“待复查异常事件”；普通数量用中性颜色，异常非零用红色。
- 修正 Grafana 自定义筛选器不能可靠表达空值的问题，“全部状态”使用明确的 `all` 值。关闭详情 JSON 的自动换行，避免内容撑高整行；名称、路径和原因仍可换行并检查完整值。

### 数据与权限边界

仅在 `mwe_observer` 增加三个投影：`processing_job_context`、`processing_step_context`、`processing_legacy_context`。通过精确 job、step 或原始错误序号/摘要关联，不按标题猜测关联，不从当前知识版本补写旧尝试的大小。

- 路径来自当次扫描记录的腾讯目录与标题，不采用后来移动的 WeKnora 目录。
- 文件大小来自该版本成功下载/核验的字节数，或对应失败的已记录实际字节数；原生接口返回量单独标注“响应”。未知显示“未记录”，不以限制值、配额或旧版本大小替代。
- 腾讯链接仅允许已记录的 HTTPS `docs.qq.com` 地址及有限的 `mode`/`resourceId` 参数；带任意签名、token 或未知参数的链接不显示。
- 不开放原始配置、完整任务输入/结果、错误正文或业务内容；监控角色仍默认只读且不能读取原始业务表。
- 每表一次完整查询结果在浏览器内分页。合并后的结果超过 10,000 行时显示下限提示并要求缩小筛选；不返回伪装完整的截断列表。

## B. llama.cpp 指标采集鉴权

现场两台 Q4 容器每 30 秒出现一次 `operator(): unauthorized: Invalid API Key`。原 Alloy 将两台 Q4 与 embedding/reranker 放在同一个无鉴权 `/metrics` 采集组。

可重复的 HTTP 对照：两台 `/health` 无密钥均为 200；`/metrics` 无密钥均为 401，携带各自容器现有密钥均为 200。修复前两台 `up{job=~"q4-gpu.*"}` 均为 0。

拆分两台 Q4 的采集配置，分别使用 `LLAMA_GPU0_API_KEY`、`LLAMA_GPU1_API_KEY` 的 Bearer 鉴权。安装器仅在缺失时从现有容器的 `--api-key` 参数填入受保护环境文件，不打印或轮换密钥；显式配置优先。embedding/reranker 不接收 Q4 密钥。密钥轮换后需同步更新监控环境文件。

## 验证与线上结果

- `python scripts/render-processing-observability.py --check`：通过。
- `python scripts/render-observability-failure-queries.py --check`：通过；原历史查询与分页配置保留。
- `python scripts/test-observability-failure-queries.py --emit <fixture.sql>`：通过；生成的最终 SQL fixture SHA-256 为 `991c4835c840370caa7038f223859eceac751954d76bdb94897ccf7a77bc9eaf`，与已经在隔离 PostgreSQL 执行、断言通过并回滚的版本相同。
- 隔离 PostgreSQL：`go test ./internal/application/repository -run '^TestProcessingPostgresConcurrentGenerationClaimAndAtomicFailure$' -count=1`，通过，3.567 秒。覆盖六条真实 Grafana SQL、只读登录和原表拒绝、未准入文件路径搜索、下载大小、危险 URL 隐藏、同类异常合并及事件 ID 保留。测试源包 SHA-256：`0b1349e170ac055e44306c9a09329f05983033da8d8c314b7b8ff7b60ff177a8`；之后仅调整显示列宽、统计颜色和入口高度。
- Grafana 13.1.0 隔离实例：使用生产表结构的 schema-only 副本和虚构记录，验证 1.50 MB 下载文件、5.00 KB 原生响应、未知大小、扫描失败、历史记录、下拉联动、路径搜索和 JSON 详情。没有复制业务数据。临时数据库在原容器停止后已清空，因此重新建立，未启动旧测试应用或模型。
- `grafana/alloy:v1.18.0 validate /etc/alloy/config.alloy` 在无网络验证容器中通过；`bash -n scripts/install.sh` 通过。
- 线上以 Grafana 原生 provisioning reload 更新页面；六条查询均通过实际 Grafana PostgreSQL 数据源执行。仅为加入密钥环境变量重建 Alloy；应用、前端、GPU 模型和其他既有生产容器的 ID、镜像、启动时间保持不变，数据库迁移版本仍为 89、dirty=false。
- 20:32:46（Asia/Shanghai）复核：自 20:28:59 的 Alloy 更新起，两台 llama.cpp 新增鉴权错误均为 0，两条指标采集 `up` 均为 1。
- 线上 Chrome 实测：默认新流程，原 66 条未解决事件显示为 11 条文档/阶段/错误组合；每行保留异常次数，完整事件仍在详情和时间线。该数量为验收时快照，会随业务运行变化。
- 修改前后页面截图与完整诊断结果存放于本地 `results/dashboard-clarity-20260911/`，不提交真实业务文档名称、路径、密钥或原文内容。

## 发布标识与回滚

- 初始观测配置包：`7b74669ad6e8a588748587a628bc95d9938ec1de965da91df2dd0c603a430cca`。
- 最终页面修订包：`a3b476b9efc1cda75828a781b919d89d8ff5daf2d99a34cffe2d4f64abfbdf12`。
- 本次无新应用镜像。沿用 Grafana 13.1.0、Alloy v1.18.0。
- 生产原文件、受保护环境备份及发布清单：`/public/knowledgebase/results/dashboard-clarity-20260911/`，目录权限 0700、环境备份 0600。
- 回滚工具：`/public/knowledgebase/upgrade-tests/dashboard-clarity-20260911/rollback.py`。在已核验主机执行后，恢复原 13 个观测文件和原监控环境，删除本次三个新增观测视图，仅重建 Alloy 并 reload Grafana。业务表、任务状态和应用镜像不回滚。回滚后复核页面、采集和应用健康；回滚会恢复原无鉴权采集问题。
- 仓库 README 和 `.env.example` 更新只随代码提交，未覆盖生产上独立维护的 README 或示例环境文件。

## 队列观察与残余限制

20:23 至 20:29，源内容读取成功数由 39 增至 44，准入文档由 180 增至 184；说明前段存在实质进展。同期转换成功数仍为 4、已发布文档仍为 3。20:32 同步队列为 2 个执行中、280 个等待，原生读取成功仍为 44。扫描持续发现任务，因此等待数增长不等于完全停滞。

当前维护工作池并发为 2，扫描、源内容读取、导出、下载和转换共用同步队列，转换仍在等待。另有问题生成空结果、摘要、分块映射等既有阻塞；本次没有修改并发、重放失败任务或声称业务处理已经全部恢复。监控鉴权修复与业务失败应分别复查。

旧记录缺失的源路径/大小无法可靠补造；显示“未记录”。原生响应量不等于导入文件大小。监控页面为既有管理员全局观测面，工作空间筛选不是新的权限边界。
