# 总览区分执行、等待与模型请求

- 上游版本：WeKnora v0.7.2；仓库 `mwe-support/weknora-dispatch-mwe`，分支 `weknora-v0.7.2`。
- 范围：Grafana 总览、只读监控汇总视图、生成器与回归检查。没有应用/MCP 业务代码、生产并发、模型配置或数据源调度变更。

## 问题与根因

用户看到 GPU 空闲，而总览显示大量活动任务。现场浏览器复现为“活动 / 待处理阶段”约 1870；主机两张 GPU 利用率均为 0，Qwythos、向量与重排服务的执行/等待请求也为 0。

原卡片把阶段队列视图的排队、外部等待、重试、失败、阻塞与执行中记录统一计数，没有展示真正已领取的阶段和模型服务请求。文档/批次总数又包含已完成记录，容易被误读为并发数。文档父状态 `running` 覆盖整个流程，不能代表每份文档都正在使用 GPU。另有少量 running 阶段的租约已失效，不能将它们算作有效执行。

此外，详细生命周期视图包含证据、历史和文档关联；总览只计数也会承担这些查询成本。第一版拆分预览的 8 张卡片约需 11.89 秒，排队/等待/异常/文档计数的独立查询均触发过 5 秒 statement_timeout。旧异常事件总数查询曾超过 30 秒。该延迟让各卡片更新时间更不一致。

## 分项提交

1. `perf(observability): add aggregate processing state counters`：在原有 observer 投影脚本增加 `processing_runtime_counts`。只从当前代 job 和 step 汇总六个计数，不联查正文、凭据、文件上下文或历史证据。保留原始业务状态与历史明细。
2. `fix(grafana): separate active stages from backlog and model requests`：首行拆为执行、排队/待派发、外部结果/重试、异常/待恢复；次行增加实际模型执行与排队请求，并注明文档/批次总数包含完成记录。生成器是单一来源，硬件和历史折叠区保留。
3. `docs(mwe): record dashboard activity verification and rollout`：记录本批次查询口径、验证、部署及回滚方式。

执行计数要求 `status='running'` 且执行租约未过期；缺失/过期租约归入异常待恢复。只统计 `is_current` 的 job，计划中的后续阶段不冒充排队。模型计数来自两个 Qwythos 实例及 Embedding/Reranker 的服务端指标，包含 4 个端点的完整性检查；缺采样或端点不可用时显示“采集不完整”，不补成 0。该模型卡片不含 MinerU，说明中已明确。

阶段与文档卡片显示数据库查询时的状态；模型和硬件显示时间选择器结束时刻的采样。页面明确说明，对照当前运行情况需保持结束时间为“现在”。原有相对时间、硬件采样说明和导航保留。

## 验证与生产结果

- `python deploy/mwe-observability/scripts/test-runtime-overview.py`：修改前失败，指出缺少执行与积压的独立卡片；修改后通过。
- `python test-runtime-overview.py --dashboard knowledgebase-overview.json --observer-sql observer-access.sql --postgres-container WeKnora-postgres --prometheus-container kb-observability-prometheus`：使用只读 CTE 执行真实视图定义及六条面板 SQL，覆盖有效/过期/缺失租约、排队、待派发、外部等待、重试、失败、阻塞、旧代、历史及未满足依赖的计划阶段；全部通过。未建测试业务记录。
- 同一脚本通过原生 `promtool test rules` 验证真实模型表达式：空闲返回 0，忙碌返回实际和数，缺一项指标或端点失败返回无有效值；执行和排队两种表达式均通过。
- `python scripts/render-processing-observability.py --check`：生成物一致。
- `node deploy/mwe-observability/scripts/test-hardware-navigation.mjs`：相对/历史时间、刷新周期与其他筛选保留，主机网卡采集口径不退回旧容器视图。
- 真实 Grafana 数据源接口：8 张卡片全部成功。轻量视图预览约 0.086 秒，生产回读约 0.091 秒。为单次现场耗时，不是长期性能保证。
- 原监控角色 `weknora_observer` 能读新汇总，但对 `public.data_sources`、`public.processing_steps` 原表的 SELECT 权限仍为 false。没有扩大到原始凭据或文档数据。
- 正式浏览器已核对新卡片与排版，记录截图保存在本次证据目录。

2026-09-12 11:05:32（Asia/Shanghai）生产回读：执行 2、排队/待派发 1373、等待结果/重试 274、异常/待恢复 227；模型执行与排队均为 0。当前版本文档 1227、当前批次记录 26，均包含完成记录。上述是阶段/请求/记录三种不同单位，不相加当作独立文档数量；正常调度会继续改变这些数字。

## 部署与回滚

发布标识 `dashboard-activity-20260912`。先创建安全汇总视图，再原位更新总览 JSON 与 observer 初始化 SQL，并调用 Grafana 原生 provisioning reload。无新镜像；所有运行容器 ID 保持一致。

- Dashboard SHA256：`ee4a47679b8f4b5d8fa0b5a21c560be0a4ba81c511909eb319aa33d9c1e59277`。
- Observer SQL SHA256：`29311fe38a1564867de74729601bd1cc92250796dfbe979c78f6f4703f49420f`。
- 两份部署文件与本地审阅源码字节一致。
- 本地证据：`results/dashboard-activity-20260912/`；主机回执/备份：`/public/knowledgebase/results/dashboard-activity-20260912/`。

回滚时，先从主机备份的 `dashboard-file-before.json`、`observer-access-before.sql` 原位恢复原路径并 reload Grafana；随后可删除本批新建的 `mwe_observer.processing_runtime_counts` 视图（不使用 CASCADE）。无需改动任务数据或重启应用。

残余边界：GPU 空闲时，源端读取、导出、下载和等待仍可继续。此修复让状态可解释、计数更快，并没有提升源端吞吐或清除既有积压/失败。完整异常事件及历史记录仍在文档处理与失败复查看板中，不用当前异常阶段数代替所有历史事件数。
