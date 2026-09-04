# 文件超限分类与同步前置失败观测

## 发布范围与状态

- 上游：WeKnora v0.7.2；主仓库 `mwe-support/weknora-dispatch-mwe`，目标分支 `weknora-v0.7.2`。
- Dashboard revision 9 已备份热加载到公司自有 marvel-kb (`192.168.18.25`, hostname `test`)。
- 后端日志增强代码及测试完成，**尚未部署后端**；无 GitHub 推送。
- 生产应用仍为 `marvel/weknora-app:v0.7.2-sheet-empty-placeholder-20260904`，启动时间 `2026-09-04T03:15:25.819858533Z`，最终核对 healthy。本次没有重启应用或 Grafana。
- 未触发/重复入队/取消同步或重试，未修改游标、凭证、范围或暂停的恢复自动化。未修改其他任务拥有的 Sheet 空占位源码、测试、数据集、脚本或发布记录。

## 动机与实现

1. 原导出下载超限只有泛化字符串。增加可 `errors.As` 识别的 `ExportSizeExceededError` 和稳定类别 `FILE_SIZE_EXCEEDED`，人可读原因明确为“文件大小超出上限”。生产上限保持 `100 << 20` 字节；重试常量、策略与队列路径完全未变。
2. 已知 Content-Length 超限时在读取 body 前拒绝，记录 `limit_bytes` 与 `actual_bytes`。未知/不可信长度读流达到上限加一时仅记录 `observed_at_least_bytes`，不把已读取前缀伪装成实际总大小。
3. 向兼容旧 JSON 的 `SyncItemError` 增加可选 external_id、file_id、source_resource_id、space_id、source、stage、category、occurred_at 和大小字段；获取元数据、内容、导出、下载及入库错误沿共享 batch/stream 路径保留身份与阶段。code=111 不映射为永久不支持，已有 323908 跳过策略未变。
4. 仅当腾讯连接器确实带着已获取的非空内容执行入库时写入 `source_fetch_completed_at` 元数据。空增量检查与旧版本重解析不能生成此抓取证据。
5. 文档失败视图从 `sync_logs.result.errors` 展开逐文件错误，关联有效数据源、知识库及租户，与当前知识解析失败合并；有效稳定 ID 同文件重复错误取最新，并保留关联 sync_log_id/knowledge_id。表格与页数使用同一 CTE。数据源汇总保留，增加已删除源/知识库及租户一致性过滤。
6. 稳定 ID 的同步失败仅在同租户、同知识库、同数据源且同文件的当前 knowledge 已 completed，并且存在失败之后的新抓取标记或新建入库版本时解除；不能只凭 updated_at、旧 completed 或后续 success 解除。

## 历史数据与可信度边界

- 生产历史错误只有 title/message，缺少稳定文件 ID。旧记录按数据源+标题归组并明确标记 `legacy_title_only / 恢复待核实`；同名不同文件的历史记录无法可靠拆分，不能声称每组必然是唯一真实文件，也不能靠标题匹配 completed 自动消除。
- 只识别精确旧超限错误格式，映射 `FILE_SIZE_EXCEEDED / download` 与其已有阈值。未将外部 HEAD 核查的大小或 ID 回填为旧日志自带字段。
- 首次只读核对：151 个保留错误样本形成 87 个历史待核实组，另有 3 个当前知识解析失败，合计 90 行/2 页。此数是显示证据组，不是 90 个已确认仍在失败的文件。当前数据源最新失败汇总为 1 行。
- 目标超限文件实际在文档视图出现，保留源 ID 与 sync_log_id，限制值 104857600，实际大小和文件 ID 保持空白（原日志没有记录）。验证日志 ID 为 `5e86cfec-c093-4548-8a76-845b3509ec95`。
- 每轮 `result.errors` 仍是最多 100 条的错误样本，已删除或未记录的历史证据无法由 Dashboard 重建；本次不扩大存储上限、不更改日志保留策略。
- 同内容真实抓取后被连接器指纹跳过、未产生新知识版本/抓取标记时，恢复将保守保持待核实，不伪造恢复证据。

## 测试与验证

Go 1.26 隔离测试使用当前工作树覆盖层、现有模块缓存，无生产凭证或应用目录挂载，未包含其他任务的临时执行测试：

```text
go test ./internal/datasource/connector/tencentdocs ./internal/application/service ./internal/types -count=1
ok tencentdocs 0.672s
ok service     2.555s
ok types       0.714s
```

覆盖已知长度超限拒绝读取、未知长度/虚报长度读流超限、恰好上限、100 MiB 常量保持、错误链包装、111 不等于永久不支持、字段 JSON 往返与未知 actual_bytes 缺省。

SQL 可重复测试：

```text
python scripts/render-observability-failure-queries.py
python scripts/test-observability-failure-queries.py --emit /tmp/observability-regression.sql
psql -v ON_ERROR_STOP=1 -f /tmp/observability-regression.sql
```

- 使用无网络、无端口、无生产卷的独立 PostgreSQL 17 容器；只创建合成临时表，事务最后 ROLLBACK；通过后测试容器已停止/自动移除，测试文件与日志保留。
- 覆盖无 knowledge 的前置失败、旧 completed 不解除、后续空增量 success 不解除、新 completed 解除、finalizing 不解除、删除源/知识库/知识过滤、跨租户不匹配、同名不同 ID、稳定身份别名及冲突、逐文件去重、解析/同步合并、未知大小、脏时间戳和页数/明细一致。
- 生成器回归确认除了面板 10/11 的 SQL/说明、doc_pages/sync_pages 的 SQL 与 revision 外，当前 HEAD Dashboard 的所有其他字段不变（包括模块内分页脚本、GPU、容器导航、最新死信逻辑）。
- 生产 `weknora_observer` 角色 READ ONLY 事务通过；EXPLAIN ANALYZE 首次计数执行 30.345 ms，无 schema/数据写入。
- 已登录 Chrome 1920px 页面实际显示前置超限行及 download/FILE_SIZE_EXCEEDED、限制字节数、sync_log_id、data_source_id、knowledge_base_id；现有输入框 Enter 从第 1 页到第 2 页，首行改变；已恢复第 1 页。浏览器错误/警告 0。
- 最终本地/生产 Dashboard SHA256 一致，Grafana 最近十分钟 level=error/warn 为 0；未运行移动端专项回归，UI 结构没有改动。
- `git diff --check` 通过。

审计日志：

- 服务器 `/public/knowledgebase/results/observability-file-errors-20260904/go-test.log` 与 `sql-test.log`。
- 本机 `F:/腾讯文档知识库设计/results/observability-file-errors-20260904/` 同名日志及合成 SQL。

## 热加载与回滚

- 写入前强制核验生产旧 SHA256 `1469ca374dd785d390f328b7d2be8124c9489e2f105cbef01621b1ad4e7e473c`，一致才备份替换，避免恢复旧快照。
- 备份 `/public/knowledgebase/results/observability-file-errors-20260904/dashboard-before.json`。
- 新 Dashboard SHA256 `c20ce3101734ee7e458a2f61a8a162d4deb0c5284177e187af14f901f262e76c`。
- 回滚仅将该备份复制回 `/public/knowledgebase/observability/grafana/dashboards/knowledgebase-overview.json` 并等待文件供应器热加载；不需要重启应用。
- 后端待协调空闲窗口后单独构建/部署，再验收新产生错误的 ID/阶段/大小字段。没有新的生产后端镜像标识，也不以 Dashboard 上线代替后端部署完成。
