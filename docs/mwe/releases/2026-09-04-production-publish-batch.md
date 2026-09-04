# 2026-09-04 生产部署与提交批次

上游WeKnora v0.7.2；唯一推送目标为 `mwe-support/weknora-dispatch-mwe:weknora-v0.7.2`。不推送MCP仓库、v0.7.1、main或官方上游。

## 本次独立提交

| 提交 | 范围 | 验证记录 |
|---|---|---|
| 88a65527 | Sheet空占位、空行与完整分页 | 2026-09-04-sheet-empty-placeholder-ablation.md |
| 0aeb94e9 | 文件失败身份、阶段、超限结构化日志 | 2026-09-04-file-size-and-sync-failure-observability.md |
| 72353532 | Dashboard逐文件失败、恢复依据及紧凑字段 | 2026-09-04-dashboard-columns-and-file-size.md |
| 23efbf30 | 有界持久化文件补偿、凭证预算与UI状态 | 2026-09-04-file-compensation-retry.md |

此外本地版本分支已有13个先前独立提交（7747af5b至610a95c5），包含空间名称刷新、成员选择、监控与分页、同步周期、MinerU预加载、后处理状态恢复和首次数据源重试修复；保持原历史，不混成单个提交。本批按正常快进发布这条分支，不强制推送。

## 生产验收

- app与UI均为 `v0.7.2-file-compensation-20260904`，两个容器healthy、RestartCount=0。
- 首次切换因6个正常同步任务入队而安全中止，未改配置；等待任务结束、全部队列归零后重新检查并切换。
- 仅app/frontend被重建；其余容器的ID、镜像与启动时间未变。
- 公网域名HTTPS本机验证返回200，本机解析直连192.168.18.25；这是访问与健康验证，不是新故障的端到端补偿验收。
- 发布后所有Asynq队列active/pending/scheduled/retry均为0。没有重新投递任何历史失败任务。
- Go四个相关包整包回归通过，12组关键用例3轮race通过，前端type-check、生产构建与452项资源完整性检查通过。
- 二进制SHA256：`15b12e63d07a7e164ce409abb7554abebdbe0596a2cffbe5dd83dddfd25ec05f`。
- 审计：`/public/knowledgebase/results/file-compensation-release-20260904/deployment.json`。
- 备份：`/public/knowledgebase/weknora/override.yml.bak.20260904-file-compensation`。

## 回滚和边界

空闲后恢复上述override备份，在weknora/src执行 `docker compose --profile minio -f docker-compose.yml -f ../override.yml up -d --no-deps app frontend`，确认旧版本健康。不得在活跃任务期间无条件回滚。

当前补偿锁和请求预算针对单app进程；多副本需分布式协调。历史无稳定ID的错误不会自动迁移；根目录枚举错误不当作单个文件补偿；未知导出开始结果转人工处理。正式业务重试效果需后续正常同步观察。

未跟踪的临时执行器 `internal/datasource/tmp_execute_datasource_recovery_test.go` 保留在本地但不提交；未纳入服务器凭证、原始业务正文、下载文件或签名URL。已检查新增差异中的常见令牌/私钥模式无命中；该检查不等于全面安全审计。

本批文档也更新Sheet验收结果、日志增强部署状态及补偿发布状态；无遗漏的独立文案改动。
