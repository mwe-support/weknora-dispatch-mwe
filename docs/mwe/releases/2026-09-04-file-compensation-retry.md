# 失败文件补偿机制（隔离回归通过，已部署）

基于WeKnora v0.7.2，主仓库weknora-dispatch-mwe，目标分支weknora-v0.7.2。
状态：服务器隔离整包回归、关键用例3轮竞态检测、前端type-check和git diff --check通过。用户授权后已部署app与UI；未重试历史任务。纳入2026-09-04提交批次；不以健康检查代替真实业务故障恢复验收。

## 范围

- 为Tencent Docs源端文件获取失败添加持久化补偿；复用sync队列及connector cursor，不增加数据库表或服务。
- 游标file_retries按external_id记录失败文件、次数、下次时间及已知export task ID；任务本身只含数据源/日志标识，从最新游标读取待补偿项。
- 每个文件最多3次补偿，约2/4/8分钟加0–25%随机延迟。待补偿文件合并成按数据源执行的批次；已有成功文档不遍历、不重新下载，重试遍历不参与删除检测。
- 保存export task ID后继续查询同一任务，不因进度失败重新StartExport。导出开始结果不明确转人工处理。
- 导出前先持久化意图，再保存返回task ID；若返回ID未能落盘就中断，恢复后转人工，不重复创建。成功/不变指纹跳过后清除已结束的导出任务记录，避免复用陈旧task ID。
- 配置/凭证变化使旧补偿失效，暂停/删除源不执行补偿；任务校验tenant/source/log归属。
- HTTP429两种常见包装、暂时超时、10012/10328按阶段区分；323908、大小超限、鉴权、未知111等不盲目自动重试。
- 同步只剩文件失败时由补偿机制处理，避免整批队列重试；目录枚举失败不是单个文件，不在文件补偿里扩展为子树重跑。根级无法获取文件清单的临时错误仍走既有任务重试。
- 日志记录retry_state/attempt/round/next_retry_at/retry_of；前端同步日志抽屉展示本次重试安排，中英文文案已添加。
- 入队失败会留下可重入计划并将未发送日志标为失败，避免“运行中”死链阻挡调度；队列恢复后复用稳定日志和任务ID。
- 数据源创建不能注入执行游标，设置/凭证更新不能覆盖执行游标，专用UpdateSyncState仍能正常推进。
- 共享HTTP凭证预算：同一进程内相同凭证至少间隔2秒；429/503尊重Retry-After秒数或HTTP日期，无提示时冷却10秒。凭证仅以摘要作为预算键。

## 已知边界与待验证风险

- 当前生产为单app。进程内数据源锁与凭证预算不适用于多副本，需要共享租约/预算后才可横向扩展。
- 仅补偿源端获取/导出/下载失败；入库错误保留为失败/需人工处理，不纳入本次源文件补偿。后续MinerU/LLM任务继续使用各自的任务处理机制。
- 历史缺少稳定ID的错误不会自动迁移进补偿列表。
- 外部导出与本地checkpoint仍不能原子提交，但已以预写意图+保守人工处理防止未知结果重复导出。这不是exactly-once：未知任务可能需人工核对，未保证自动恢复。
- 子任务创建与队列入队仍不是单一事务。已验证单独入队失败后的重入与真实Asynq去重；数据库和Redis同时持久故障、真实进程kill/reboot不是本次测试内容，需保留运维告警与人工介入。
- 未验证生产及Grafana对新状态的最终展示；前端仅类型检查通过，未浏览器实测。

## 验证状态

已编写回归：失败文件定向重取、成功游标保留、凭证/范围变更拒绝、export task复用、尝试上限、错误分类、Retry-After、预算隔离/取消、稳定入队ID/日志去重、永久错误与暂停源不入队。

2026-09-04实际通过：

```text
go test ./internal/datasource/connector/tencentdocs ./internal/application/service ./internal/application/repository ./internal/types -count=1
ok tencentdocs 7.111s
ok service 2.604s
ok repository 1.019s
ok types 0.689s

go test -race ./internal/datasource/connector/tencentdocs ./internal/application/service ./internal/application/repository -run 'Test(FileRetry|FileCompensation|ExportIntent|RetryAfter|DataSourceSyncLock|DataSourceSettingsCannotOverwrite)' -count=3 -v
ok tencentdocs 1.882s
ok service 1.910s
ok repository 1.756s
```

本机已执行：`npm run type-check`（frontend）通过；gofmt、git diff --check通过。

初次传输曾被安全审查拒绝；用户随后明确授权将本次源码和测试发送到公司自有marvel-kb（192.168.18.25）指定隔离目录。现已在 `/public/knowledgebase/upgrade-tests/file-retry-20260904/repo` 完成测试，使用golang:1.26-bookworm，禁网、最多2核4GiB，仅挂载隔离源码和Go缓存。没有生产数据库、Redis、凭证或数据卷；Redis用miniredis，数据库用内存SQLite。

12组关键用例连续3轮race通过；验证导出意图后中断、最后一次尝试中断、稳定ID去重、实际Asynq队列仅生成1个scheduled任务、入队失败重入无孤儿running日志、暂停/永久错误不安排补偿、凭证预算隔离/取消、Retry-After解析以及配置更新不覆盖新游标。原导出测试因新增检查点由1次调整为3次，并增加了完成后无陈旧任务记录的断言。

竞态日志：`F:/腾讯文档知识库设计/results/file-retry-isolated-20260904-race.log`。

发布前仍需独立变更审查、浏览器状态展示验收及生产空闲窗口协调。本次没有构建生产镜像或执行回滚。部署前必须重新记录当前生产镜像与配置备份，不能依据历史镜像名盲目覆盖。

共享工作区中的Sheet空占位、超限日志和Dashboard改动属于此前独立批次，本记录不将它们混为已发布补偿机制。

## 本次生产发布

- app：`marvel/weknora-app:v0.7.2-file-compensation-20260904`，镜像ID `sha256:3b5f03680eab93eef0ca7902d7dca3ae15059a8e3f2d7df3dfd5559a12d25382`。
- UI：`marvel/weknora-ui:v0.7.2-file-compensation-20260904`，镜像ID `sha256:a3124325a3d3459cd5df7cfad7e08f633fbce4235220d0cae9295fc31798916e`。
- 前端生产构建及资源完整性检查通过：452文件，2 HTML、313 JS、50 CSS。保留现有旧hash静态资源以兼容已打开页面，不改nginx路由。
- 第一次切换预检发现新入队6个正常同步任务，立即中止；未改配置。等待队列再次全部归零后执行发布。
- 实际切换仅重建app/frontend，两个容器均healthy；其他运行容器的ID、镜像、启动时间未变。
- 配置备份：`/public/knowledgebase/weknora/override.yml.bak.20260904-file-compensation`。
- 部署审计：`/public/knowledgebase/results/file-compensation-release-20260904/deployment.json`。
- 回滚：先确认任务空闲，恢复上述override备份，在`/public/knowledgebase/weknora/src`执行`docker compose --profile minio -f docker-compose.yml -f ../override.yml up -d --no-deps app frontend`，核对旧app/UI健康。发布脚本已包含启动失败自动回滚。
- 不自动迁移历史title-only失败，不重新入队旧文档。补偿适用于此版本开始正常同步产生的、可分类且有稳定身份的失败文件。
