# 恢复本地转换与源端同步的队列隔离

- 上游：WeKnora v0.7.2；已核对 Tencent 上游标签 SHA 为 `3d5d8bfcdfeeea266b292b71cea616847af28d0f`，与本地标签一致。
- 目标：`mwe-support/weknora-dispatch-mwe` / `weknora-v0.7.2`。无 MCP 专属改动。
- 本批提交：`fix(processing): isolate local normalization from source queue`；发布记录归入同一修复批次。

## 根因与修改

定制生命周期把 `normalize` 与目录扫描、源版本探测、原生读取一起投递到 `sync`。这些任务争用维护池的 2 个执行名额；本地转换前面排着大量慢速网络任务时，已经拿到源内容的其他文档仍不能进入解析，核心解析池与模型服务持续空闲。

上游的数据源路径在取得 `item.Content` 后调用 `CreateKnowledgeFromFile` 进入既有文档处理流水线。本次定制额外拆出的本地转换阶段，本应遵守同样的资源边界；它仅校验/读取已有阶段产物、转换内容及登记候选文档，不需要调用腾讯接口。

现场 Grafana 在 11:38 显示 89 份当前文档停在内容转换阶段；部署前 11:58 冻结的可跟踪集合为 79 份。同期执行阶段为 2、模型执行请求为 0；近 20 分钟解析器和两张 Qwythos GPU 服务没有工作日志。

生产代码只将 `ProcessingQueue("normalize")` 改为现有 `default` 队列，由核心解析池及共享池消费。派发器和队列恢复器均复用这一函数。没有扩大总工作池容量、模型上限或腾讯接口请求预算；原有输入校验、依赖关系和重试上限继续生效。

失败/重试处理本身在提交状态后释放执行租约，重试耗尽不阻塞独立任务的原有检查通过。本次被复现的具体问题是源端慢队列阻止已就绪的本地阶段启动。同一文档的失败依赖仍然阻止该文档越过校验，不通过跳过失败或伪造完成来提高进度。

## 验证

- `go test ./internal/types -run TestLocalNormalizationDoesNotWaitBehindSourceIO -count=1`：旧代码失败，修复后通过；同时验证核心/共享池可消费本地转换，维护池不再消费。
- 使用带标签的隔离 PostgreSQL/Redis，执行 `TestProcessingReadyDocumentProgressesWhileSourceWorkerIsOccupied`：真实 Asynq 工作池与派发器下，旧代码 3 秒内无法完成另一个已就绪文档；修复后约 0.30—0.35 秒完成转换及其后续解析，而源端工作名额仍被占用。
- `TestProcessingNormalizationRerouteFencesOldDelivery`：旧投递仍在原队列时，恢复器可生成新队列投递；业务尝试次数保持不变，仅投递序号增加。先后重放旧投递都不能执行业务，当前投递只执行一次。
- `TestProcessingExhaustedRetryDoesNotBlockIndependentWork`：通过。
- `TestProcessingKnowledgeNormalizesParsesAndCommitsRealChunks`、`TestProcessingRealRedisRedeliveryAfterLostAcknowledgement`、`TestExportReceiptsDoNotWaitBehindSourceScanFIFO`、队列注册与消费检查：全部通过。
- 三个修改的 Go 文件通过 gofmt 检查。测试 PostgreSQL/Redis 已恢复原先的 exited 状态；测试使用独立 schema 和带随机前缀的队列。

最后一批验证的完整命令为：

```sh
go test ./internal/types ./internal/application/service ./internal/application/repository \
  -run 'TestLocalNormalization|TestProcessingReadyDocumentProgresses|TestProcessingNormalizationReroute|TestProcessingExhaustedRetryDoesNotBlock|TestProcessingKnowledgeNormalizesParses|TestProcessingRealRedisRedelivery|TestExportReceipts|TestQueue' \
  -count=1 -timeout=90s -v
```

真实 PostgreSQL/Redis 集成测试使用限定的 `PROCESSING_TEST_POSTGRES` / `PROCESSING_TEST_REDIS` 测试环境；不配置 Redis 时这部分会跳过，不能把跳过当作验证完成。本次实际运行并通过。

## 部署与现场结果

2026-09-12 11:58:58（Asia/Shanghai）上线。先临时暂停取新任务，等待有效执行阶段、模型请求和实际队列执行计数均归零，再只更新应用。11 个暂停标记有独立所有权与 15 分钟过期保护，更新完成后全部按所有权校验解除；没有恢复用户暂停的数据源。

- 镜像：`marvel/weknora-app:v0.7.2-local-stage-queue-20260912`。
- 镜像 ID：`sha256:3f0c733723b446629a300a397de879c06ccf1e3e3561f24d06aa4972d5c790ce`。
- 应用二进制 SHA256：`5364e76e76611dabd8eae0490d1bf7061dd693d72949d7abf1aa0fbda99e5719`。
- 已验证源码包 SHA256：`7c9570faca8bc62c868ed5bbab7b7eb146eeb0062f348772697f76e6daf29f05`。
- 应用健康；环境变量完整一致，其他运行容器 ID 均未变化。Compose 配置差异仅为应用镜像。

上线后通过 Grafana 保存的面板和容器日志回读：

| 项目 | 11:58 部署前 | 12:02 回读 |
| --- | ---: | ---: |
| 执行中阶段 | 2 | 14 |
| 模型执行请求 | 0 | 8 |
| 跟踪集合仍停在内容转换 | 79 | 0 |
| 跟踪集合已有当前可用版本 | 0 | 11 |
| 跟踪集合全部后处理完成 | 0 | 4 |

79 份文档均仍可在当前文档视图中定位，已进入图片收集、图片处理、OCR、摘要或 Wiki 等后续阶段；余下任务按原容量继续处理，并非全部完成。两张 GPU 服务在 3 分钟窗口分别产生 755 / 856 行工作日志，向量服务产生 71 行非健康检查日志。12:04 的 Grafana 新采样显示 GPU 0 / GPU 1 利用率为 100% / 95%，模型执行请求为 8。

原有异常仍保留，12:02 时异常/待恢复阶段为 331，但没有阻止这些独立文档推进。诊断现场证据来自 Grafana 与容器日志；部署时 Redis 仅用于暂停/解除取任务及确认执行排空。没有手工删除积压队列或重置业务重试次数。

## 回滚与边界

旧镜像为 `marvel/weknora-app:v0.7.2-export-queue-20260912`，ID `sha256:7d2913769be9375298ac5b31c15d4d2686d324b9a3ed88da9350af8516b21f42`。从主机备份 `results/queue-isolation-20260912/override.before.yml` 恢复应用镜像配置，仅重建 app；无需回退业务数据库或阶段产物。回滚前同样应等待在途请求排空。

旧队列的历史投递可能暂时保留。原生恢复逻辑使用投递序号进行重新投递与去重，旧消息再次消费时会因序号不符被安全忽略。该修复恢复已就绪文档的独立推进，不会使腾讯导出失败、源内容不完整等既有错误自动成功，也不绕过同一文档的必要前置条件。

证据与操作回执：工作区 `results/queue-isolation-20260912/`；主机 `/public/knowledgebase/results/queue-isolation-20260912/`、`/public/knowledgebase/upgrade-tests/queue-isolation-20260912/`。只有受限权限的主机备份保存运行配置，不把凭证或业务文档正文写入本记录。
