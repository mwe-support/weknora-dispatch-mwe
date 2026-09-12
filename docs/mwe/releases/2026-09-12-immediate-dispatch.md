# 消除阶段完成后的固定派发等待

- 上游 WeKnora v0.7.2；目标仓库 `mwe-support/weknora-dispatch-mwe`，分支 `weknora-v0.7.2`。
- 本批提交：`perf(processing): dispatch committed successors independently of recovery`。只改变正常派发时机与循环隔离，没有 MCP Server 专属改动。

## 根因与实现

旧 `ProcessingService.Run` 每轮先恢复检查最多 100 个 job，再派发 outbox，并受 5 秒 ticker 控制。阶段已经提交成功、后继已经具备执行条件，仍可能等下一轮；恢复检查也能延迟正常派发。部署前 10 分钟真实事件样本中，分块的派发等待平均 4.382 秒、实际执行约 0.067 秒；Embedding 分别为 4.479 / 0.137 秒。

修改为独立的派发循环：成功完成事务后发送容量为 1 的合并唤醒信号；正常派发不等待恢复巡检。持久化 outbox 仍是唯一依据，提交失败不会触发成功唤醒。1 秒兜底轮询覆盖其他入口/实例、重启及合并通知；成功派发满 100 条后立即尝试下一批，错误时交回兜底轮询，不形成失败热循环。恢复巡检继续使用原来的游标和 5 秒周期。

没有修改阶段依赖、版本/租约校验、业务重试次数、模型并发、腾讯接口预算或已成功产物。没有把 CPU、本地 I/O 与模型任务合并为一个绕过队列隔离的执行器。此改动消除一类确定的派发空档，不保证 GPU 始终满载；源端输入速度仍需单独评估。

## 验证

- 新测试 `TestProcessingCompletionDispatchDoesNotWaitForRecoveryScan` 使用隔离 PostgreSQL，故意卡住恢复查询，同时提交正常阶段：旧代码在 800 毫秒内无法派发后继，修复后约 24—25 毫秒派发；测试同时确认无本地通知的外部生产者可由兜底轮询送出，循环能随 context 结束。
- 真实 Redis 重投递、队列迁移旧消息校验、源端占用时独立文档推进、单一业务重试所有者、耗尽重试不阻塞独立任务等检查通过。
- `go test -race ./internal/application/service -run TestProcessingCompletionDispatchDoesNotWaitForRecoveryScan -count=1 -timeout=90s`：通过。
- 修改的 Go 文件已格式化，`git diff --check` 通过。测试数据库与 Redis 已恢复原先 exited 状态；未对生产表执行测试写入。

## 发布

2026-09-12 13:05:37（Asia/Shanghai）上线。暂停取新任务，等待有效执行租约、实际队列执行数和模型请求归零后，只更新应用。11 个本次拥有的临时暂停标记均已解除；暂停具有 15 分钟过期保护。应用健康、环境变量完整一致，其他运行容器 ID 保持不变。

- 新镜像 `marvel/weknora-app:v0.7.2-immediate-dispatch-20260912`。
- 镜像 ID `sha256:33dfe00c8445080e94f8237ddbdd5b3f349e5187e40055cbab21b1ad4a6a28dd`。
- 二进制 SHA256 `33df7616c49b48e3e583e28d146655af188a839045fcfac4bf7adc444920781a`。
- 已验证源码包 SHA256 `2d51ecc076434081dff843d513622135e6f3b61862e5d3c1837f61cd9089e926`。

13:08:42 的 Grafana 事件回读，仅取部署后新调度且有投递确认的样本：

| 阶段 | 部署前平均派发等待 | 部署后平均派发等待 | 部署后样本数 |
| --- | ---: | ---: | ---: |
| 分块 | 4.382 秒 | 0.008 秒 | 4 |
| Embedding | 4.479 秒 | 0.012 秒 | 10 |
| 索引写入 | 4.420 秒 | 0.009 秒 | 10 |
| 图片下载 | 4.441 秒 | 0.015 秒 | 10 |
| Summary | 4.608 秒 | 0.014 秒 | 2 |

这里比较的是阶段已经调度到投递确认的等待，不是整份文档总耗时。部署后样本窗口较短，不能当作完整新旧架构基准。后续工作池排队和源端耗时仍存在：该窗口中导出/下载的可配对样本仍有约 140—146 秒队列等待，不能用本次毫秒级派发改善掩盖这项瓶颈。

## 回滚和证据

恢复主机 `/public/knowledgebase/results/dispatch-latency-20260912/override.before.yml`，仅重建 app，即回到 `marvel/weknora-app:v0.7.2-local-stage-queue-20260912`（镜像 ID `sha256:3f0c733723b446629a300a397de879c06ccf1e3e3561f24d06aa4972d5c790ce`）。回滚前同样应等待执行排空。没有数据库结构变更，无需回退业务数据；原 outbox 与去重规则兼容。

本地证据 `results/dispatch-latency-20260912/`；主机证据 `/public/knowledgebase/results/dispatch-latency-20260912/`；测试/构建回执 `/public/knowledgebase/upgrade-tests/dispatch-latency-20260912/`。延迟通过 Grafana 生命周期事件查询，以相同 step/attempt/dispatch 对齐时间，只使用具备对应事件的样本；跨窗口的缺失值不记为零。
