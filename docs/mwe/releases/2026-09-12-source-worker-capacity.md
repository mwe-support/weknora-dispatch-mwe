# 源端维护池容量调整与试运行边界

- 上游 WeKnora v0.7.2；仓库 `mwe-support/weknora-dispatch-mwe`，目标分支 `weknora-v0.7.2`。
- 发布标识 `source-capacity-20260912`；最终提交 `ops(mwe): set maintenance workers to four after capacity trial`。
- 最终配置：`WEKNORA_ASYNQ_MAINTENANCE_CONCURRENCY=4`，从生产原值 2 增加到 4。其余池不变，总工作名额从 18 增至 20。

## 动机和实际变更

扫描、内容读取、导出、下载和清理共享维护池。原生产环境只有 2 个名额，修复阶段派发空档后，源端仍有长时间排队。采用已有配置项调整容量，不修改调度代码、任务依赖或模型调用上限。

`deploy/mwe/source-worker-capacity.override.yml` 保存不含凭证的最终配置。生产将该环境变量覆盖合入 `/public/knowledgebase/weknora/override.yml`；在 Compose 中亦可将此配置文件作为最后一个 override 使用。没有覆盖上游代码默认值。

## 试运行与决策

1. 2026-09-12 14:02:50（Asia/Shanghai）试增至 8；启动日志确认并发值生效。
2. 试运行出现约 30 秒的源端调用耗时、更多临时失败和导出结果不确定。未将“运行名额增加”当作性能优化成功。
3. 14:09:20 降至上游默认容量 4，并保留该配置。启动日志明确报告 `maintenance-pool server starting with concurrency=4`。

代码仍按同一凭据约两秒发起一个请求，并保留 429/503 冷却。HTTP 调用的超时包含本地等待时间；扩大名额不能直接消除这项限制。试运行观察与这些限制相符，但没有逐请求网络追踪，不能把所有失败都确定归因于同一种原因。

初期可配对事件样本：原配置下下载排队平均 423.479 秒；4 名额初期从约 52 秒变为约 84 秒（后一次窗口仅 2 个新派发样本）。阶段执行仍有等待，且前后窗口、文档类型和积压状态不同，不能据此宣称总吞吐提高若干倍或 GPU 将持续满载。

## 试运行异常的处理

8 名额试运行窗口为 14:02:50—14:09:20，其中出现 11 条 `EXPORT_START_UNCERTAIN` 提交。旧配置对照窗口中也有链接过期和少量临时失败；不能把试运行期间的全部错误都当作本次新引入的问题。

针对上述 11 条，检查锁定的当前尝试与导出意图检查点：10 条没有导出意图记录。运行代码必须成功持久化意图后才调用 StartExport，因此这 10 条可证明尚未发出该尝试的导出请求。

通过现有 `ProcessingRepository.ResolveExport` 的 `NotStarted` 分支恢复这 10 条：先逐项事务回滚验证，再正式执行；审计 actor 为 `mwe-source-capacity-20260912`，证据引用 `audit:source-capacity-20260912/no-intent-check`。只处理该时间窗口、该尝试且仍处于不确定状态的记录，重读状态并锁定 job；不删除历史、不直接修改队列消息、不处理大小超限任务。

14:27 回读：11 条中 8 条导出启动阶段成功，3 条仍阻塞。其中 1 条从未重发；另 2 条是在有明确未发出证明后重新安排、再次失败的记录。后续检查显示剩余 3 条中没有已知 provider task ID；1 条保留明确 NotSent 证明，另外 2 条不能排除请求已发出。未继续循环重试，也未把“导出启动成功”当作整份文档同步完成。

这 3 条及相关 IDs 保留于本地/主机 `recovery-final.json`，需要继续核实源端失败或腾讯回执，不能盲目再次导出。本次容量调整并未完成所有历史错误修复。

## 验证、发布和回滚

- 两次变更均先暂停取新任务，等待执行队列、有效租约及模型请求排空，再仅重建 app；每次 11 个本次拥有的暂停标记均已解除，带 15 分钟自动过期保护。
- Compose 标准化配置对比确认每次只改变这一环境变量；实际容器环境与预期一致，其他运行容器 ID 不变，应用健康。
- 镜像未变：`marvel/weknora-app:v0.7.2-immediate-dispatch-20260912`，ID `sha256:33dfe00c8445080e94f8237ddbdd5b3f349e5187e40055cbab21b1ad4a6a28dd`。
- 模型上限、腾讯请求预算、大小超限不自动重试规则和用户暂停的数据源没有被主动调整。
- 回滚到原值 2：恢复 `/public/knowledgebase/results/source-capacity-20260912/override.before.yml` 后仅重建 app。注意 `source-capacity-4-20260912/override.before.yml` 是 8 名额试运行的备份，不能误用它回滚到 2。
- 回滚配置不能撤销已经完成的处理或证明一个不确定请求未发出；保留原有执行记录与去重规则。

证据：工作区 `results/source-capacity-20260912/`；主机 `/public/knowledgebase/results/source-capacity-20260912/`、`/public/knowledgebase/results/source-capacity-4-20260912/`。运行配置备份仅留在主机并限制文件权限，不提交凭证或真实业务文档正文。
