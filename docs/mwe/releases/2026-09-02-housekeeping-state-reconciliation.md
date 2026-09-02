# Housekeeping状态收敛修复

日期：2026-09-02

上游版本：WeKnora v0.7.2

目标分支：`weknora-v0.7.2`

## 问题与根因

高负载恢复期间，housekeeping将超过`DocumentProcessTimeout + 10分钟`的记录标记为failed，并同时把`pending_subtasks_count`强制清零。此后仍在运行的延迟子任务即使全部成功，也无法可靠判断自己是不是最后一个任务；`FinalizeSubtask`又只允许从finalizing提升到completed，因此顶层状态永久停留在failed。

生产表现为124份未解决失败，其中122份带有housekeeping错误，但已完成摘要、写入processed_at，并生成1,426个全部ready的分块；只有2份MinerU没有返回可用Markdown或图片，属于真实内容失败。

## 变更范围

- housekeeping标记超时失败时不再清零`pending_subtasks_count`。
- 最后一个延迟子任务将计数降到0后，可以把仅由housekeeping产生的failed状态恢复为completed。
- 通过共享错误后缀严格识别housekeeping状态；普通解析失败、取消和删除状态不能被恢复。
- 成功恢复时清空旧`error_message`并更新`processed_at`。
- 增加housekeeping计数保留、延迟完成恢复和真实失败不恢复三项回归测试。

## 一次性状态校正

仅校正同时满足以下条件的记录：

- 位于受影响知识库`ae42b027-fdec-41cc-a0e8-c64af4698199`；
- 当前状态为failed，错误来自housekeeping；
- `processed_at`存在；
- `summary_status=completed`；
- `pending_subtasks_count=0`；
- 至少有一个有效分块，且所有有效分块`index_status=ready`。

严格预检和更新均命中122份。没有重新解析、删除或改写文档与分块。校正后全局状态为completed 2,959、draft 13、failed 2，housekeeping误判剩余0。

校正前审计快照：

`/public/knowledgebase/results/ingestion-recovery/20260902-housekeeping-reconcile/pre-update.csv`

快照共123行（含表头），SHA256：

`60761a4cf10f5067058feb080e4fa3eb4bc208b41f8dbdc31a3071c04346b52e`

## 验证

- 修复前目标测试稳定失败：housekeeping把计数2清零；延迟完成后状态仍为failed。
- 修复后三项目标测试通过。
- `go test ./internal/application/service ./internal/application/repository -count=1`通过。
- 新应用镜像启动后healthy，重启计数0。
- Postgres、Redis、MinIO、双Q4及MinerU保持运行；未重建这些服务。
- 状态校正后只剩2份真实内容失败，无OOM、Xid、CUDA、NVML、panic或应用异常。

## 生产镜像与备份

应用镜像：

`marvel/weknora-app:v0.7.2-housekeeping-reconcile-20260902`

生产override备份：

`/public/knowledgebase/weknora/override.yml.bak.20260902-housekeeping-reconcile`

## 回滚

将应用镜像恢复为`marvel/weknora-app:v0.7.2-ingestion-resilience-20260901`，然后仅重建app服务。一次性状态校正不应机械回滚：122份记录的分块和索引已验证完整，恢复为failed只会重新制造错误状态。

## 残余风险

- housekeeping仍使用固定超时阈值作为最终兜底；极端长任务仍可能先显示失败，但新逻辑允许延迟子任务完整收敛后自动恢复。
- 队列检查是读取Redis任务状态的瞬时快照；未来若再次出现误判，应同时检查队列扫描日志和处理span心跳。
- 两份真实内容失败仍保留，需由语料所有者检查源文件是否为空、损坏或仅包含MinerU无法导出的对象。
