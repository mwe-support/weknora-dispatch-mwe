# GPU 容器 CDI 与运行期防护

## 发布信息

- 上游版本：WeKnora v0.7.2
- 目标分支：`weknora-v0.7.2`
- 生产标识：marvel-kb GPU Compose CDI migration 2026-09-02

## 根因

marvel-kb 使用 Docker 的 systemd cgroup 驱动和传统 NVIDIA GPU reservation。执行 `systemctl daemon-reload` 后，宿主机 GPU 正常，但所有既有 GPU 容器同时出现 `Failed to initialize NVML: Unknown Error`。这与 NVIDIA Container Toolkit 官方记录的 legacy hook/systemd cgroup 已知问题一致。

## 变更

- GPU Compose 改用 CDI 设备名 `nvidia.com/gpu=0|1`，避免 systemd reload 删除运行中容器的 GPU 设备访问。
- 增加开机一次性整组强制重建，防止 Docker 恢复旧容器状态。
- 增加 MinerU 每分钟健康检查；连续两次 HTTP、NVML 或 CUDA 探针失败后，仅重建 MinerU，并限制为十分钟最多一次。

## 验证

- `docker compose config --quiet`：通过。
- CDI 临时容器在 `systemctl daemon-reload` 前后均可执行 `nvidia-smi -L`。
- 两个脚本 `bash -n`：通过。
- MinerU `--check-only`：HTTP、NVML、CUDA 探针通过后才启用定时器。
- 生产整组重建：六个服务均 healthy；五个 GPU 容器内 `nvidia-smi -L` 均成功，MinerU `torch.cuda.is_available()=true`、`device_count=1`。
- 生产执行 `systemctl daemon-reload` 后重复上述验证，NVML/CUDA 与健康状态保持正常，确认 CDI 修复生效。
- `marvel-kb-gpu-recreate.service` 与 `marvel-kb-mineru-watchdog.timer` 均 enabled；watchdog 首次手工执行 `Result=success`。
- 容器 ID 已全部更新：Q4 GPU0 `4011cc8b→14cd39ef`、Q4 GPU1 `8f32556a→8a86f93b`、Embedding `a2e2b9c9→69b9404b`、Reranker `c8b3800e→9f77754e`、MinerU `94aed3a6→41d6cee1`、Router `704e610f→6e61d23d`。
- 事故失败任务定向恢复：220份基础设施失败分4个租户批量重排，4个批任务均 `submitted` 且 `0 failed`；2份无有效 Markdown/图片的内容失败未重试。审计报告：`/public/knowledgebase/results/ingestion-recovery/20260902-cdi-recovery/requeue-report.json`。

## 部署影响

首次从 legacy reservation 切换到 CDI 必须重建 GPU 容器。本次按用户明确授权在 `processing=0`、`finalizing=61` 时执行；切换完成后再定向重排事故失败任务。

## 回滚

1. 恢复 `/public/knowledgebase/gpu/compose.yml.bak.20260901T2025Z-pre-cdi`。
2. 禁用 `marvel-kb-mineru-watchdog.timer` 和 `marvel-kb-gpu-recreate.service`。
3. 执行 GPU Compose 整组强制重建并确认健康。

## 残余风险

- 显式修改运行中容器的资源限制仍可能破坏设备访问；运维变更后必须重建受影响容器。
- 开机重建会延长 GPU 服务就绪时间，但不会修改模型或业务参数。
