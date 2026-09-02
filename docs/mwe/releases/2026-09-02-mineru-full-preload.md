# MinerU GPU全量预加载

日期：2026-09-02

上游版本：WeKnora v0.7.2

目标分支：`weknora-v0.7.2`

## 变更动机

GPU0同时承载Q4和MinerU，但MinerU原配置关闭启动预加载且只允许使用18%的GPU内存。空闲时MinerU仅占用约1.2GB，首次解析需要临时加载VLM，增加冷启动等待并降低突发任务的处理稳定性。

## 变更范围

- `--enable-vlm-preload`：`false`调整为`true`。
- `--gpu-memory-utilization`：`0.18`调整为`0.30`。
- 继续保持`--max-model-len 4096`和API并发2，不扩大业务并发。
- 仅重建`kb-mineru-api`，不重建Q4、Embedding、Reranker或WeKnora应用。

## 用户与部署影响

- MinerU在容器启动时完整加载`MinerU2.5-Pro-2605-1.2B`，避免首个解析请求承担模型初始化。
- GPU0稳定显存由约10.6GB增至约16.8GB，剩余约7.3GB。
- vLLM为4096 Token请求建立约4.0GiB KV Cache；日志报告理论最大并发85.25，但MinerU API仍限制并发为2。
- 容器启动预加载约72.79秒，在完成前健康状态为`starting`。

## 验证

- 生产Compose：`docker compose config --quiet`通过。
- MinerU VLM架构：`Qwen2VLForConditionalGeneration`，BF16模型权重约2.16GiB。
- vLLM编译、KV Cache初始化、CUDA Graph捕获及预热全部完成。
- 日志出现`vllm-async-engine init successfully`和`Application startup complete`。
- `kb-mineru-api`状态为healthy，重启计数0，`/health`返回200。
- GPU0稳定状态约16.8GB已用、7.3GB可用；无OOM、Xid、CUDA或NVML错误。

## 生产配置与备份

生产配置：`/public/knowledgebase/gpu/compose.yml`

备份：`/public/knowledgebase/gpu/compose.yml.bak.20260902-mineru-full-preload`

## 回滚

将`--enable-vlm-preload`恢复为`false`，将`--gpu-memory-utilization`恢复为`0.18`，验证Compose后仅重建`mineru-api`服务。

## 残余风险

- `gpu-memory-utilization=0.30`限制的是MinerU内部vLLM内存池，不代表GPU0整体占用30%。
- GPU0仍与Q4共享计算资源；两者同时高负载时可能互相影响吞吐，但当前显存余量足以覆盖已观察到的峰值。
- 后续若提高MinerU并发或上下文长度，需要重新进行Q4与MinerU并发压力测试，不能继续只按显存空闲值上调。
