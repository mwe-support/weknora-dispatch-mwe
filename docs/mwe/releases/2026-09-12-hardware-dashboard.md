# 硬件看板时间与采集范围修复

- 上游：WeKnora v0.7.2；仓库 `mwe-support/weknora-dispatch-mwe`，目标分支 `weknora-v0.7.2`。仅监控部署、看板与验证脚本，无 MCP 专属修改。
- 用户报告硬件看板与实际不符，并要求 GPU、CPU、内存、网络和磁盘全部检查。

## 根因与分项变更

1. `fix(observability): expire stale GPU snapshots and expose sample time`：探针异常时原实现继续返回旧缓存，Prometheus 的新抓取时间会掩盖旧硬件读数。增加每张 GPU 的真实采样时间；超过采样有效期不再输出旧值，配套缺采样告警。默认约 30 秒采样，默认缓存有效期 90 秒。
2. `fix(observability): collect network counters from the host namespace`：挂载宿主 `/proc` 后，`/proc/net` 仍指向采集进程的网络命名空间。原看板读取 `eth0/eth1/eth2`，实际主机接口为 `eno1np0/eno2np1/wg-kb`。复用原生 Unix exporter，仅为 netdev 使用 `/host/proc/1`，独立标记 `integrations/host_network`；原 exporter 禁用 netdev，未增加容器、端口或网络权限。
3. `fix(grafana): preserve rolling time when switching log streams`：日志来源链接用 `__from/__to` 拼接固定时间，导致“最近 6 小时”点击后被冻结。用户现场结束时间为 09:06:15，GPU 显示 0%、约 29°C，而 09:30 主机已高负载运行。改为已有 Business Text 插件与 Grafana 原生变量更新，只改变日志来源，保留相对/历史时间、刷新周期及分页。增加返回当前时间按钮、采样时间和统计口径说明，去掉误导性的“实时”名称；无 Swap 的公式由错误的 100% 修正为 0%。渲染生成器保留新增硬件面板。

## 实测结果

| 项目 | 主机与采集链路核验 |
| --- | --- |
| CPU | 64 个逻辑核；每核 idle 计数落在主机读取的前后界限内。曲线为非空闲率的 5 分钟平均，包含 I/O 等待。 |
| 内存 | 总量 134,909,018,112 字节，约 125.64 GiB，与 `/proc/meminfo` 完全一致；可用内存差异小于总量的 1%。 |
| GPU | 两张 RTX 3090 的物理索引、UUID、24 GiB 容量与主机 nvidia-smi 一致；核验时采样年龄约 5 秒。动态占用、温度与功耗应按采样时刻比较。 |
| 网络 | 原始复现检查从不匹配变为匹配；主机 PID 1 视图的收发计数位于 `/proc/net/dev` 前后读取界限内。按接口分别展示，避免物理网卡与 VPN 流量相加。 |
| 磁盘 | md0、nvme0n1、nvme1n1、sda 的读写字节计数与 `/proc/diskstats` 对应；根文件系统容量 490,050,658,304 字节与 statvfs 一致。I/O 吞吐不等于已用空间。 |

Grafana 原生数据源接口的 17 条硬件查询全部成功并返回样本。正式页面已显示主机接口、GPU 采样时间及当前窗口；日志切换保留 `now-6h/to=now`、5 秒刷新和分页。固定历史范围也保持原来的时间点（Grafana 将毫秒参数规范化为等价 ISO 时间）。临时验证看板和测试容器已清理。

## 验证命令与证据

- `node deploy/mwe-observability/scripts/test-hardware-navigation.mjs`：相对/历史时间及无关参数保留；旧看板失败，新看板通过。
- `python scripts/render-processing-observability.py --check`：生成物一致，新增说明与采样时间面板不会被重新生成时删除。
- `PROBE_SELF_TEST=1 python3 deploy/mwe-observability/capability-probe/probe.py`：过期缓存不再返回 GPU 值。对照旧版，600 秒前的缓存会被继续返回；修复后不会。
- 在主机执行 `python3 verify-host-metrics.py --gpu`：CPU、内存、网卡、磁盘与 GPU 身份/容量检查全部通过；探针和 Prometheus 链路另有 Grafana 17 查询回读。
- 独立 Alloy 容器验证 PID 1 网络视图；`alloy validate` 与 `promtool check rules` 通过。PromQL 零 Swap 对照：旧公式 100，新公式 0，未改动主机 Swap 配置。
- 本地证据目录：`results/hardware-dashboard-20260912/`；部署主机备份与执行回执：`/public/knowledgebase/results/hardware-dashboard-20260912/`。

## 部署、回滚及边界

2026-09-12 09:58:57（Asia/Shanghai）上线。探针镜像 `local/mwe-kb-capability-probe:hardware-20260912`，ID `sha256:70dd0cbd6cf5de4ee97bbe73749d244c38e2f947bb82db98f5fcc89420b9f428`；运行别名仍为原有 `v1`。Alloy、Prometheus 和 Grafana 原生热加载，只重建探针，应用、数据库、MCP 和模型容器 ID/启动时间均保持不变。

回滚：从部署备份的 `files/` 恢复四份配置，恢复旧探针镜像 ID `sha256:48e770de0a3c865e76a2d6a8c7c9df4047d185ed39b517d803e41ad5caf69504` 到 `local/mwe-kb-capability-probe:v1` 并仅重建探针，再执行三个监控组件的原生 reload。单文件挂载配置使用原位写入，不用重命名替换挂载 inode。

采样仍有正常延迟，5 分钟平均与瞬时命令输出不要求逐点相等。修复前的容器网络历史仍保存在旧序列中，新图仅使用正确的主机网络序列，不伪造过去的主机数据。说明、README 和验证脚本属于本批记录的一部分。

原生采集器参数依据 [Grafana Alloy 文档](https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.exporter.unix/)；本机复现也符合 [上游容器网络采集问题](https://github.com/grafana/alloy/issues/1933)，最终结论以本机差分验证为准。
