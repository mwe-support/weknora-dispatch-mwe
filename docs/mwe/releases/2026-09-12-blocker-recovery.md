# DOCX、分块、导出回执和模型输出恢复

- 上游：Tencent WeKnora v0.7.2。
- 目标：`mwe-support/weknora-dispatch-mwe`，`weknora-v0.7.2`；无 MCP 专属修改。
- 授权范围：修复 DOCX 解析与覆盖比对，恢复已修复的 30 份分块任务，随后处理导出回执和模型空输出；失败达到重试上限后继续独立任务。

## 分批提交范围与根因

1. `fix(docreader): preserve DOCX stories and original media`：原转换链遗漏页眉页脚、文本框及部分媒体；Go 覆盖检查未解码 Markdown 对标点的转义。使用标准库读取 OOXML 中遗漏的原文段落和媒体，保留原图片字节及重复数量。只解码输出中的一层 Markdown 转义，原文反斜杠仍须匹配，不放宽原文/图片覆盖要求。
2. `fix(models): retry empty output with effective thinking controls`：旧内置聊天配置未传递思考开关，通用视觉请求未带 `chat_template_kwargs.enable_thinking=false`。在运行时修复映射，尊重显式覆盖，不修改模型数据库配置。摘要和图片空输出使用 `MODEL_OUTPUT_EMPTY`，走现有有上限的退避重试；超大输出、权限、源身份和导出结果不明不纳入此策略。
3. `feat(processing): recover failed stages and expired export receipts`：增加一次性恢复 CLI，复用 `RetryStep`；固定操作编号保证重复执行不追加额度。导出重启仅处理已获得任务编号、收到不可用回执且未保存下载快照的任务，保留旧回执和产物引用，重新验证源身份；拒绝结果不明请求、已有下载快照、已开始解析、错误租户及过期修订号。保留已被问题关闭策略跳过的投影步骤。各任务独立提交，单个失败不阻塞其他候选。
4. `fix(processing): keep export receipts out of the scan queue`：恢复后的 38 个导出请求在 `sync` 队列中持续等待大量扫描。将启动、轮询、下载路由到 `sync_export`，沿用 maintenance 工作池及上游容量限制，以权重 3 与扫描权重 2 分配领取机会。旧投递仍可读取；恢复器会为新队列中缺失的投递增加派发序号，旧序号消息无法重复取得执行租约，不消耗业务重试额度。
5. `fix(processing): split long image text without truncation`：一条恢复后的 OCR 为 39,962 字符，触及向量化的 20,000 字符安全上限。将 OCR/描述按最多 4,096 个 Unicode 字符分段，保留完整文字、图片关联与段序号；对该失败文档使用原生新代次重建，复用认证过的原始快照和模型结果，重新生成图片分块及索引计划。

分块父子映射修复已经包含在前一批提交 `f03ddaa0`；本批只恢复其历史失败步骤。

## 验证

- Python 标准库 `unittest`：原文段落、Markdown 转义、页脚重复、文本框、图片原字节与重复数量、重复调用幂等、SVG 栅格化尺寸/像素及外部引用拒绝；3 项通过。
- 生产保存的 51 份失败 DOCX，只读、无网络的独立解析容器复测：文本缺失 0；原始有效图片 139，解析图片 139；解析错误 0。真实业务内容未落入发布记录。
- Go：模型配置映射与 HTTP 请求字段；DOCX 覆盖；混合父子分块；空输出重试分类；重试耗尽后独立图片步骤和另一个文档继续；导出恢复幂等与拒绝条件。
- PostgreSQL：独立合成数据库的并发代次、租约和事务失败测试通过。
- SVG 真实文件补测：一份文档含 31,688 × 13,889 坐标范围的 SVG 基本路径图形，复用既有 ImageMagick 转为 4,096 × 1,795 PNG。原 SVG 保留在加密的 DOCX 快照中；转换前限制资源引用与尺寸，PNG 去除时间戳保证重复解析幂等。补丁上线后该文档解析成功。
- 队列注册、旧队列兼容、导出路由及工作池容量测试通过；长 OCR 分段重组与 Unicode 完整性测试通过。既有完整处理流程与重建复用回归通过（含 48 图片、多批次、索引失败重试等场景）。
- 验证命令：`go test ./cmd/processing-recover ./internal/models/chat ./internal/models/vlm ./internal/application/repository ./internal/application/service ./internal/datasource/connector/tencentdocs -run 'TestConfigFromModel|TestBuiltinThinking|TestRemoteVLM|TestNativeFailure|TestProcessingExhaustedRetryDoesNotBlock|TestProcessingDOCXCoverage|TestProcessingChunksAcceptMixed|TestProcessingRestartExport|TestProcessingPostgresConcurrent' -count=1`。生产凭据只在主机进程环境中，测试数据库使用合成数据。
- 队列/分段回归命令：`go test ./cmd/processing-recover ./internal/types ./internal/application/repository ./internal/application/service ./internal/router -run 'TestExportReceipts|TestQueue|TestEveryAsynq|TestProcessingImageTextParts|TestProcessing.*(Dispatch|Delivery|Redeliver|Exhausted|RestartExport|Rebuild|Pipeline)' -count=1`。Python：`python -m unittest discover -s /tests -p test_docx_text.py -v`，在无网络、只读文件系统的独立解析容器中执行。

## 部署与恢复

- 第一批应用：`marvel/weknora-app:v0.7.2-blocker-repair-20260912`，镜像 `sha256:e887ef10987257e48b6a0ae47e265f187be8c4d5fd4b9ffcd70393c5c18c6e63`。
- 最终 DOCX/SVG 解析器：`marvel/weknora-docreader:v0.7.2-docx-svg-20260912`，镜像 `sha256:856c8e30a51df2ab05226539e7ce73bfa2815e2dbbec31a11251ea0bdbabb3ae`。
- 2026-09-12 02:46（Asia/Shanghai）恢复 81 个文档步骤、38 个过期导出、63 个模型步骤；随后补恢复 1 个 SVG 解析步骤。03:00 实际回读：51/51 DOCX 解析、30/30 原失败分块、63/63 原失败模型步骤已成功；81 份 DOCX/分块对应文档均已发布。一条 OCR 的 `UPSTREAM_TEMPORARY` 事件进入自动退避，重试 1 次后成功；空输出分类由回归测试独立验证。
- 109/109 知识库仍保持问题生成关闭/数量 0；数据源配置、模型非密钥参数及应用环境保持一致。应用启动刷新了内置模型密钥的加密封装，因此密文行哈希会改变；不能将密文哈希不一致解读为模型参数语义变化。
- 审计与备份位于部署主机 `/public/knowledgebase/results/blocker-recovery-20260912/`，目录权限 0700，含原 override、恢复回执、投递编号和数据库备份。第一批备份 SHA-256：`a69736e5b25590200b95a383e00b50484e3c266edaebf74a45e735fbe2447984`。
- 最终应用：`marvel/weknora-app:v0.7.2-export-queue-20260912`，镜像 `sha256:7d2913769be9375298ac5b31c15d4d2686d324b9a3ed88da9350af8516b21f42`；二进制 SHA-256：`b0438f2d1fba7548397e497b461f09488b63276a1668e87411f20aad31d6d242`。03:22 上线，未改变工作池/模型容量配置。
- 已逐一比较 Git 提交 `5fb743e9b3ac9c347d0f10e12ee64c5901f24481` 涉及的 21 个代码/测试文件与生产构建源，标准化换行后全部一致。构建源归档 SHA-256：`afd70d0dcdc11a6144551c0df03f655c02bc792abb324d849010b2d910c6e325`。
- 为验证这批恢复，在原有队列中对该批次已确认待执行的快照读取、导出结果和本地规范化步骤做了一次性优先排序。原子检查任务仍为 pending 后只移动其列表位置，任务编号、内容、派发/重试计数及其他任务均保留；每个投递编号仅优先一次，操作写入 `recovery-priority.json`。
- 03:40 长 OCR 新代次已完整成功并发布。按图片资产和类型重组后，138 组文字哈希全部一致，重建前后均为 133,752 字符，最大分段 4,096 字符；源快照、模型结果和旧代次均保留。两个数据库备份的归档目录均已用 `pg_restore --list` 验证可读。
- 03:55 最终故障步骤核验：38/38 导出启动、38/38 结果轮询、38/38 下载均成功；全部保留在第 2 次业务尝试，队列切换仅增加派发序号。81 个 DOCX/分块文档及 30 个原模型文档、1 个长 OCR 重建文档已完整成功；导出批次当时 32 份完整成功、5 份已发布并收尾、1 份继续正常处理，本批没有 failed/blocked 步骤。原故障恢复已完成，正常后续步骤仍由原生工作器处理。
- 03:57 补充核验：相关 150 份文档均已发布；导出批次尚有 6 份正常后置任务执行中，本批原失败步骤仍全部成功、无新增失败。本次补充仅记录执行结果，不包含新的代码或部署变更。

## 回滚与剩余边界

- 部署前暂停领取同步任务，等待在途导出请求完成，优雅停止应用并备份处理账本及相关表。
- 回滚使用部署审计目录的 `override.before.yml` 恢复原应用/解析器镜像，仅重建这两个服务。已提交的恢复操作和已完成的新产物保留，不通过整库还原覆盖后续业务。
- 每次明确恢复给予原生 4 次自动重试和 24 小时执行预算，保留累计次数及历史。重试耗尽后保持失败记录并释放执行位置；依赖该失败结果的步骤仍不能被错误标记为完成，其他文档和独立步骤继续。
- 页眉页脚及文本框补入 Markdown 主体之后，保留段内文本和原故事顺序，不承诺复刻 Word 空间布局。
- SVG 支持自包含基本图形，外部资源、脚本及超出白名单的 SVG 元素保持明确失败；原始 SVG 留在源快照。长 OCR 采用字符边界分段，未增加语义重叠。
- 新导出仍可能受上游任务有效期、权限或源版本变化影响。未知请求不会自动重发；不支持的格式和身份错误仍需单独处理。
- 模型可能继续返回空文本，有限重试不能保证上游成功；完成状态必须以持久化结果为准。
