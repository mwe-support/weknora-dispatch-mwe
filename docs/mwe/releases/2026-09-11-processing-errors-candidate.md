# 2026-09-11 分块与本地模型思考参数修复候选

## 范围与发布状态

- 上游：WeKnora v0.7.2；仓库：`mwe-support/weknora-dispatch-mwe`；分支：`weknora-v0.7.2`。
- 基线：`c6a8264e`。本批按“独立分块映射修复”和“本地模型请求参数修复”拆分提交。
- 修改应用分块逻辑与内置模型配置，未涉及 MCP 仓库、数据库迁移或前端。
- **候选已构建，未部署。** 当前生产应用、模型参数、任务状态和已发布内容均未修改。

## 现场分类（21:51，Asia/Shanghai）

| 阶段 / 错误码 | 失败单元 | 涉及文档 | 已发布 | 含义 |
|---|---:|---:|---:|---|
| question / QUESTION_OUTPUT_EMPTY | 1229 | 22 | 22 | 问题增强未完成，正文仍已发布 |
| chunk / CHUNK_PARENT_MAPPING_INVALID | 13 | 13 | 0 | 分块阻塞正文发布 |
| summary / STAGE_EXECUTION_ERROR | 3 | 3 | 3 | 摘要失败，通用错误未说明底层原因 |
| export_poll / TENCENT_404 | 10 | 10 | 0 | 腾讯导出轮询失败，需保留任务身份单独核实 |

以上是采样时状态，不同错误可涉及同一份文档，不能直接把文档数相加。另有身份、源类型和其他腾讯接口错误，本候选不把它们当作已解决。

## A. 独立小段被误判为父子关系错误

`chunker.SplitParentChild` 使用 `ParentIndex=-1` 表示不需要冗余父块的独立叶子。在同一文档同时存在长段落和短段落时，旧生命周期实现只看到父块数组非空，就要求所有子段都有非负父索引，因而错误拒绝合法短段落。

修复仅允许这个已有、明确的 `-1` 标记；其他负数和越界索引仍被拒绝。不删除校验、不丢段落、不关闭父子分块。回归用一份短标题段加长正文的合成文档，检查分块总数、每段正文和有父块/无父块关系。

- 修复前：`TestProcessingChunksAcceptMixedStandaloneAndParentedText` 复现 `CHUNK_PARENT_MAPPING_INVALID`。
- 修复后：同一检查通过；子段内容及父块引用均符合分块器返回契约。

## B. 关闭思考的调用选项没有到达 llama.cpp

失败任务使用内置 `builtin-local-qwythos-q4-mm`。其 provider 配置为 `openai`，没有 `extra_config.thinking_control`；对应适配器不传思考控制字段。尽管问题和摘要调用设置 `Thinking=false`，llama.cpp 仍执行默认思考。

用同一个失败问题单元的当前输入、同一个本地模型、相同 512-token 预算重放，仅变更思考参数；源内容和输出正文仅在受信任主机进程中处理，没有写入本记录：

| 请求 | 正文字符数 | 思考字符数 | finish_reason | 输出 token |
|---|---:|---:|---|---:|
| 当前 provider 请求 | 0 | 1828 | length | 512 |
| 显式 `chat_template_kwargs.enable_thinking=false` | 81 | 0 | stop | 45 |

候选在该内置聊天模型的 YAML 中补上既有 `thinking_control: chat_template_kwargs`，复用现有适配器，不增加新供应商分支或增大 token 上限。请求层回归直接读取部署 YAML，检查非流式请求的显式 false/true 均正确传到 HTTP body；不强制关闭用户明确启用的思考。

## C. 通用摘要错误的证据边界

“本阶段未产出有效结果”是 `STAGE_EXECUTION_ERROR` 的通用展示文案，不能据此断言是模型、存储或内容问题。三个历史摘要失败记录只保留这条脱敏通用消息。本轮限定时间窗口的应用日志查询未取得对应具体错误，因此不把问题生成的已证实根因直接当作这三个摘要失败的根因，也不把它们标记恢复。

上线恢复时，应逐个取得新的真实执行结果并记录可安全展示的具体错误码。保持历史事件原样，不回填猜测的错误原因。

## 验证与候选标识

- 测试命令：`go test ./internal/application/service ./internal/models/chat ./internal/infrastructure/chunker -run 'TestProcessingChunksAcceptMixedStandaloneAndParentedText|TestBuiltinLocalChatThinkingControlReachesWire|TestBuildOutbound_Thinking|TestSplitParentChild' -count=1`。
- 在无外网的既有 Go 构建容器中通过；三个包分别耗时 0.773、0.675、0.672 秒。
- 测试及构建源包 SHA-256：`59975daf63a81be8d459c661c7023d0e99b05cf5aa074762b002b33b01b44842`。
- 候选镜像：`local/weknora-app:processing-errors-59975daf63a81be8`。
- 候选镜像 ID：`sha256:7f60f17c1292b2b27472d740d77b84fdd82134396bf845436b3be801c1ee02e1`。
- 二进制 SHA-256：`fbd7546be2ba464cd6ea434917ef35f4d92c47fed79e4249adb8814a7ef20331`。
- 基于当前生产镜像 `marvel/weknora-app:v0.7.2-processing-lifecycle-20260911-minio`，ID `sha256:1a0c73fb3b6c8af31c603f462f200eb0924ae6c844c6af99bf3d0161cae30555`；仅替换二进制和内置模型 YAML。
- 镜像构建完成不等于已通过完整应用启动或生产回归；本候选尚未执行生产切换和失败任务恢复。
- 本地证据：`results/processing-errors-20260911/`；远端只读诊断与候选记录：`/public/knowledgebase/upgrade-tests/processing-errors-20260911/`。业务正文、请求密钥及模型思考正文不进入提交记录。

## 上线前必须处理的迁移范围

22:08:22 的只读快照显示，受此模型配置约束的当前作业为 259 个：247 个文档作业、12 个扫描作业，涉及 12 个数据源；22 份已有发布版本。没有已记录的 uncertain 单元，但 103 个附件/导出快照作业尚未具备完整源读取、导出、轮询及下载确认产物。

`lockProcessingJob` 会把模型参数纳入配置摘要，每次提交都核对 `ConfigurationRevision`。直接修改模型参数会导致旧作业校验不匹配，普通 retry 不能解决。`RebuildProcessingVersion` 又要求附件作业具有完整已验证源快照，因此不能把 259 个作业一律批量 rebuild，也不能覆盖旧配置摘要绕过校验。

上线顺序应为：

1. 重新读取影响范围并冻结作业/修订清单，保存当前模型行和发布指针，核实运行中的非幂等导出操作，安排工作进程交接。
2. 在隔离数据库验证候选启动、内置模型配置同步以及既有配置变化的恢复流程；保留当前生产镜像作回滚目标。
3. 对已有完整快照的文档使用既有新代次重建入口，复用输入摘要一致的读取、解析和索引产物，保留旧发布版本直到新版本就绪。
4. 对未具备完整附件快照、尚未完成的源扫描分别制定续接方式；先核对既有导出任务，不能盲目重发导出或全库重抓。
5. 分批恢复并核验正文可用、问题/摘要产物与索引真实完成；以新事件解决旧异常，不删除历史失败记录。

此处记录的是已核实的上线约束与候选恢复顺序，不是已经执行或已经通过演练的迁移。

## 回滚与残余风险

当前未部署，因此无需生产回滚。代码可按独立提交回退。以后若部署，应用镜像与内置模型配置须成对恢复；若已创建新代次，不能只把模型行改回去而不处理其配置版本关系。回滚应保留原有发布指针、资源和事件，不删除已完成业务数据。

分块修复已用真实分块器回归验证；模型关闭思考已用真实失败片段验证。历史摘要的具体原因、腾讯导出 404、既有未完成作业迁移和完整应用启动仍未验证，不能宣称整条生产链路已经恢复。
