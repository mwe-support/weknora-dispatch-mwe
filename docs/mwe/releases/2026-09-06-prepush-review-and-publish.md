# WeKnora v0.7.2 提交前复审与推送批次

- 上游版本：WeKnora v0.7.2；基点 `66bd6afd8bd6aaa78ce4b04c915167b25a748d45`。
- 目标仓库：`https://github.com/mwe-support/weknora-dispatch-mwe`；目标分支：`weknora-v0.7.2`。
- 用户授权：提交前复审，修复所有 P0/P1 后提交并推送。
- 范围：主项目内的数据源连接器、摄取/检索、FAQ、认证、前端、部署模板及发布记录。没有 MCP Server/传输/租户路由的独立服务修改，未向官方上游或 MCP 定制仓库推送。
- 生产镜像仍为 `marvel/weknora-app:v0.7.2-sync-safe-reset-20260906` 和同标识 UI；本批复审追加修复是源码发布，**尚未重新部署**，无数据库迁移。

## 复审结果与修正

对完整工作区差异及新增源码进行两路独立审查：实现/安全与需求符合性。首轮发现下列四类 P1；修正并复查后，两路均未发现剩余明确 P0/P1。结论限于已审查的具体代码与验证，不是无缺陷保证。

1. **未入队子任务被算成完成**：原完成计数回收没有保存失败，可能让候选完成并删除旧版。现在对 summary/question/graph 的实际入队失败先保存失败标记，再释放空缺计数；图谱未启用的正常跳过不标为失败。
2. **图片失败或提前 fan-in 导致错误发布**：现在图片读取、OCR、描述和索引故障均写入源版本失败标记；终失败先写标记再参与完成计数。候选在任何状态都拒绝失败标记；图片完成后若存在失败，将候选置为 failed，继续保留旧版。图片 pending/done 计数按解析轮次隔离，重复投递只计一次，丢失/异常计数不再提前触发发布；初始化失败停止派发。候选发布后的后处理任务和图片后处理均绑定真实解析轮次，旧轮次不能放行新轮次。
3. **FAQ 在慢请求期间失去范围约束**：获取完成、入库前、慢索引后的正式发布及成功游标确认前均复查源范围/暂停状态；新增批次若跨越配置变化，清理该批新增记录与向量；已完成的其他批次保留。暂停错误不进入自动任务重试，避免重试把用户后来暂停误认为最初允许的暂停状态。
4. **Compose SMTP 项缩进错误**：密码行可能被 YAML 合入用户名。现已对齐列表项，并用独立合成环境验证专用密码和 `SYSTEM_EMAIL_PASSWORD` 回退分别形成正确的用户名/密码字段。

新增回归集中在 `internal/application/service/datasource_prepush_safety_test.go`，复用已有测试支撑，未增加依赖。

## 提交划分

代码按五个可独立审阅的功能提交划分，随后提交本批汇总记录：

1. `feat(auth): add one-time email password reset and revoke sessions`
2. `fix(tencentdocs): use bounded minute-scale retries and export budgets`
3. `feat(tencentdocs): preserve selected source folders and retry paths`
4. `feat(datasource): validate and synchronously import Tencent FAQ sources`
5. `fix(datasource): publish completed source versions before retiring old content`

| 提交 | 范围 |
| --- | --- |
| `89c8e920` | feat(auth): add one-time email password reset and revoke sessions |
| `fb5d9b7e` | fix(tencentdocs): use bounded minute-scale retries and export budgets |
| `f4b418d5` | feat(tencentdocs): preserve selected source folders and retry paths |
| `560ef207` | feat(datasource): validate and synchronously import Tencent FAQ sources |
| `54e7f38e` | fix(datasource): publish completed source versions before retiring old content |

共享文件的改动按功能分阶段记录，最终树须与通过测试的完整工作区一致。历史隔离开发说明、目录/FAQ 发布说明和本页文案更新均属于最后的文档提交。无关临时文件 `internal/datasource/tmp_execute_datasource_recovery_test.go` 不执行、不提交、不推送，文件哈希保持不变。

## 验证

测试在服务器独立源码目录和无网络的 Go 容器中运行，2 CPU/4 GiB，未挂载生产数据库、Redis或业务卷。复用已存在的公开依赖缓存；公共 DNS 单测使用此前记录的固定公开地址映射。排除上述无关临时恢复测试。

```sh
go test ./... -count=1
go test -race ./internal/application/service ./internal/application/repository \
  ./internal/datasource/connector/tencentdocs ./internal/handler \
  ./internal/middleware ./internal/router ./internal/config \
  -run 'TestPrepush|TestTencentCandidate|TestFAQSource|TestTencentFAQ|TestPasswordReset|TestDataSourceCheckpoint|TestTencentMCPRead|TestTencentRetryDoesNot' -count=3
docker compose -f <isolated-source>/docker-compose.yml config --format json
```

- 全量 Go：68 个测试包通过。
- 本次故障回归与相关用例：通过；相关 race 用例连续三轮通过。router 在此筛选表达式下无匹配用例，已由全量包测试覆盖，不将其算作运行过 race 用例。
- Compose：合成专用 SMTP 密码与备用变量两种组合均通过。未使用生产密码做此测试。
- 前端未在本次复审中改动逻辑；沿用同一源码此前已完成的类型检查、360 个测试、构建及生产 Chrome 验收，详情见主发布记录。
- 五个中间版本各自的针对性隔离测试通过；79 个最终源码文件与全量/race 测试目录逐一比对一致。11 个涉及的前端文件与此前验证的版本一致。

证据目录：`/public/knowledgebase/upgrade-tests/sync-safe-20260906/` 下的 `prepush-*.log/json`；本地 `results/prepush-review-20260906/`。日志只包含合成测试数据和验证信息。

## 部署影响、回滚与残余风险

新增失败标记会使不完整的新版本明确失败并保留旧版，可能增加可见失败/补偿记录，但不会把不完整版本确认为成功。SMTP 模板修复影响按仓库 Compose 配置启动的实例；既有生产邮箱覆盖配置及账号未在本次推送任务中修改。

本批源码回退可按独立提交逐项 revert；生产运行仍由现有镜像控制。后续部署必须先确认摄取空闲并备份当前 Compose/Grafana。旧镜像不理解候选可见性协议，降级前必须处理未完成候选并保留最后可用版本；密码重置和已撤销会话不能通过镜像回滚恢复。

已知非 P0/P1 边界仍保留：纯图片智能文档的通用正文接口可能返回空内容；失败后按三轮补偿上限转人工处理，不能声称该能力已支持。没有稳定来源身份的历史记录不按标题猜测绑定/删除。数据源与 FAQ 各批次、数据库与外部索引之间没有跨系统事务；已完成批次可以保留，永久外部故障仍需人工处理。此次复审修复尚无新的生产运行证据，不能混用先前 8 成功/1 失败的生产验收来证明它们已部署。

源码最终提交：`54e7f38e7baf4dbfd918139109d6b3de4ec7c779`。推送前远程基点仍为 `66bd6afd8bd6aaa78ce4b04c915167b25a748d45`；使用显式 `origin HEAD:refs/heads/weknora-v0.7.2` 快进推送，并以 `git ls-remote` 核验远程最终提交。本批文档提交紧随上述源码提交。
