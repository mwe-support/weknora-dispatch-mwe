# FAQ 数据源与邮箱验证码重置密码

> 本页是 2026-09-01 隔离开发阶段的历史记录；下述实验提交号不是本次目标分支的提交号。当前实现、生产验证及最终提交清单以 [2026-09-06 发布记录](2026-09-06-sync-safety-password-reset.md) 和 [提交前复审记录](2026-09-06-prepush-review-and-publish.md) 为准。

## 发布信息

- 上游版本：WeKnora v0.7.2
- 目标分支：`weknora-v0.7.2`
- 发布标识：`mwe-v0.7.2-faq-datasource-password-reset-20260901`
- 计划测试镜像：`marvel/weknora-app:v0.7.2-faq-datasource-password-reset-test-20260901`、`marvel/weknora-ui:v0.7.2-faq-datasource-password-reset-test-20260901`
- 当前状态：隔离实现与自动化测试通过，尚未部署生产
- 功能提交：`c5f734c9`（FAQ 数据源）、`ac1ab1fe`（邮箱验证码重置密码）

## 变更范围

### FAQ 外部数据源

- FAQ 知识库设置页开放“数据源”入口，复用现有凭证、范围选择、计划同步和同步日志。
- 数据源内容支持标准 FAQ JSON、CSV/TSV，以及腾讯 Sheet 导出的 Markdown 表格。
- 表格至少包含“问题/Question”和“机器人回答/Answer”列；分类、相似问题、反例问题和是否停用为可选列，多值使用 `##` 分隔。
- 合法条目汇总后复用现有 FAQ 批量导入处理器，并在数据源任务内等待验证与索引结束后再落同步结果。
- 首版使用 append/merge，不自动删除外部源中消失的条目，避免遍历不完整时误删人工维护内容。

### 忘记密码

- 登录页增加“忘记密码”入口，通过注册邮箱发送六位验证码并设置新密码。
- 验证码保存在 Redis：10 分钟有效、单邮箱 60 秒发送冷却、最多 5 次尝试、成功后一次性消费。
- 请求接口对存在和不存在的邮箱返回相同结果，避免账户枚举。
- 重置复用统一的 8–32 位密码策略，并撤销该用户全部既有会话。
- SMTP 由环境变量配置，支持 465 隐式 TLS 与 587 STARTTLS；生产发件人计划为 `sales001@marveltechgroup.com`。

## 配置

```env
WEKNORA_AUTH_PASSWORD_RESET_ENABLED=true
WEKNORA_AUTH_PASSWORD_RESET_SMTP_HOST=<smtp-host>
WEKNORA_AUTH_PASSWORD_RESET_SMTP_PORT=587
WEKNORA_AUTH_PASSWORD_RESET_SMTP_USERNAME=sales001@marveltechgroup.com
WEKNORA_AUTH_PASSWORD_RESET_SMTP_PASSWORD=<secret>
WEKNORA_AUTH_PASSWORD_RESET_FROM=sales001@marveltechgroup.com
```

## 验证命令与结果

```bash
# 目标 Go 测试
go test ./internal/application/service -run TestParseFAQFetchedItem -count=1
go test ./internal/handler -run TestPasswordReset -count=1
go test ./internal/config -run PasswordResetSMTP -count=1

# 完整 Go 回归（一次性 golang:1.26-bookworm 容器内安装 libsqlite3-dev）
go test ./... -count=1

# 前端
node node_modules/vue-tsc/bin/vue-tsc.js --build
node node_modules/tsx/dist/cli.mjs --test src/i18n/localeKeyAudit.test.ts
node node_modules/tsx/dist/cli.mjs --test
```

- FAQ JSON、CSV、腾讯 Sheet Markdown 解析测试：通过。
- 密码验证码请求、未知邮箱不枚举、一次性消费、弱密码不消费验证码：通过。
- 完整 Go 回归：全部包通过。
- 前端 Vue 类型检查：通过；四语言 i18n 审计 11/11；完整前端测试 358/358。

## 生产影响与回滚

- 生产部署需要重建 app/frontend；必须等待摄取任务空闲。
- 未配置完整 SMTP 时 `/auth/config` 不启用忘记密码入口。
- 回滚到上一版 app/frontend 镜像即可；新增环境变量可保留但应关闭 `WEKNORA_AUTH_PASSWORD_RESET_ENABLED`。

## 残余风险

- FAQ 数据源删除同步暂不删除 FAQ 条目；需要来源级 provenance 后再安全实现。
- FAQ 数据源任务会等待条目索引完成；超大批次可能延长一次同步的运行时间，受现有两小时任务上限约束。
- SMTP 真实投递、垃圾邮件策略和发件域认证需在提供邮箱 SMTP 凭证后做生产前联调。
