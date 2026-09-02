# 数据源每周与每月同步周期

日期：2026-09-02  
上游版本：WeKnora v0.7.2  
目标分支：`weknora-v0.7.2`

## 变更动机

数据源同步频率原本最高只能选择“每天”。对更新较少的部门资料，这会产生不必要的外部接口请求、同步任务和解析负载。

## 变更范围

- 新增“每周（周日 02:00）”，cron 为 `0 0 2 * * 0`。
- 新增“每月（1 日 02:00）”，cron 为 `0 0 2 1 * *`。
- 列表页可将两个 cron 表达式显示为对应的人类可读文案。
- 中、英、韩、俄语言包保持相同键集合。
- 复用后端现有六字段 cron 调度器，没有新增接口、数据库字段或调度服务。

## 用户与部署影响

- 空间管理员可以为低频更新的数据源选择每周或每月同步。
- 已有数据源的同步设置不自动改变。
- 生产只重建 `WeKnora-frontend`；`WeKnora-app` 与文档处理队列未重启。

## 验证

- `npm run type-check`：通过。
- `npm run check-i18n`：11/11 通过。
- 周/月 cron 显示映射断言：通过。
- `npm run build-only`：通过；dist 完整性检查为 176 个文件、2 个 HTML、56 个 JS、31 个 CSS。
- 生产浏览器验证：同步频率下拉中“每周（周日 02:00）”和“每月（1 日 02:00）”均存在且可见；未保存测试知识库设置。
- 生产容器验证：`WeKnora-frontend` healthy；`WeKnora-app` 启动时间未变化且保持 healthy。

## 生产镜像

`marvel/weknora-ui:v0.7.2-sync-schedules-20260902`

生产配置备份：

`/public/knowledgebase/weknora/override.yml.bak.20260902-sync-schedules`

## 回滚

将 `override.yml` 中前端镜像恢复为 `marvel/weknora-ui:v0.7.2-org-member-picker-test-20260821`，然后仅重建 `frontend` 服务。本次没有数据库迁移。

## 残余风险

- 周期日期和时间目前为固定低峰值，没有提供自定义星期、日期或时刻。
- 调度时区依赖应用容器的 `TZ=Asia/Shanghai`；变更部署时区前需要重新核对这些预设。
