# 共享空间成员选择器批量添加改进

日期：2026-08-21

上游版本：WeKnora v0.7.2

目标分支：`weknora-v0.7.2`

## 问题与根因

共享空间的成员单位已经迁移为租户/空间，但成员添加弹层仍沿用单结果远程搜索交互：

- 空选择器不请求候选空间；
- 前端要求至少输入2个字符；
- 后端空查询直接返回空数组；
- 选择与提交均只能处理一个空间。

这使17个以上部门空间的集中纳管需要管理员记住空间名并逐个添加。

## 变更范围

- 后端候选接口允许空查询，返回全部未加入的可邀请空间；任意非空文本按空间名过滤。
- 后端在过滤已有成员前读取完整匹配集，避免第一页全是已加入成员时漏掉后续候选。
- 默认返回上限200，显式上限500。
- 前端打开添加成员弹层时立即加载候选空间。
- 移除2字符限制，支持任意长度搜索。
- 空间选择器改为多选，超过3个选中项时折叠标签。
- 批量提交最多4个并发请求；单空间邀请接口保持兼容。
- 批量成功后只主动刷新一次空间列表；部分失败时保留失败项供重试。
- 更新中、英、韩、俄提示文案。

## 用户与部署影响

- 共享空间管理员可以直接浏览、搜索并多选全局可邀请空间。
- 已加入空间仍由后端自动过滤。
- 未新增数据库表、迁移或破坏性接口。
- 单成员邀请客户端保持兼容。

## 验证

- 浏览器旧版复现：空下拉为“暂无数据”；1字符不请求；界面明确显示“至少2个字符”；只能单选。
- `node --test src/views/organization/OrganizationSettingsModal.test.mjs`：4/4 PASS。
- `npm test`：360/360 PASS。
- `npm run check-i18n`：11/11 PASS。
- `npm run type-check`：PASS。
- `npm run build`与dist完整性检查：PASS，410个文件。
- Go 1.26隔离克隆：`go test ./internal/handler ./internal/application/service -count=1` PASS。
- `go vet ./internal/handler`：PASS。
- 本地`git diff --check`：PASS。

## 测试镜像

- `marvel/weknora-app:v0.7.2-org-member-picker-test-20260821`
- `marvel/weknora-ui:v0.7.2-org-member-picker-test-20260821`

生产已于2026-08-21切换至上述测试镜像。仅app和frontend被重建，10秒内恢复healthy；Postgres、Redis和MinIO未重建。

生产override备份：

`/public/knowledgebase/weknora/override.yml.bak.20260821T070916Z-org-member-picker`

浏览器修复后验证（`https://kb.mwexk.com/platform/organizations`）：

- 打开选择器即列出10个未加入空间；
- 空查询后端响应200，约8.05ms；
- 单字符`采`响应200，约6.57ms，只返回`采购部知识库`；
- 输入`IT`返回`IT日常维护知识`和`AIT组`；
- 跨搜索条件同时选中`采购部知识库`和`IT日常维护知识`，两个标签均保留且添加按钮启用；
- 点击取消，成员数仍为2，没有产生写入；
- 浏览器无相关console error/warn；
- app/frontend healthy、重启计数0，非终态知识0。

## 回滚

生产验证若失败，只需将app和frontend镜像恢复为部署前标签，使用原compose/override执行`up -d --no-deps app frontend`。本次无数据库迁移和数据回滚要求。

## 残余风险

- 批量添加复用单成员接口，不具备全批次原子性；部分成功时UI会显示失败数量并保留失败项重试。
- 候选接口为管理员界面一次性读取全部匹配租户后在内存中过滤，当前企业规模适用；租户数量达到数千时应升级为游标分页。
- 尚未在生产执行真实批量添加提交；为避免无意改变共享空间成员，本次浏览器验收止于多选、按钮启用和取消。批量提交逻辑由回归测试、类型检查和单成员后端接口覆盖。
