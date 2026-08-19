# 空间重命名后名称即时同步（2026-08-19）

## 状态与范围

- 上游版本：WeKnora `v0.7.2`
- 目标仓库：`mwe-support/weknora-dispatch-mwe`
- 目标分支：`weknora-v0.7.2`
- 当前状态：代码与自动化测试通过，已部署生产环境；待真实页面验收。

## 现象与根因

- 空间设置保存新名称后，空间切换列表已显示新名称，但侧栏和底部账户区仍显示旧名称。
- 切换到其他空间再切回后恢复一致。
- 重命名流程已更新 `tenant.name` 与 `memberships[].tenant_name`，但激活空间存在 override 时，
  `UserMenu` 与 `currentTenantName` 优先读取 `selectedTenantName`。
- `selectedTenantName` 及 `weknora_selected_tenant_name` 未在重命名成功后更新，因此保持旧值；
  空间切换会重新调用 `setSelectedTenant`，这解释了切换后恢复。

## 修复

- 重命名成功后，如果被重命名空间正是 `selectedTenantId`，调用
  `setSelectedTenant(同一ID, 新名称)`。
- 因空间 ID 未改变，Store 不会触发 `tenantChanged`，不会清空空间级缓存或重载页面。
- 单空间/home 场景继续由同步后的 `tenant.name` 与 membership 名称提供显示值。

## 验证

- 修复前回归测试稳定失败：当前名称与 localStorage 仍为旧名称。
- 修复后同一测试通过，无需切换空间。
- 前端类型检查：通过。
- 前端测试：359/359 通过。
- 前端生产构建：通过；资源检查统计 370 个文件、235 个 JS、46 个 CSS。
- Browser 插件自动化因当前插件文件未在 Node 运行时受信任路径中而无法建立会话；未绕过该安全限制。
- 生产 Frontend：healthy、重启次数 0；App/MCP 容器未重建。
- `https://kb.mwexk.com/` 与抽样静态资源均返回 200，Content-Type 正确。

## 部署与回滚

- 前端镜像：`marvel/weknora-ui:v0.7.2-tenant-rename-cache-20260819`
- 部署提交：`43712c03d4d7cf08770a2200352d85b98c30ff07`
- 配置备份：`/public/knowledgebase/weknora/override.yml.bak-tenant-rename-cache-20260819-085153`
- 直接回滚点：`marvel/weknora-ui:v0.7.2-owner-original-file-20260817`。
- 生产验收：当前空间改名后不切换空间，确认侧栏、底部账户区、空间切换列表同步显示新名称；
  再刷新页面确认持久化名称一致。

## 残余风险

- 自动浏览器交互尚未执行，部署后需人工或恢复 Browser 受信任路径后补测。
