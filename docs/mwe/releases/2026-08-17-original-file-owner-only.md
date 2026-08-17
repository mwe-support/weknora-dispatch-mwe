# 原始文档仅空间所有者可访问（2026-08-17）

## 状态与范围

- 安全等级：高；原策略允许 Editor/Contributor 和 retrieve API Key 获取原始文件字节。
- 上游版本：WeKnora `v0.7.2`
- 目标仓库：`mwe-support/weknora-dispatch-mwe`
- 目标分支：`weknora-v0.7.2`
- 当前状态：已部署生产环境并通过验收；非 Owner 下载被拒绝，API Key 生产拒绝测试通过。

## 安全不变量

- 只有原始文档所属空间的 Tenant Owner 可以访问原始文件字节。
- Tenant Admin、Contributor/Editor、Viewer、共享接收方空间 Owner 和所有 API Key 均禁止。
- Editor/Agent 仍可使用解析后的全文、分块、检索结果、元数据与 Trace。

## 根因与旁路

- `/knowledge/:id/download` 使用 `Contributor + KBAccessWrite` 并声明 retrieve API Key 能力。
- `/knowledge/:id/preview` 同样返回原始字节；只隐藏下载按钮会留下另存和脚本批量读取旁路。
- 共享接收方空间 Owner 可凭本空间 Owner 角色和 Editor share grant 读取源空间原件。

## 修复

- download/preview 从 API Key 能力表移除，保持默认拒绝。
- JWT 路由要求 Tenant Owner；handler 再校验文档 `tenant_id` 必须等于当前空间。
- 前端仅源空间 Owner 显示下载及 raw preview；其他角色默认使用解析内容视图。
- KB 文件代理仍只允许 `exports/` 派生资源，继续拒绝原始知识文件路径。

## 验证

- 修复前：Tenant Admin、Contributor 和共享接收方 Owner 均返回 200；download 被声明为 retrieve API Key 路由。
- 修复后：仅源空间 Owner 返回 200；Admin、Contributor、Viewer、共享接收方 Owner 和 API Key 均返回 403/默认拒绝。
- raw preview 采用同一 Owner-only 边界；主动内容类型安全测试继续通过。
- router/handler/middleware 全包测试：通过；目标 race 通过，vet 无诊断输出。
- 前端类型检查：通过；测试 358/358 通过。
- 前端生产构建：通过；资源检查统计 359 个文件、225 个 JS、45 个 CSS。
- 生产 API Key 实测：同一有效 Key 列知识库/文档返回 200/200；download/preview 返回 403/403。
- 生产人工验收：非空间 Owner 已无法下载原始文档。
- 生产 app/frontend：均 healthy、重启次数 0；容器内 `/health` 返回 `{"status":"ok"}`。

## 部署与回滚

- 应用镜像：`marvel/weknora-app:v0.7.2-owner-original-file-20260817`
- 前端镜像：`marvel/weknora-ui:v0.7.2-owner-original-file-20260817`
- 部署提交：`84ef372f346a4b24c3d430e6fcf748858ef18d24`
- 配置备份：`/public/knowledgebase/weknora/override.yml.bak-owner-original-file-20260817-093251`
- 直接回滚点：`marvel/weknora-app:v0.7.2-editor-content-20260817`、
  `marvel/weknora-ui:v0.7.2-editor-content-20260817`。
- 生产验收：Owner 下载/预览测试文件成功；Editor 页面无下载/raw preview，直接请求返回 403；
  API Key 调用 download/preview 返回默认拒绝。

## 残余风险

- 浏览器已获得的历史缓存或用户此前下载的副本无法被服务端追溯撤回。
- 解析后的知识内容仍可被 Editor/Agent 检索和阅读，这是业务要求而非原始文件访问。
