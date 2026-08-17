# 受邀编辑者无法上传知识库文档（2026-08-17）

## 状态与范围

- 严重等级：高；阻断受邀编辑者向已有知识库添加内容。
- 上游版本：WeKnora `v0.7.2`
- 目标仓库：`mwe-support/weknora-dispatch-mwe`
- 目标分支：`weknora-v0.7.2`
- 当前状态：代码与隔离测试通过，已部署生产环境；待受邀 Contributor 完成业务上传验收。

## 根因

知识库文件、URL 和手工文档创建路由同时应用了两类权限：

1. `OwnedKBOrAdmin`：要求调用者是知识库创建者或工作区管理员；
2. `KBAccessWrite`：允许自有知识库或具有 Editor 权限的共享知识库写入。

前一个门禁会在后一个门禁解析有效编辑权限之前返回 403。因此，加入工作区并具有
Contributor/编辑权限、但不是该知识库原始创建者的成员，无法向已有知识库添加文档。
前端同样把“添加文档”复用了更严格的 `canEdit` 判断，导致上传入口不显示或操作被提前拦截。

## 修复

- 文件、URL、手工文档创建改为 `Contributor + KBAccessWrite`。
- 新增独立的前端 `canAddKnowledge` 权限，仅控制新增内容入口。
- 知识库设置、删除、文件夹重命名及已有内容的破坏性操作仍保留 owner/admin 边界。
- Viewer 仍不能上传；API Key 仍受 ingest 能力及知识库范围约束。

## 验证

- 修复前真实路由回归：有效 multipart 上传稳定返回 403，服务层未被调用。
- 修复后同一回归：Contributor 返回 200，服务层收到的有效租户为知识库所属租户。
- Viewer 负向回归：仍返回 403，服务层未被调用。
- 后端 `go test ./internal/router`：通过。
- 后端目标 `go test -race`：通过。
- 后端 `go vet ./internal/router`：通过，无诊断输出。
- 前端 `npm run type-check`：通过。
- 前端 `npm test`：355/355 通过。
- 前端 `npm run build`：通过；资源完整性检查统计 337 个文件、205 个 JS、43 个 CSS。
- 生产 app 容器内 `/health`：`{"status":"ok"}`。
- 生产 app/frontend：均为 healthy、重启次数 0。
- `https://kb.mwexk.com/`：返回 200；首页引用的抽样 JS/CSS 资源均返回 200 且 Content-Type 正确。

## 部署与回滚

- 应用镜像：`marvel/weknora-app:v0.7.2-invited-editor-upload-20260817`
- 前端镜像：`marvel/weknora-ui:v0.7.2-invited-editor-upload-20260817`
- 部署提交：`806634dc2c6fc47114d6a70fc913322cf725f2ef`
- 配置备份：`/public/knowledgebase/weknora/override.yml.bak-invited-editor-upload-20260817-074203`
- 回滚方式：恢复上述配置备份，并仅重建 app/frontend；代码层可回退本修复提交。
- 生产验收：使用受邀 Contributor 在既有知识库上传一个小型测试文档，确认创建成功、
  列表即时出现且解析进入正常状态；随后确认 Viewer 页面无添加入口且直接 API 调用仍为 403。

## 残余风险

- 本次只调整“新增内容”权限，没有扩大知识库设置、删除或已有内容修改权限。
- 文件上传完成了真实 multipart 路由回归；URL 和手工文档复用相同门禁链，部署后仍应各做一次冒烟测试。
