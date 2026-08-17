# 受邀编辑者无法操作知识库内容（2026-08-17）

## 状态与范围

- 严重等级：高；阻断受邀编辑者向已有知识库添加和管理内容。
- 上游版本：WeKnora `v0.7.2`
- 目标仓库：`mwe-support/weknora-dispatch-mwe`
- 目标分支：`weknora-v0.7.2`
- 当前状态：完整内容权限修复已部署生产环境，并通过受邀 Contributor 业务操作验收。

## 根因

知识库文件、URL 和手工文档创建路由同时应用了两类权限：

1. `OwnedKBOrAdmin`：要求调用者是知识库创建者或工作区管理员；
2. `KBAccessWrite`：允许自有知识库或具有 Editor 权限的共享知识库写入。

前一个门禁会在后一个门禁解析有效编辑权限之前返回 403。因此，加入工作区并具有
Contributor/编辑权限、但不是该知识库原始创建者的成员，无法向已有知识库添加文档。
前端同样把“添加文档”复用了更严格的 `canEdit` 判断，导致上传入口不显示或操作被提前拦截。

首轮上传修复上线后进一步确认：单文档删除/更新/重建、分块、FAQ、Wiki 和内容标签路由仍
重复使用 owner/admin 门禁；批量删除、目录移动和跨知识库移动还在 handler 内重复检查
知识库所有权。Trace 后端本可读取，但前端因为没有渲染文档操作菜单而无法进入。

## 修复

- 知识库内容统一使用 `Contributor + KBAccessWrite`：上传、重建、Trace、删除/移动文档、
  批量操作、分块、FAQ、Wiki、文件夹与内容标签。
- handler 的批量删除、目录移动和跨知识库移动取消重复的 KB 所有权检查，仍校验 Editor
  权限、知识归属、源/目标知识库和 API Key 范围。
- 前端 `canEdit` 统一为内容编辑权限，Contributor 可见文档操作菜单；`canManage` 继续仅允许
  KB 创建者或 Admin 管理知识库设置、共享和删除知识库。
- Viewer 仍只能读取；API Key 仍受 ingest 能力及知识库范围约束。

## 验证

- 修复前真实路由回归：有效 multipart 上传稳定返回 403，服务层未被调用。
- 修复后同一回归：Contributor 返回 200，服务层收到的有效租户为知识库所属租户。
- Viewer 负向回归：仍返回 403，服务层未被调用。
- 后端 `go test ./internal/router`：通过。
- 后端目标 `go test -race`：通过。
- 后端 `go vet ./internal/router`：通过，无诊断输出。
- 前端 `npm run type-check`：通过。
- Contributor 内容矩阵回归：上传、单文档删除、重建、批量删除和目录移动均到达业务层；
  Viewer 仍在业务层之前返回 403。
- 后端 router/handler/middleware 全包测试：通过；目标 race 通过，vet 无诊断输出。
- 前端 `npm test`：356/356 通过。
- 前端 `npm run build`：通过；资源完整性检查统计 348 个文件、215 个 JS、44 个 CSS。
- 生产 app 容器内 `/health`：`{"status":"ok"}`。
- 生产 app/frontend：均为 healthy、重启次数 0。
- `https://kb.mwexk.com/`：返回 200；首页引用的抽样 JS/CSS 资源均返回 200 且 Content-Type 正确。

## 部署与回滚

- 应用镜像：`marvel/weknora-app:v0.7.2-editor-content-20260817`
- 前端镜像：`marvel/weknora-ui:v0.7.2-editor-content-20260817`
- 部署提交：`f22742704158a642c905f2753e6c137352921315`
- 直接回滚备份：`/public/knowledgebase/weknora/override.yml.bak-editor-content-20260817-081834`
- 上一版上传修复镜像：`marvel/weknora-app:v0.7.2-invited-editor-upload-20260817`、
  `marvel/weknora-ui:v0.7.2-invited-editor-upload-20260817`
- 回滚方式：恢复上述配置备份，并仅重建 app/frontend；代码层可回退本修复提交。
- 生产验收：使用受邀 Contributor 在既有知识库验证重建、Trace、删除、目录移动和批量管理；
  同时确认不能进入 KB 设置或删除知识库，Viewer 页面仍无编辑入口。

## 残余风险

- 内容删除与移动属于真实数据变更，生产验收应使用专门测试文档，避免影响业务资料。
- 跨租户组织共享 Editor 仍需补一次真实 UI 冒烟；本次两个已知实例均为同一工作区中的 Contributor。
