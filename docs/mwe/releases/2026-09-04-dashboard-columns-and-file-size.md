# Dashboard 字段精简与文件大小显示

## 范围

- 上游 WeKnora v0.7.2；主仓库 `mwe-support/weknora-dispatch-mwe`，分支 `weknora-v0.7.2`。
- 仅 Dashboard SQL 展示、表格字段配置、生成脚本与合成测试；不修改后端、异常恢复规则、队列或凭证，不部署/重启任何业务或监控服务。
- 最终 Dashboard revision 13；保留现有 50 条分页、Enter 跳页、GPU、容器日志导航和死信逻辑。

## 用户可见变化

- 主表 11 列：文档名称、腾讯文档路径、导入文件大小、WeKnora 工作空间、WeKnora 知识库、来源、失败环节、异常原因、状态、最近失败时间、详情。
- 明确 WeKnora 工作空间/知识库是导入目标；原“空间”是腾讯文档源空间信息，不再混在目标位置列中。
- 数据源平台显示中文名称；获取、导出、下载、入库等常用阶段翻译为可读名称；超限原因直接显示可读上限。
- 详情列显示“查看”，悬停后点击原生“检查值”图标打开完整信息。错误类别、恢复依据、源空间、连接配置、原始字节数和技术 ID 保留在详情，不再铺开为主表列。缺失 ID 不猜测、不构造源链接。
- 数据源汇总视图的工作空间/知识库标题也明确为 WeKnora，其余内容与筛选不变。
- 用户追加“腾讯文档路径”列，仅使用同步错误或知识元数据的 `source_path`；非腾讯来源显示不适用。只读核对当前 1916 条未删除腾讯知识记录及既有同步错误均无此字段，当前显示未记录。连接器现有遍历未保存祖先目录链；真实路径采集/历史回填需要后续后端改动或可靠源端核对，本次未猜测、未把目标知识库/本地文件夹替代源路径。

## 文件大小口径

- 仅从当前失败 knowledge 的正值 `file_size` 或该同步错误已有 `actual_bytes` 读取。
- 不使用旧 completed 版本大小、下载限制值、流式读取下界替代未知总大小。
- KB/MB/GB 统一按 1024 换算并保留两位小数；底层字节值保留在详情。没有可靠值显示“未记录”。
- WeKnora 的接收文件可能是腾讯文档转换后的 Markdown/导出文件，不代表腾讯源空间占用或原在线文档的总存储量，详情明确说明大小依据。
- 历史超限记录没有实际大小，仍显示“未记录”；不依据标题回填其他检查得到的大小。

## 验证

```text
python scripts/render-observability-failure-queries.py
python scripts/test-observability-failure-queries.py --emit /tmp/compact-regression.sql
psql -v ON_ERROR_STOP=1 -f /tmp/compact-regression.sql
```

- 使用无网络/无生产数据卷 PostgreSQL 17 临时容器，合成事务最终回滚。
- 既有身份去重、恢复反例、软删除、大小分类、分页/计数回归保持通过。
- 新增 1024/1048576/1073741824 字节分别显示 1.00 KB/MB/GB；1457777536 字节显示 1.36 GB；未知流长度及仅有旧版本大小保持“未记录”；详情保留同步 ID；主表严格 11 列；已有源路径原样显示、缺失路径保持未记录。
- 生产观察者 READ ONLY 查询通过，首屏大小分布包含 9.00 KB、1.31 MB 与未记录，不读取或下载文件正文。
- `git diff --check` 通过。浏览器验收与最终状态追加在本记录末尾。

## 热加载与回滚

- 替换前核对生产 Dashboard 与本地一致：`c20ce3101734ee7e458a2f61a8a162d4deb0c5284177e187af14f901f262e76c`。
- 回滚备份：`/public/knowledgebase/results/observability-columns-20260904/dashboard-before.json`。
- 最终文件 SHA256：`8bd1db963a21c0ac03bfcf2214de6efcd52fbd2d539e8a7f4138bcdfc9f5a2b6`。
- 将备份复制回 `/public/knowledgebase/observability/grafana/dashboards/knowledgebase-overview.json` 并等待热加载即可回滚；不需要重启应用。
- 当前后端超限日志增强仍按前一发布记录等待另行部署，本次不会让缺失的历史大小自动补齐。

## 最终验收

- 已在用户内置浏览器默认窄视口及批注对应的 1372×769 视口检查。主表的腾讯文档路径/导入大小/WeKnora归属列可见；未知路径和大小均为未记录。
- 详情列最终显示“查看”；悬停后点击检查值，原始 JSON 中同步ID、数据源ID、错误类别、大小依据及原始阈值完整保留。
- 现有输入框从第一页输入2回车，URL及首行记录改变，再恢复第一页。未新增顶部筛选器或修改分页脚本。
- 浏览器控制台错误/警告为0。本地/远端 Dashboard 哈希一致。
- Grafana 最后十分钟日志有一条权限读取 `SQLITE_BUSY/database is locked`，发生在文件供应/页面重载验证期间；页面查询、详情和翻页均成功。未将该后台日志误报为文档解析故障，本次不扩大为存储层调优。
- 临时 PostgreSQL 测试容器已停止/自动移除，测试 SQL 与日志保留在 `/public/knowledgebase/results/observability-columns-20260904/`。没有重启生产服务。
