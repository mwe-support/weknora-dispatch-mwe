"""Generate only failure SQL/descriptions; preserve the live dashboard's UI."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GRAFANA = ROOT / "deploy/mwe-observability/grafana"
DASHBOARD = GRAFANA / "dashboards/knowledgebase-overview.json"


def size_label(column):
    # Human-facing KB/MB/GB use a documented 1024-based conversion. Never
    # substitute a limit, observed prefix or old knowledge version for size.
    value = f"({column})::numeric"
    return f"""CASE WHEN {column} ~ '^[0-9]{{1,18}}$' THEN
      CASE WHEN {value}>=1073741824 THEN round({value}/1073741824,2)::text||' GB'
           WHEN {value}>=1048576 THEN round({value}/1048576,2)::text||' MB'
           ELSE round({value}/1024,2)::text||' KB' END ELSE '未记录' END"""


def queries():
    doc = (GRAFANA / "queries/document_failures.sql").read_text(encoding="utf-8").strip()
    source = (GRAFANA / "queries/source_failures.sql").read_text(encoding="utf-8").strip()
    detail = f'''SELECT title AS "文档名称",
      CASE WHEN source='tencent_docs' THEN COALESCE(source_path,'未记录') ELSE '不适用' END AS "腾讯文档路径",
      {size_label('actual_bytes')} AS "导入文件大小",
      workspace AS "WeKnora 工作空间",kb_name AS "WeKnora 知识库",
      CASE source WHEN 'tencent_docs' THEN '腾讯文档' WHEN 'web' THEN '手工/API上传'
        WHEN 'feishu' THEN '飞书' WHEN 'feishu_drive' THEN '飞书云盘' WHEN 'notion' THEN 'Notion'
        ELSE COALESCE(NULLIF(source,''),ds_name) END AS "来源",
      CASE stage WHEN 'fetch_metadata' THEN '获取文件信息' WHEN 'fetch_content' THEN '获取内容'
        WHEN 'fetch' THEN '源端获取' WHEN 'export' THEN '导出' WHEN 'download' THEN '下载'
        WHEN 'faq_validate' THEN 'FAQ格式校验' WHEN 'faq_import' THEN 'FAQ入库'
        WHEN 'ingest' THEN '入库' WHEN 'knowledge_processing' THEN '知识处理' ELSE stage END AS "失败环节",
      CASE WHEN category='FILE_SIZE_EXCEEDED' THEN '文件大小超出上限（上限：'||{size_label('limit_bytes')}||'）'
        ELSE LEFT(regexp_replace(reason,'https?://[^[:space:]]+','[URL_REDACTED]','g'),600) END AS "异常原因",
      CASE WHEN identity_basis='legacy_title_only' THEN '恢复待核实' ELSE '未解决' END AS "状态",
      failed_at AS "最近失败时间",
      jsonb_strip_nulls(jsonb_build_object(
        '错误类别',category,'完整原因',regexp_replace(reason,'https?://[^[:space:]]+','[URL_REDACTED]','g'),
        '恢复判定',resolution,'定位依据',identity_basis,'连接配置名称',ds_name,
        '腾讯文档源空间',CASE WHEN source='tencent_docs' THEN NULLIF(space,'') END,
        '文件大小依据',CASE WHEN actual_bytes IS NULL THEN '未记录；未使用上限、读取下界或旧版本大小替代'
          WHEN knowledge_id IS NOT NULL THEN 'WeKnora接收文件（可能为源文档转换后的文件）' ELSE '本次导出/下载响应记录' END,
        '实际字节数',actual_bytes,'限制字节数',limit_bytes,'至少已读取字节数',observed_at_least_bytes,
        'sync_log_id',NULLIF(linked_sync_log_id,''),'data_source_id',NULLIF(data_source_id,''),
        'knowledge_id',NULLIF(linked_knowledge_id,''),'knowledge_base_id',knowledge_base_id,
        'external_id',external_id,'file_id',file_id))::text AS "详情"
      FROM failure_rows ORDER BY failed_at DESC,data_source_id,file_key'''
    summary = '''SELECT updated_at AS "时间",workspace AS "WeKnora 工作空间",kb_name AS "WeKnora 知识库",source_name AS "数据源",source_type AS "类型",status AS "状态",items_total AS "总数",items_failed AS "失败数",LEFT(regexp_replace(COALESCE(NULLIF(error_message,''),result::text,'未记录错误信息'),'https?://[^[:space:]]+','[URL_REDACTED]','g'),600) AS "异常信息",id AS "sync_log_id",data_source_id FROM failure_rows ORDER BY updated_at DESC,id DESC'''
    count = "SELECT GREATEST(CEIL(COUNT(*) / 50.0)::int,1)::text AS __text,GREATEST(CEIL(COUNT(*) / 50.0)::int,1)::text AS __value FROM failure_rows"
    page = lambda name: " LIMIT 50 OFFSET (GREATEST(COALESCE(NULLIF(${" + name + ":sqlstring}, '')::int,1),1)-1)*50"
    return {10: (doc + "\n" + detail + page("doc_page"), doc + "\n" + count, "doc_pages"),
            11: (source + "\n" + summary + page("sync_page"), source + "\n" + count, "sync_pages")}


def render():
    text = DASHBOARD.read_text(encoding="utf-8")
    before = json.loads(text)
    after = json.loads(text)
    for panel_id, (detail, count, variable) in queries().items():
        panel = next(p for p in after["panels"] if p["id"] == panel_id)
        panel["targets"][0]["rawSql"] = detail
        var = next(v for v in after["templating"]["list"] if v["name"] == variable)
        var["query"] = var["definition"] = count
    document_panel = next(p for p in after["panels"] if p["id"] == 10)
    document_panel["description"] = "WeKnora工作空间/知识库表示导入目标。腾讯文档路径仅使用源端source_path记录；当前历史未采集则显示未记录，不使用WeKnora文件夹或标题推断。腾讯文档源空间与技术ID在详情中。导入文件大小按1024换算KB/MB/GB，可能为转换后文件；未知显示未记录。悬停详情点击检查图标查看完整信息。保留逐文件恢复判定及50条分页，每轮错误样本最多100条。"
    document_panel["options"]["sortBy"] = [{"desc": True, "displayName": "最近失败时间"}]
    document_panel["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": "详情"}, "properties": [
            {"id": "custom.cellOptions", "value": {"type": "auto"}},
            {"id": "mappings", "value": [{"type": "regex", "options": {"pattern": ".*", "result": {"text": "查看"}}}]},
            {"id": "custom.inspect", "value": True},
            {"id": "custom.width", "value": 90}]},
        *[{"matcher": {"id": "byName", "options": name}, "properties": [{"id": "custom.width", "value": width}]}
          for name, width in [("文档名称",260),("腾讯文档路径",240),("导入文件大小",115),("失败环节",110),("状态",110),("最近失败时间",175),("异常原因",300)]]]
    next(p for p in after["panels"] if p["id"] == 11)["description"] = "有效数据源最新同步失败/部分成功汇总。具体文件见上方文档失败表；一次增量success不代表旧失败文件已重新处理。已删除源/知识库排除；每页50条。"
    # Replace JSON string values, not a historical dashboard snapshot. This
    # preserves formatting and all other panels, paging scripts and variables.
    replacements = {}
    for p, q in zip(before["panels"], after["panels"]):
        if p != q:
            replacements[p["targets"][0]["rawSql"]] = q["targets"][0]["rawSql"]
            replacements[p["description"]] = q["description"]
    for p, q in zip(before["templating"]["list"], after["templating"]["list"]):
        if p != q:
            replacements[p["query"]] = q["query"]
            replacements[p["definition"]] = q["definition"]
    for old, new in replacements.items():
        text = text.replace(json.dumps(old, ensure_ascii=False), json.dumps(new, ensure_ascii=False))
    lines = text.splitlines(keepends=True)
    panel_line = next(i for i, line in enumerate(lines) if line.strip() == '"id": 10,')
    field_line = next(i for i in range(panel_line-1, -1, -1) if '"fieldConfig":' in lines[i])
    options_line = next(i for i in range(panel_line+1, len(lines)) if '"options":' in lines[i])
    for index, key in ((field_line, "fieldConfig"), (options_line, "options")):
        lines[index] = '      "' + key + '": ' + json.dumps(document_panel[key], ensure_ascii=False) + ',\n'
    text = ''.join(lines)
    assert json.loads(text) == after, "unexpected dashboard mutation"
    if text != DASHBOARD.read_text(encoding="utf-8"):
        DASHBOARD.write_text(text, encoding="utf-8")
    print("failure query generation OK; UI and pagination preserved")


if __name__ == "__main__":
    render()
