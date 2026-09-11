"""Generate the six ledger panels. Pagination uses a complete frozen query result."""
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
GRAFANA = ROOT / 'deploy/mwe-observability/grafana'
VIEWS = {
 'current_run_progress': ('当前同步批次', ['documents','documents_succeeded','documents_failed','documents_blocked','documents_active','documents_unadmitted','containers','external_links','original_finished_status','original_finished_at']),
 'current_document_lifecycle': ('文档生命周期', ['availability','evidence_basis','published_job_id','published_generation','doc_type','native_raw_bytes','native_pages','native_units','images','media_bytes','readiness','completeness','steps_total','steps_succeeded','cleanup_problem','resources_active','charged_bytes','estimated_index_bytes']),
 'stage_retry_queue': ('阶段与精准重试', ['step_id','phase','unit_key','step_attempt','dispatch_seq','retry_count','max_retries','next_run_at','deadline_at','heartbeat_at','error_class','error_code']),
 'unresolved_incidents': ('未解决异常（含旧版本清理）', ['event_id','step_id','step_attempt','dispatch_seq','event_type','error_class','error_code','cleanup_problem']),
 'attempt_timeline': ('尝试与操作时间线', ['event_id','step_id','event_revision','step_attempt','dispatch_seq','event_type','actor','action','operation_request_id','resolves_event_id','resolution_type']),
 'lifecycle_inconsistencies': ('生命周期一致性检查', ['step_id','readiness','completeness','retirement_state','is_current','is_published','cleanup_problem']),
}


def query(view, detail):
    filters = [f"(${{{var}:sqlstring}}='' OR {column}::text=${{{var}:sqlstring}})" for var,column in
               [('workspace','tenant_id'),('knowledge_base','knowledge_base_id'),('source','datasource_id'),('run','run_id'),('status','status'),('stage','stage')]]
    filters.append("(${search:sqlstring}='' OR position(lower(${search:sqlstring}) in lower(title||' '||external_id||' '||job_id))>0)")
    if view == 'attempt_timeline':
        filters.append('$__timeFilter(observed_at)')
    fields = ["'job_id',job_id", "'knowledge_base_id',knowledge_base_id", "'datasource_id',datasource_id", "'source_revision',source_revision", "'revision',revision", "'publication_epoch',publication_epoch"]
    fields += [f"'{field}',{field}" for field in detail]
    fields += [f"'{field}',{field}" for field in ['run_id','original_error_ordinal','original_error_digest','linked_job_id','legacy_action','legacy_actor','legacy_evidence_at','legacy_dead_letter_id','legacy_run_status']]
    order = 'observed_at DESC,row_id DESC'
    return f"""-- All pages use this one result set. Above the bound, return an explicit
-- lower-bound notice, never a truncated list with a fabricated complete count.
WITH candidates AS MATERIALIZED (
 SELECT * FROM mwe_processing_{view} WHERE {' AND '.join(filters)}
 ORDER BY {order} LIMIT 10001
), gate AS (SELECT COUNT(*) AS n FROM candidates)
SELECT '完整快照：'||gate.n::text||'条' AS "查询状态", c.title AS "文档或批次",
 c.tenant_id::text AS "工作空间ID", c.kb_name AS "知识库", c.source_name AS "数据源",
 c.generation::text AS "文档版本", c.status AS "状态", c.stage AS "阶段", c.observed_at::text AS "记录时间",
 jsonb_build_object({', '.join(fields)})::text AS "详情"
FROM candidates c CROSS JOIN gate WHERE gate.n<=10000
UNION ALL
SELECT '至少10001条，请缩小筛选范围','','','','','','','','','' FROM gate WHERE gate.n>10000;
"""


def outputs():
    panels = []
    result = {}
    for index,(view,(title,detail)) in enumerate(VIEWS.items()):
        sql = query(view,detail)
        result[GRAFANA / 'queries' / f'processing_{view}.sql'] = sql
        panels.append({
            'id': index+1, 'type':'table', 'title':title, 'gridPos':{'h':13,'w':24,'x':0,'y':index*13},
            'description':'只读生命周期事实视图。每个表格一次读取完整结果；在浏览器内翻页不会重新查询。默认关闭自动刷新；手动刷新生成新快照。超过10000条明确提示缩小筛选。工作空间变量用于过滤，不构成访问权限；仅管理员可访问此数据源。操作入口：WeKnora 设置 → 处理生命周期。旧流程数据标记 legacy_unverified。',
            'datasource':{'type':'postgres','uid':'weknora-postgres'},
            'targets':[{'refId':'A','format':'table','rawQuery':True,'editorMode':'code','rawSql':sql,'datasource':{'type':'postgres','uid':'weknora-postgres'}}],
            'options':{'showHeader':True,'cellHeight':'sm','enablePagination':True,'footer':{'show':False,'enablePagination':True,'countRows':True,'reducer':['count'],'fields':''}},
            'fieldConfig':{'defaults':{'custom':{'filterable':True,'minWidth':90}},'overrides':[
                {'matcher':{'id':'byName','options':'详情'},'properties':[{'id':'custom.width','value':130},{'id':'custom.inspect','value':True},{'id':'mappings','value':[{'type':'regex','options':{'pattern':'.*','result':{'text':'查看记录'}}}]}]},
                {'matcher':{'id':'byName','options':'文档或批次'},'properties':[{'id':'custom.width','value':260}]},
            ]},
        })
    variables = [{'name':name,'label':label,'type':'textbox','query':'','current':{'text':'','value':''},'skipUrlSync':False,'hide':0}
                 for name,label in [('workspace','工作空间ID'),('knowledge_base','知识库ID'),('source','数据源ID'),('run','批次ID'),('status','状态'),('stage','阶段'),('search','文档标题/ID')]]
    dashboard = {'uid':'mwe-processing-lifecycle-v2','title':'WeKnora · 文档处理生命周期','description':'六类视图与应用历史页使用同一组数据库投影。仅时间线使用右上角日期范围；未解决异常保留所有时间。数据库账户仅能读取安全视图。',
                 'schemaVersion':39,'version':1,'editable':False,'refresh':'','timezone':'browser','time':{'from':'now-7d','to':'now'},'tags':['weknora','lifecycle','admin'],
                 'panels':panels,'templating':{'list':variables},'links':[{'title':'基础设施与旧流程记录','type':'link','url':'/d/mwe-kb-observability-v1','targetBlank':False}]}
    result[GRAFANA / 'dashboards' / 'processing-lifecycle.json'] = json.dumps(dashboard,ensure_ascii=False,indent=2)+'\n'
    # The existing operations homepage links to the frozen detail lists and
    # shows compact live counts. Keep its infrastructure panels and label the
    # historical summary tables explicitly instead of mixing their evidence.
    home_path = GRAFANA / 'dashboards' / 'knowledgebase-overview.json'
    home = json.loads(home_path.read_text(encoding='utf-8'))
    generated_ids = set(range(101,108))
    integrated = any(panel['id'] in generated_ids for panel in home['panels'])
    home['panels'] = [panel for panel in home['panels'] if panel['id'] not in generated_ids]
    if not integrated:
        for panel in home['panels']:
            if panel.get('gridPos',{}).get('y',0)>=43: panel['gridPos']['y']+=8
    for panel in home['panels']:
        if panel['id'] in (3,4,5,9) and not panel.get('title','').startswith('旧流程 · '): panel['title']='旧流程 · '+panel.get('title','')
        if panel['id'] in (10,11,12):
            panel['title']={10:'旧流程文档记录（待核实）',11:'旧流程同步记录（待核实）',12:'旧流程死信记录（待核实）'}[panel['id']]
    titles=['当前同步批次','当前文档条目','待处理或异常阶段','未解决异常事件','近7天处理事件','一致性异常']
    for index,view in enumerate(VIEWS):
        where=" WHERE observed_at >= NOW()-INTERVAL '7 days'" if view=='attempt_timeline' else ''
        home['panels'].append({'id':101+index,'type':'stat','title':titles[index],
            'description':'来自统一生命周期视图。计数单位是视图行，不把目录、文档、尝试或异常事件混算。完整明细及冻结分页见处理生命周期看板。',
            'gridPos':{'h':5,'w':4,'x':index*4,'y':43},'datasource':{'type':'postgres','uid':'weknora-postgres'},
            'targets':[{'refId':'A','format':'table','rawQuery':True,'rawSql':f'SELECT COUNT(*)::bigint AS total FROM mwe_processing_{view}{where}'}],
            'options':{'reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False},'textMode':'auto','colorMode':'value','graphMode':'none'},
            'fieldConfig':{'defaults':{'noValue':'读取状态未知','min':0},'overrides':[]}})
    home['panels'].append({'id':107,'type':'text','title':'完整生命周期明细','gridPos':{'h':3,'w':24,'x':0,'y':48},
        'options':{'mode':'markdown','content':'[打开处理生命周期看板](/d/mwe-processing-lifecycle-v2) · 当前批次、文档可用性、精准阶段重试、未解决异常、事件时间线、一致性检查。\n\n下方旧流程记录的阶段证据尚未核实；实际恢复操作进入 WeKnora 设置 → 处理生命周期。'}})
    home['version']=max(home.get('version',1),16)
    home.setdefault('links',[])
    if not any(link.get('url')=='/d/mwe-processing-lifecycle-v2' for link in home['links']):
        home['links'].append({'title':'处理生命周期','type':'link','url':'/d/mwe-processing-lifecycle-v2','targetBlank':False})
    result[home_path]=json.dumps(home,ensure_ascii=False,indent=2)+'\n'
    return result


if __name__ == '__main__':
    for path,content in outputs().items():
        if '--check' in sys.argv:
            assert path.read_text(encoding='utf-8')==content, f'Stale generated file: {path}'
        else:
            path.write_text(content,encoding='utf-8')
