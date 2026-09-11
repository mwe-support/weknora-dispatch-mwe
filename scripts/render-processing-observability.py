"""Generate the six ledger panels. Pagination uses a complete frozen query result."""
import json
import importlib.util
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


def sql_label(value, labels):
    return 'CASE '+value+' '+ ' '.join("WHEN '"+key+"' THEN '"+label+"'" for key,label in labels.items())+" ELSE COALESCE(NULLIF("+value+",''),'—') END"


STATUS = {'planned':'待启动','enqueue_pending':'待派发','queued':'排队中','running':'处理中','waiting_external':'等待外部结果','retry_wait':'等待重试','succeeded':'已完成','success':'历史成功','completed':'历史完成','partial':'历史部分成功','failed':'失败','blocked':'需处理','canceled':'已取消','superseded':'已替代','skipped':'已跳过','legacy_unverified':'历史待核实'}
STAGE = {'scan':'扫描批次','legacy_scan':'历史扫描','scan_page':'扫描目录','scan_document':'确认源文档','native_read':'读取源内容','export_start':'发起导出','export_poll':'等待导出','download':'下载文件','normalize':'内容转换','parse':'解析','assets':'收集图片','images':'图片处理','image_ocr':'图片文字识别','image_caption':'图片描述','chunk':'文档分块','embedding':'生成向量','text_index':'正文索引','image_index':'图片索引','summary':'摘要','questions':'生成问题','publish':'发布版本','retire':'清理版本','retire_previous':'清理旧版本','legacy_snapshot':'核验旧文件','legacy_indexes':'核验旧索引'}
ERROR = {'IDENTITY_UNRESOLVED':'未能确认腾讯文档身份或状态','AUTH_REQUIRED':'来源凭据不可用或失效','SOURCE_SIZE_EXCEEDED':'内容超出大小限制','FILE_SIZE_EXCEEDED':'文件超出大小限制','NATIVE_SIZE_EXCEEDED':'原生内容超出读取限制','NATIVE_CONTENT_UNSUPPORTED':'文档含尚不支持完整读取的内容','SOURCE_CHANGED_DURING_READ':'读取期间源内容发生变化','SOURCE_EMPTY':'新源内容为空，旧版本继续保留','LEGACY_UNVERIFIED':'历史失败尚未核实','STAGE_EXECUTION_ERROR':'本阶段未产出有效结果','RETIREMENT_IO_FAILED':'版本清理未完成','EXPORT_OUTCOME_UNKNOWN':'导出结果不确定，需要核实','STORAGE_QUOTA_EXCEEDED':'存储配额不足','PIPELINE_CHANGED':'处理配置变化，需要新的文档版本'}
STAGE.update({'question':'生成单条问题','question_index':'问题索引','summary_index':'摘要索引','index':'索引写入'})
ERROR.update({'QUESTION_OUTPUT_EMPTY':'模型未返回有效问题','CHUNK_PARENT_MAPPING_INVALID':'分块父子关系校验失败','NATIVE_TYPE_UNSUPPORTED':'暂不支持此类源文档','UPSTREAM_TEMPORARY':'来源接口暂时不可用'})
size_spec=importlib.util.spec_from_file_location('failure_sizes',Path(__file__).with_name('render-observability-failure-queries.py'))
size_module=importlib.util.module_from_spec(size_spec)
size_spec.loader.exec_module(size_module)


def query(view, detail):
    filters = [f"(${{{var}:sqlstring}}='' OR {column}::text=${{{var}:sqlstring}})" for var,column in
               [('workspace','c.tenant_id'),('knowledge_base','c.knowledge_base_id'),('source','c.datasource_id'),('run','c.run_id'),('status','c.status'),('stage','c.stage')]]
    filters[4] = "(${status:sqlstring} IN ('','all') OR c.status::text=${status:sqlstring})"
    filters.append("(${records:sqlstring}='all' OR (${records:sqlstring}='current' AND c.evidence_basis='verified_v2') OR (${records:sqlstring}='legacy' AND c.evidence_basis<>'verified_v2'))")
    filters.append("(${search:sqlstring}='' OR position(lower(${search:sqlstring}) in lower(concat_ws(' ',sc.document_title,c.title,c.external_id,c.job_id,sc.file_id,jc.file_id,sc.source_path,jc.source_path,lc.source_path,k.metadata->>'source_path')))>0)")
    if view == 'attempt_timeline':
        filters.append('$__timeFilter(c.observed_at)')
    fields = ["'job_id',job_id", "'knowledge_base_id',knowledge_base_id", "'datasource_id',datasource_id", "'source_revision',source_revision", "'revision',revision", "'publication_epoch',publication_epoch"]
    fields += [f"'{field}',{field}" for field in detail]
    fields += [f"'{field}',{field}" for field in ['run_id','original_error_ordinal','original_error_digest','linked_job_id','legacy_action','legacy_actor','legacy_evidence_at','legacy_dead_letter_id','legacy_run_status']]
    fields += ["'腾讯文档路径',display_source_path","'腾讯文件ID',display_file_id","'来源类型',display_source_type","'原文字节',display_file_bytes","'大小依据',display_size_basis","'原生响应字节',native_raw_bytes","'大小上限',display_limit_bytes","'至少已读取字节',display_lower_bytes"]
    rollup = view=='unresolved_incidents'
    if rollup:
        fields += ["'同类异常次数',incident_count","'同类异常事件ID',incident_event_ids"]
    step = "NULLIF(c.step_id,'')" if 'step_id' in detail else 'NULL::text'
    context_step = 'jc.active_step_id' if view=='current_document_lifecycle' else step
    same_attempt = 'sc.step_attempt=c.step_attempt' if 'step_attempt' in detail else 'TRUE'
    error = "NULLIF(c.error_code,'')" if 'error_code' in detail else 'NULL::text'
    stage_label = sql_label('stage',STAGE)
    state_label = sql_label('status',STATUS)
    reason = "CASE WHEN display_error_code LIKE 'TENCENT_%' THEN '腾讯接口错误：'||display_error_code ELSE "+sql_label('display_error_code',ERROR)+' END'
    size = "CASE WHEN display_file_bytes IS NULL AND native_raw_bytes IS NOT NULL THEN "+size_module.size_label('native_raw_bytes::text')+"||'（响应）' ELSE "+size_module.size_label('display_file_bytes::text')+' END'
    if view=='current_run_progress':
        columns=[('同步来源',"COALESCE(NULLIF(source_name,''),'未记录')"),('工作空间 / 知识库',"display_workspace||E'\\n'||kb_name"),('状态',state_label),('发现文档',"documents::text"),('已完成',"documents_succeeded::text"),('需处理',"(documents_failed+documents_blocked)::text"),('处理中',"documents_active::text"),('未准入',"documents_unadmitted::text"),('更新时间','observed_at'),('详情',f"jsonb_build_object({', '.join(fields)})::text")]
    else:
        columns=[('文档名称','display_title'),('腾讯文档路径',"COALESCE(NULLIF(display_source_path,''),CASE WHEN display_source_type='tencent_docs' THEN '未记录' ELSE '不适用' END)"),('文件 / 响应大小',size),('工作空间 / 知识库',"display_workspace||E'\\n'||kb_name"),('处理阶段',stage_label),('状态',state_label)]
        if view=='current_document_lifecycle':
            columns += [('阶段进度',"CASE WHEN steps_total IS NULL THEN '历史阶段未核实' ELSE steps_succeeded::text||' / '||steps_total::text END"),('可用版本',sql_label('availability',{'current_available':'当前版本可用','previous_available':'旧版本仍可用','not_published':'尚未发布','legacy_available':'历史版本可用','legacy_unverified':'历史待核实'}))]
        elif view=='attempt_timeline':
            columns += [('事件',sql_label('event_type',{'step_failed':'阶段失败','step_succeeded':'阶段完成','legacy_error':'历史失败','legacy_evidence':'后续核验证据','run_finished':'批次结束'})),('操作人',"COALESCE(NULLIF(actor,''),'—')")]
        else:
            columns += [('异常原因',reason)]
        if rollup:
            columns += [('异常次数','incident_count::text')]
        if view=='stage_retry_queue':
            columns += [('下次重试','next_run_at')]
        columns += [('记录时间','observed_at'),('腾讯原文','display_source_url'),('详情',f"jsonb_build_object({', '.join(fields)})::text")]
    projection=',\n '.join(f'{value} AS "{name}"' for name,value in columns)
    empty=['\'至少10001条，请缩小筛选范围\'']+[('NULL::timestamptz' if name in ('记录时间','更新时间','下次重试') else "''") for name,_ in columns[1:]]
    incident_key="c.tenant_id,c.knowledge_base_id,c.datasource_id,CASE WHEN c.kind='scan' THEN COALESCE(NULLIF(sc.file_id,''),NULLIF(c.step_id,''),c.row_id) ELSE COALESCE(NULLIF(c.job_id,''),c.row_id) END,c.stage,c.error_code"
    incident_fields=(f",count(*) OVER (PARTITION BY {incident_key}) AS incident_count,jsonb_agg(c.event_id) OVER (PARTITION BY {incident_key}) AS incident_event_ids,row_number() OVER (PARTITION BY {incident_key} ORDER BY c.observed_at DESC,c.row_id DESC) AS incident_rank" if rollup else '')
    return f"""-- All pages use this one result set. Above the bound, return an explicit
-- lower-bound notice, never a truncated list with a fabricated complete count.
WITH matching AS (
 SELECT c.*,
 COALESCE(NULLIF(sc.document_title,''),NULLIF(jc.document_title,''),NULLIF(c.title,''),NULLIF(c.source_name,''),'未记录文档名称') AS display_title,
 COALESCE(sc.source_path,jc.source_path,lc.source_path,k.metadata->>'source_path') AS display_source_path,
 COALESCE(sc.file_id,jc.file_id,k.metadata->>'file_id') AS display_file_id,
 COALESCE(sc.source_url,jc.source_url) AS display_source_url,
 COALESCE(jc.source_type,ds.type) AS display_source_type,
 COALESCE(jc.workspace_name,t.name,c.tenant_id::text) AS display_workspace,
 COALESCE(jc.file_bytes,CASE WHEN {same_attempt} AND sc.stage='download' THEN sc.actual_bytes END,lc.actual_bytes,CASE WHEN k.file_size>0 THEN k.file_size END) AS display_file_bytes,
 COALESCE(jc.size_basis,CASE WHEN {same_attempt} AND sc.stage='download' AND sc.actual_bytes IS NOT NULL THEN '本次下载失败记录' END,CASE WHEN lc.actual_bytes IS NOT NULL THEN '原始失败记录' END,CASE WHEN k.file_size>0 THEN '旧知识文件记录' END,CASE WHEN c.native_raw_bytes IS NOT NULL THEN '原生接口响应；不是导入文件大小' ELSE '未记录；不使用配额、上限或旧版本大小替代' END) AS display_size_basis,
 COALESCE(CASE WHEN {same_attempt} THEN sc.limit_bytes END,lc.limit_bytes) AS display_limit_bytes,
 COALESCE(CASE WHEN {same_attempt} THEN sc.observed_at_least_bytes END,lc.observed_at_least_bytes) AS display_lower_bytes,
 COALESCE({error},sc.error_code,'') AS display_error_code{incident_fields}
 FROM mwe_processing_{view} c
 LEFT JOIN processing_job_context jc ON jc.tenant_id=c.tenant_id AND jc.job_id=c.job_id
 LEFT JOIN processing_step_context sc ON sc.tenant_id=c.tenant_id AND sc.job_id=c.job_id AND sc.step_id={context_step}
 LEFT JOIN processing_legacy_context lc ON lc.tenant_id=c.tenant_id AND lc.datasource_id=c.datasource_id AND lc.run_id=c.run_id AND lc.error_ordinal=c.original_error_ordinal AND lc.error_digest=c.original_error_digest
 LEFT JOIN knowledges k ON COALESCE(c.job_id,'')='' AND c.original_error_ordinal IS NULL AND k.id=c.knowledge_id AND k.tenant_id=c.tenant_id AND k.knowledge_base_id=c.knowledge_base_id AND k.metadata->>'datasource_id'=c.datasource_id
 LEFT JOIN tenants t ON t.id=c.tenant_id
 LEFT JOIN data_sources ds ON ds.id=c.datasource_id AND ds.tenant_id=c.tenant_id AND ds.knowledge_base_id=c.knowledge_base_id
 WHERE {' AND '.join(filters)}
), candidates AS MATERIALIZED (
 SELECT * FROM matching{' WHERE incident_rank=1' if rollup else ''}
 ORDER BY observed_at DESC,row_id DESC LIMIT 10001
), gate AS (SELECT COUNT(*) AS n FROM candidates)
SELECT {projection}
FROM candidates c CROSS JOIN gate WHERE gate.n<=10000
UNION ALL
SELECT {','.join(empty)} FROM gate WHERE gate.n>10000;
"""


def outputs():
    panels = []
    result = {}
    for index,(view,(title,detail)) in enumerate(VIEWS.items()):
        sql = query(view,detail)
        result[GRAFANA / 'queries' / f'processing_{view}.sql'] = sql
        panels.append({
            'id': index+1, 'type':'table', 'title':title, 'gridPos':{'h':13,'w':24,'x':0,'y':index*13},
            'description':'按文档定位和复查。失败表按同一文档、阶段和错误合并，详情保留所有事件 ID，完整事件仍在时间线。腾讯路径使用当时源端扫描的选定范围，不采用 WeKnora 后来移动的目录；原文链接仅使用已记录的腾讯地址。大小以1024换算，标注“响应”时是原生接口返回量，不是文件大小；未知不使用配额、上限或旧版本替代。每表完整快照，在浏览器分页；超过10000条要求缩小筛选。旧记录通过“记录范围”单独查看，不改写其当时结果。',
            'datasource':{'type':'postgres','uid':'weknora-postgres'},
            'targets':[{'refId':'A','format':'table','rawQuery':True,'editorMode':'code','rawSql':sql,'datasource':{'type':'postgres','uid':'weknora-postgres'}}],
            'options':{'showHeader':True,'cellHeight':'sm','enablePagination':True,'footer':{'show':False,'enablePagination':True,'countRows':True,'reducer':['count'],'fields':''}},
            'fieldConfig':{'defaults':{'noValue':'未记录','custom':{'filterable':True,'minWidth':70,'wrapText':False}},'overrides':[
                {'matcher':{'id':'byName','options':'详情'},'properties':[{'id':'custom.width','value':70},{'id':'custom.inspect','value':True},{'id':'mappings','value':[{'type':'regex','options':{'pattern':'.*','result':{'text':'查看'}}}]}]},
                *[{'matcher':{'id':'byName','options':name},'properties':[{'id':'custom.width','value':width}]} for name,width in [('文档名称',190),('腾讯文档路径',230),('文件 / 响应大小',145),('工作空间 / 知识库',180),('处理阶段',105),('状态',90),('异常原因',230),('记录时间',165),('更新时间',165)]],
                *[{'matcher':{'id':'byName','options':name},'properties':[{'id':'custom.wrapText','value':True},{'id':'custom.inspect','value':True}]} for name in ['文档名称','腾讯文档路径','工作空间 / 知识库','异常原因']],
                {'matcher':{'id':'byName','options':'腾讯原文'},'properties':[{'id':'custom.width','value':80},{'id':'links','value':[{'title':'打开腾讯原文','url':'${__value.raw}','targetBlank':True}]},{'id':'mappings','value':[{'type':'regex','options':{'pattern':'^https://docs[.]qq[.]com/.*','result':{'text':'打开'}}}]}]},
            ]},
        })
    variables=[]
    choices={
      'workspace':('工作空间',"SELECT '全部工作空间' AS __text,'' AS __value UNION ALL SELECT name||' ('||id::text||')',id::text FROM tenants ORDER BY __text"),
      'knowledge_base':('知识库',"SELECT '全部知识库' AS __text,'' AS __value UNION ALL SELECT name,id FROM knowledge_bases WHERE deleted_at IS NULL AND (${workspace:sqlstring}='' OR tenant_id::text=${workspace:sqlstring}) ORDER BY __text"),
      'source':('数据源',"SELECT '全部数据源' AS __text,'' AS __value UNION ALL SELECT name,id FROM data_sources WHERE deleted_at IS NULL AND (${workspace:sqlstring}='' OR tenant_id::text=${workspace:sqlstring}) AND (${knowledge_base:sqlstring}='' OR knowledge_base_id::text=${knowledge_base:sqlstring}) ORDER BY __text"),
    }
    for name,(label,sql) in choices.items():
        variables.append({'name':name,'label':label,'type':'query','datasource':{'type':'postgres','uid':'weknora-postgres'},'query':sql,'definition':sql,'refresh':1,'current':{'text':'全部'+label,'value':''},'options':[],'hide':0,'skipUrlSync':False})
    variables += [{'name':name,'label':label,'type':'textbox','query':'','current':{'text':'','value':''},'skipUrlSync':False,'hide':0} for name,label in [('search','文件名 / 腾讯路径 / ID'),('run','批次 ID'),('stage','阶段代码')]]
    for name,label,options,selected in [('records','记录范围',[('新流程','current'),('历史待核实','legacy'),('全部','all')],'current'),('status','状态',[('全部','all')]+[(label,key) for key,label in STATUS.items()],'all')]:
        variables.append({'name':name,'label':label,'type':'custom','query':','.join(text+' : '+value for text,value in options),'current':{'text':next(text for text,value in options if value==selected),'value':selected},'options':[{'text':text,'value':value,'selected':value==selected} for text,value in options],'hide':0,'skipUrlSync':False})
    by_view=dict(zip(VIEWS,panels))
    primary=[]
    for view,title,y in [('unresolved_incidents','失败文档复查',0),('current_document_lifecycle','文档处理进度',14),('current_run_progress','同步批次概览',28)]:
        panel=by_view[view];panel['title']=title;panel['gridPos']={'h':14,'w':24,'x':0,'y':y};primary.append(panel)
    extra=[by_view[name] for name in ['stage_retry_queue','attempt_timeline','lifecycle_inconsistencies']]
    for i,panel in enumerate(extra):panel['gridPos']={'h':13,'w':24,'x':0,'y':43+i*13}
    primary.append({'id':1000,'type':'row','title':'阶段重试、事件和一致性明细','collapsed':True,'gridPos':{'h':1,'w':24,'x':0,'y':42},'panels':extra})
    dashboard = {'uid':'mwe-processing-lifecycle-v2','title':'WeKnora · 文档处理与失败复查','description':'默认只看新流程。先查看失败文件，再看处理进度和批次；技术明细默认折叠。历史记录可切换范围复查。仅事件时间线使用右上角日期范围，未解决异常不按时间隐藏。',
                 'schemaVersion':39,'version':2,'editable':False,'refresh':'','timezone':'browser','time':{'from':'now-7d','to':'now'},'tags':['weknora','lifecycle','admin'],
                 'panels':primary,'templating':{'list':variables},'links':[{'title':'运行总览与历史复查','type':'link','url':'/d/mwe-kb-observability-v1','targetBlank':False},{'title':'WeKnora 处理操作','type':'link','url':'https://kb.mwexk.com/platform/settings?section=processing-history','targetBlank':True}]}
    result[GRAFANA / 'dashboards' / 'processing-lifecycle.json'] = json.dumps(dashboard,ensure_ascii=False,indent=2)+'\n'
    # The existing operations homepage links to the frozen detail lists and
    # shows compact live counts. Keep its infrastructure panels and label the
    # historical summary tables explicitly instead of mixing their evidence.
    home_path = GRAFANA / 'dashboards' / 'knowledgebase-overview.json'
    home = json.loads(home_path.read_text(encoding='utf-8'))
    def flatten(items):
        for panel in items:
            if panel['type']=='row':yield from flatten(panel.get('panels',[]))
            else:yield panel
    retained={p['id']:p for p in flatten(home['panels']) if p['id'] not in range(101,108)}
    home['panels']=[]
    summary=[('current_run_progress','新流程同步批次'),('current_document_lifecycle','新流程文档'),('unresolved_incidents','待复查异常事件'),('stage_retry_queue','活动 / 待处理阶段')]
    for index,(view,title) in enumerate(summary):
        where=" WHERE evidence_basis='verified_v2'"
        home['panels'].append({'id':101+index,'type':'stat','title':title,
            'description':'来自统一生命周期视图。计数单位是视图行，不把目录、文档、尝试或异常事件混算。完整明细及冻结分页见处理生命周期看板。',
            'gridPos':{'h':4,'w':6,'x':index*6,'y':0},'datasource':{'type':'postgres','uid':'weknora-postgres'},
            'targets':[{'refId':'A','format':'table','rawQuery':True,'rawSql':f'SELECT COUNT(*)::bigint AS total FROM mwe_processing_{view}{where}'}],
            'options':{'reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False},'textMode':'auto','colorMode':'value','graphMode':'none'},
            'fieldConfig':{'defaults':{'noValue':'读取状态未知','min':0,'color':{'mode':'thresholds'} if view=='unresolved_incidents' else {'mode':'fixed','fixedColor':'blue'},'thresholds':{'mode':'absolute','steps':[{'color':'green','value':None},{'color':'red','value':1}]}},'overrides':[]}})
    home['panels'].append({'id':107,'type':'text','title':'定位失败文档','gridPos':{'h':3,'w':24,'x':0,'y':4},
        'options':{'mode':'markdown','content':'[打开文档处理与失败复查看板](/d/mwe-processing-lifecycle-v2) · 按工作空间、知识库和腾讯路径定位文件，查看大小、失败阶段和原因。历史流程记录保留在本页底部的折叠区。'}})
    for id,x in [(6,0),(7,12)]:retained[id]['gridPos']={'h':8,'w':12,'x':x,'y':7};home['panels'].append(retained[id])
    for id,x in [(1,0),(2,12)]:retained[id]['gridPos']={'h':4,'w':12,'x':x,'y':15};home['panels'].append(retained[id])
    infra=[]; y=20
    for group in [[14,15,16,17],[18,19],[8,20,21,22],[23],[13]]:
        for id in group:
            p=retained[id];p['gridPos']['y']=y;infra.append(p)
        y+=max(retained[id]['gridPos']['h'] for id in group)
    home['panels'].append({'id':2001,'type':'row','title':'硬件趋势与服务日志','collapsed':True,'gridPos':{'h':1,'w':24,'x':0,'y':19},'panels':infra})
    legacy=[]
    for id,x,y,w,h in [(10,0,20,24,13),(43,0,33,24,2),(11,0,35,12,10),(12,12,35,12,10),(44,0,45,12,2),(45,12,45,12,2),(3,0,47,8,4),(4,8,47,8,4),(5,16,47,8,4),(9,0,51,24,8)]:
        p=retained[id];p['gridPos']={'h':h,'w':w,'x':x,'y':y+1};legacy.append(p)
    home['panels'].append({'id':2002,'type':'row','title':'历史流程复查（保留原始记录）','collapsed':True,'gridPos':{'h':1,'w':24,'x':0,'y':20},'panels':legacy})
    home['title']='WeKnora · 运行总览'
    home['version']=max(home.get('version',1),18)
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
