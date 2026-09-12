"""File-source panels layered onto the existing overview generator."""
PG={'type':'postgres','uid':'weknora-postgres'}
PROM={'type':'prometheus','uid':'prometheus'}

def configure(home):
    panels={p['id']:p for p in home['panels']}
    up='(redis_up{host="marvel-kb",job="integrations/redis"} == 1)'
    fresh='(time() - timestamp(redis_up{host="marvel-kb",job="integrations/redis"}) < 60)'
    for pid,title,pattern in [(104,'实际后台执行任务','active'),(110,'普通后台排队任务','pending|scheduled|retry')]:
        p=panels[pid]
        p['title'],p['datasource']=title,PROM
        exclude=',key!~"asynq:[{]sync_retry[}]:.*"' if pid==110 else ''
        expr=f'(sum(redis_key_size{{host="marvel-kb",job="integrations/redis",key=~"asynq:.*:({pattern})"{exclude}}}) or vector(0)) and on() {up} and on() {fresh}'
        p['targets']=[{'refId':'A','expr':expr,'instant':True,'range':False}]
        p['description']='Redis 中实际 Asynq 任务数量；任务可能执行网络或 CPU 工作，不等于 GPU 推理。监控不可用或超过60秒未更新时显示未知。'
    cards={
      111:('待补试文档',"SELECT count(*) AS total FROM tencent_file_failures WHERE state='scheduled'",'同源正常文件获取和提交完成后补试，每文件最多一次；一个补试任务可以处理多个文件。'),
      103:('源文件最终失败 / 待核对',"SELECT count(*) AS total FROM tencent_file_failures WHERE state IN ('needs_manual','exhausted')",'永久拒绝、补试仍失败及未知导出；后续同步不自动重开这些文件。'),
      102:('知识文件（含候选与已完成）',"SELECT count(*) AS total FROM knowledge_file_inventory",'数据库知识文件数量，包含后台处理中和候选文件，不是并发数。'),
      101:('源同步批次（含已结束）',"SELECT count(*) AS total FROM tencent_file_runs",'源文件获取提交的批次记录，不代表解析、索引、摘要已结束。'),
    }
    for pid,(title,sql,description) in cards.items():
        p=panels[pid];p.update(title=title,description=description,datasource=PG)
        p['targets']=[{'refId':'A','format':'table','rawQuery':True,'rawSql':sql}]
    if 107 in panels:
        panels[107]['options']['content']='**正常源同步与补试分开。** 每个数据源本轮正常文件导出、下载并提交 WeKnora 后，才补试该源失败文件一次。解析、图片、向量和摘要使用原有流程。后台活动任务包括网络/CPU工作；GPU请求单独统计。目录树来自本地路径，不调用腾讯 MCP。'
    variables=home.setdefault('templating',{}).setdefault('list',[])
    variables[:]=[v for v in variables if v.get('name')!='td_source']
    query="SELECT '全部腾讯数据源' AS __text,'' AS __value UNION ALL SELECT name,id FROM data_sources WHERE type='tencent_docs' AND deleted_at IS NULL"
    variables.append({'name':'td_source','label':'腾讯源文件筛选','type':'query','datasource':PG,'query':query,'definition':query,'refresh':1,'multi':False,'includeAll':False,'current':{'text':'全部腾讯数据源','value':''},'options':[]})
    home['panels']=[p for p in home['panels'] if p['id'] not in (120,121,122,123)]
    y=max(p['gridPos']['y']+p['gridPos']['h'] for p in home['panels'])
    home['panels'].append({'id':120,'type':'row','title':'腾讯文件同步、补试与最终失败','collapsed':False,'panels':[],'gridPos':{'x':0,'y':y,'w':24,'h':1}})
    where="(${td_source:sqlstring}='' OR datasource_id=${td_source:sqlstring})"
    queries=[
      (121,'文件补试和永久失败',f'''SELECT source_name AS "数据源",kb_name AS "知识库",concat_ws('/',NULLIF(folder_path,''),title) AS "腾讯文档路径",file_id AS "文件ID",
CASE WHEN category='EXPORT_START_UNCERTAIN' THEN '需核对已有导出' WHEN state='scheduled' THEN '待补试：等待同源正常获取结束' WHEN state='running' THEN '正在补试' WHEN state='exhausted' THEN '补试已用尽：永久失败' ELSE '永久失败：需人工解除' END AS "状态",
attempt AS "已补试次数",stage AS "失败阶段",category AS "错误类别",error_message AS "具体错误",first_error AS "首次错误",error_at AS "最近错误记录"
FROM tencent_file_failures WHERE {where} ORDER BY error_at DESC NULLS LAST,datasource_id,external_id LIMIT 1000'''),
      (122,'源文件获取与提交记录',f'''SELECT source_name AS "数据源",kb_name AS "知识库",CASE WHEN retry_of IS NULL OR retry_of='' THEN '正常获取' ELSE '同源补试' END AS "阶段",status AS "记录状态",items_total AS "本次处理",items_created AS "已提交新文件",items_updated AS "已提交更新",items_failed AS "失败",started_at AS "开始时间",finished_at AS "获取提交结束",error_message AS "错误说明",run_id AS "同步记录ID"
FROM tencent_file_runs WHERE {where} ORDER BY started_at DESC LIMIT 1000'''),
      (123,'提交后的 WeKnora 文件处理',f'''SELECT title AS "文档",folder_path AS "目录",parse_status AS "解析索引状态",summary_status AS "摘要状态",CASE WHEN unpublished THEN '候选：尚未发布' ELSE '已发布/普通文件' END AS "发布状态",error_message AS "处理错误",updated_at AS "更新时间",file_id AS "腾讯文件ID",id AS "知识ID"
FROM tencent_file_knowledge WHERE {where} ORDER BY updated_at DESC LIMIT 1000'''),
    ]
    for pid,title,sql in queries:
        y+=1
        home['panels'].append({'id':pid,'type':'table','title':title,'description':'按时间排序，最多显示1000条，可按腾讯数据源筛选。补试/永久错误来自持久游标，不受同步日志100条错误样本上限影响。','gridPos':{'x':0,'y':y,'w':24,'h':10},'datasource':PG,'targets':[{'refId':'A','format':'table','rawQuery':True,'rawSql':sql}],'options':{'showHeader':True,'cellHeight':'sm','footer':{'show':False},'enablePagination':True},'fieldConfig':{'defaults':{'custom':{'align':'auto','cellOptions':{'type':'auto'}}},'overrides':[]}})
        y+=10
