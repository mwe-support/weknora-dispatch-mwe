<script setup lang="ts">
import { ref, watch, onBeforeUnmount } from 'vue'
import { useI18n } from 'vue-i18n'
import { getProcessingHistory, getProcessingHistoryPage, appendHistoryPage, type HistoryPage, type HistoryRow, type HistoryView } from '@/api/processing'
import ProcessingJobDetails from './ProcessingJobDetails.vue'
import { useAuthStore } from '@/stores/auth'

const props = defineProps<{ kbId?: string; sourceId?: string }>()
const { t, te } = useI18n()
const auth = useAuthStore()
const views: HistoryView[] = ['current_run_progress','current_document_lifecycle','stage_retry_queue','unresolved_incidents','attempt_timeline','lifecycle_inconsistencies']
const view = ref<HistoryView>('current_document_lifecycle')
const search = ref(''), status = ref(''), stage = ref(''), from = ref(''), to = ref('')
const sourceFilter = ref(''), runFilter = ref('')
const page = ref<HistoryPage>()
const loading = ref(false), error = ref(''), selected = ref('')
let request = 0
const label = (value: unknown) => value == null || value === '' ? '—' : te(`processing.${value}`) ? t(`processing.${value}`) : String(value)
const formatTime = (value: string) => new Date(value).toLocaleString()
const metrics = (row: HistoryRow) => view.value === 'current_run_progress'
  ? [['countDocuments', row.documents], ['countContainers', row.containers], ['countLinks', row.external_links], ['countUnadmitted', row.documents_unadmitted], ['originalResult', row.original_finished_status]]
  : [['readiness', row.readiness], ['completeness', row.completeness], ['docType', row.doc_type], ['nativeBytes', row.native_raw_bytes], ['nativePages', row.native_pages], ['nativeUnits', row.native_units], ['images', row.images], ['mediaBytes', row.media_bytes], ['retirement', row.retirement_state], ['cleanupProblems', row.cleanup_problem], ['resources', row.resources_active], ['chargedBytes', row.charged_bytes], ['estimatedIndexBytes', row.estimated_index_bytes]]
async function refresh() {
  const serial = ++request
  loading.value = true; error.value = ''; page.value = undefined
  try {
    const result = await getProcessingHistory(props.kbId, { view: view.value, source_id: props.sourceId || sourceFilter.value, run_id: runFilter.value, search: search.value, status: status.value, stage: stage.value, from: from.value ? new Date(from.value).toISOString() : undefined, to: to.value ? new Date(to.value).toISOString() : undefined, limit: 50 })
    if (serial === request) page.value = result
  } catch (e: any) { if (serial === request) error.value = t(e?.code === 'FILTER_REQUIRED' ? 'processing.filterRequired' : 'processing.loadingError') }
  finally { if (serial === request) loading.value = false }
}
async function more() {
  if (!page.value || loading.value) return
  const current = page.value, serial = request
  loading.value = true; error.value = ''
  try { const next = await getProcessingHistoryPage(props.kbId, current); if (serial === request) page.value = appendHistoryPage(current, next) }
  catch (e: any) { if (serial === request) error.value = t(e?.code === 'SNAPSHOT_EXPIRED' ? 'processing.expired' : 'processing.loadingError') }
  finally { if (serial === request) loading.value = false }
}
watch(() => [props.kbId, props.sourceId, view.value, auth.selectedTenantId], () => { selected.value = ''; refresh() }, { immediate: true })
onBeforeUnmount(() => { ++request })
</script>

<template>
  <section class="processing-history" :aria-busy="loading">
    <ProcessingJobDetails v-if="selected" :kb-id="kbId" :job-id="selected" @close="selected = ''; refresh()" />
    <template v-else>
      <form class="filters" @submit.prevent="refresh">
        <label>{{ t('processing.title') }}<select v-model="view"><option v-for="item in views" :key="item" :value="item">{{ t(`processing.${item}`) }}</option></select></label>
        <label>{{ t('processing.search') }}<input v-model="search" maxlength="200" /></label>
        <label v-if="!sourceId">{{ t('processing.sourceId') }}<input v-model="sourceFilter" maxlength="64" /></label>
        <label>{{ t('processing.runId') }}<input v-model="runFilter" maxlength="64" /></label>
        <label>{{ t('processing.status') }}<input v-model="status" maxlength="64" :placeholder="t('processing.all')" list="processing-states" /></label>
        <datalist id="processing-states"><option v-for="item in ['planned','enqueue_pending','queued','running','waiting_external','retry_wait','succeeded','blocked','failed','canceled','superseded','skipped']" :key="item" :value="item">{{ label(item) }}</option></datalist>
        <label>{{ t('processing.stage') }}<input v-model="stage" maxlength="64" :placeholder="t('processing.all')" /></label>
        <label>{{ t('processing.from') }}<input v-model="from" type="datetime-local" /></label><label>{{ t('processing.to') }}<input v-model="to" type="datetime-local" /></label>
        <t-button type="submit" :loading="loading">{{ t('processing.refresh') }}</t-button>
      </form>
      <p class="muted">{{ t('processing.timelineWindow') }}</p>
      <t-alert v-if="error" theme="error" :message="error" role="alert" />
      <template v-if="page">
        <div class="summary"><span>{{ t('processing.loaded', { loaded: page.rows.length, total: page.total }) }}</span><span>{{ t('processing.snapshot', { time: formatTime(page.created_at) }) }}</span></div>
        <p v-if="!page.total">{{ t('processing.empty') }}</p>
        <div v-else class="table-wrap"><table><thead><tr><th>{{ t('processing.document') }}</th><th>{{ t('processing.version') }}</th><th>{{ t('processing.status') }}</th><th>{{ t('processing.stage') }}</th><th>{{ t('processing.detail') }}</th></tr></thead><tbody>
          <tr v-for="row in page.rows" :key="row.row_id"><td><button v-if="row.job_id" class="document-link" @click="selected = row.job_id">{{ row.title || row.job_id }}</button><span v-else>{{ row.title }}</span><small>{{ row.folder_path }}</small><small>{{ row.evidence_basis === 'legacy_unverified' ? t('processing.legacy_unverified') : row.source_name }}</small></td><td>{{ row.generation ?? '—' }}<small>{{ label(row.availability) }}</small></td><td>{{ label(row.status) }}<small>{{ row.error_code }}</small><small>{{ row.event_type }} {{ row.resolution_type }}</small></td><td>{{ row.stage }}<small v-if="row.step_attempt != null">{{ t('processing.attempt') }}: {{ row.step_attempt }}</small><small v-if="row.next_run_at">{{ t('processing.nextRetry') }}: {{ formatTime(String(row.next_run_at)) }}</small></td><td><small v-for="[key, value] in metrics(row)" :key="String(key)">{{ t(`processing.${key}`) }}: {{ label(value) }}</small></td></tr>
        </tbody></table></div>
        <t-button v-if="page.has_more" variant="outline" :loading="loading" @click="more">{{ t('processing.more') }}</t-button>
      </template>
    </template>
  </section>
</template>

<style scoped>
.processing-history { display: grid; gap: 16px; color: var(--td-text-color-primary); min-width: 0; } .filters { display: flex; flex-wrap: wrap; align-items: end; gap: 12px; } label { display: grid; gap: 6px; font-size: 12px; } input, select { max-width: 220px; font: inherit; padding: 8px; border: 1px solid var(--td-component-stroke); border-radius: 4px; background: var(--td-bg-color-container); color: inherit; } .summary { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 8px; } p { margin: 0; } .muted, small { color: var(--td-text-color-secondary); } small { display: block; margin-top: 5px; overflow-wrap: anywhere; } .table-wrap { overflow: auto; } table { width: 100%; border-collapse: collapse; font-size: 13px; } td, th { text-align: left; vertical-align: top; padding: 12px 8px; border-bottom: 1px solid var(--td-component-stroke); } th { white-space: nowrap; } .document-link { color: var(--td-brand-color); border: 0; background: none; padding: 0; text-align: left; font: inherit; cursor: pointer; }
</style>
