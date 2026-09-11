<script setup lang="ts">
import { ref, computed, watch, onBeforeUnmount } from 'vue'
import { useI18n } from 'vue-i18n'
import { useAuthStore } from '@/stores/auth'
import { getProcessingJob, getProcessingHistory, getProcessingHistoryPage, appendHistoryPage, controlProcessingJob, type JobDetail, type ProcessingAction, type ProcessingStep, type HistoryPage } from '@/api/processing'

const props = defineProps<{ kbId?: string; jobId: string }>()
const emit = defineEmits<{ changed: []; close: [] }>()
const { t, te } = useI18n()
const auth = useAuthStore()
const detail = ref<JobDetail>()
const stepPage = ref(0)
const visibleSteps = computed(() => detail.value?.steps.slice(stepPage.value * 50, stepPage.value * 50 + 50) ?? [])
const events = ref<HistoryPage>()
const loading = ref(false)
const error = ref('')
const notice = ref('')
const action = ref<ProcessingAction>()
const step = ref<ProcessingStep>()
const reason = ref('')
const taskId = ref('')
const notStarted = ref(false)
const evidence = ref('')
const busy = ref(false)
const submitted = ref<Record<string, unknown>>()
let request = 0
let actionRequest = 0
let operationID = ''
let expectedRevision = 0
const label = (value: unknown) => typeof value === 'boolean' ? t(value ? 'processing.yes' : 'processing.no') : value == null || value === '' ? '—' : te(`processing.${value}`) ? t(`processing.${value}`) : String(value)
const time = (value?: unknown) => value ? new Date(String(value)).toLocaleString() : '—'
const canManage = computed(() => auth.hasRole('admin') && detail.value?.job.tenant_id === auth.effectiveTenantId)
watch(canManage, value => { if (!value) { ++actionRequest; action.value = undefined; submitted.value = undefined; busy.value = false } })
const intent = computed(() => detail.value?.external_intents?.find(item => item.step_id === step.value?.id))

async function refresh(refreshEvents = true) {
  const serial = ++request
  loading.value = true; error.value = ''
  try {
    const value = await getProcessingJob(props.kbId, props.jobId)
    if (serial !== request) return
    detail.value = value
    stepPage.value = Math.min(stepPage.value, Math.max(0, Math.ceil(value.steps.length / 50) - 1))
    if (refreshEvents) {
      const history = await getProcessingHistory(props.kbId, { view: 'attempt_timeline', job_id: props.jobId, limit: 50 })
      if (serial === request) events.value = history
    }
  } catch { if (serial === request) error.value = t('processing.loadingError') }
  finally { if (serial === request) loading.value = false }
}
watch(() => [props.kbId, props.jobId, auth.selectedTenantId], () => { ++actionRequest; stepPage.value = 0; busy.value = false; submitted.value = undefined; detail.value = undefined; events.value = undefined; action.value = undefined; notice.value = ''; refresh() }, { immediate: true })
const timer = setInterval(() => {
  if (!loading.value && !action.value && stepPage.value === 0 && detail.value?.steps.some(item => ['planned','enqueue_pending','queued','running','waiting_external','retry_wait'].includes(item.status))) refresh(false)
}, 5000)
onBeforeUnmount(() => { ++request; ++actionRequest; clearInterval(timer) })

async function moreEvents() {
  if (!events.value || loading.value) return
  const current = events.value, serial = request
  loading.value = true
  try {
    const next = await getProcessingHistoryPage(props.kbId, current)
    if (serial === request) events.value = appendHistoryPage(current, next)
  } catch { if (serial === request) error.value = t('processing.expired') }
  finally { if (serial === request) loading.value = false }
}
function choose(value: ProcessingAction, target?: ProcessingStep) {
  if (!canManage.value || busy.value) return
  action.value = value; step.value = target; reason.value = ''; taskId.value = ''; evidence.value = ''; notStarted.value = false
  notice.value = ''; submitted.value = undefined; operationID = crypto.randomUUID(); expectedRevision = detail.value!.job.revision
}
async function submit() {
  if (!canManage.value || !detail.value || !action.value || busy.value || !reason.value.trim()) return
  if (action.value === 'resolve-export' && (!intent.value || !evidence.value.trim() || (!notStarted.value && !taskId.value.trim()))) return
  submitted.value ??= { expected_revision: expectedRevision, operation_request_id: operationID, reason: reason.value.trim(),
    ...(step.value ? { step_id: step.value.id } : {}),
    ...(action.value === 'pin' ? { pinned: !detail.value.job.rollback_pin } : {}),
    ...(action.value === 'resolve-export' ? { ...intent.value, task_id: notStarted.value ? '' : taskId.value.trim(), not_started: notStarted.value, evidence_reference: evidence.value.trim() } : {}) }
  const serial = ++actionRequest
  busy.value = true; error.value = ''
  try {
    await controlProcessingJob(props.kbId, props.jobId, action.value, submitted.value)
    if (serial !== actionRequest) return
    action.value = undefined; notice.value = t('processing.done'); emit('changed'); await refresh()
  } catch { if (serial === actionRequest) error.value = t('processing.actionError') }
  finally { if (serial === actionRequest) busy.value = false }
}
</script>

<template>
  <section class="processing-detail" :aria-busy="loading">
    <div class="toolbar"><h3>{{ detail?.document.title || t('processing.detail') }}</h3><t-button variant="outline" :loading="loading" @click="refresh(true)">{{ t('processing.refresh') }}</t-button><t-button variant="text" @click="emit('close')">{{ t('processing.close') }}</t-button></div>
    <t-alert v-if="error" theme="error" :message="error" role="alert" />
    <t-alert v-if="notice" theme="success" :message="notice" />
    <template v-if="detail">
      <dl class="facts">
        <template v-for="[key, value] in Object.entries({ version: detail.job.generation, status: detail.job.status, current: detail.job.is_current, published: detail.job.is_published, readiness: detail.job.readiness, completeness: detail.job.completeness, retirement: detail.job.retirement_state, publicationEpoch: detail.job.publication_epoch })" :key="key"><dt>{{ t(`processing.${key}`) }}</dt><dd>{{ label(value) }}</dd></template>
      </dl>
      <div v-if="canManage && kbId" class="toolbar">
        <t-button variant="outline" :disabled="!detail.job.is_current" @click="choose('rebuild')">{{ t('processing.rebuild') }}</t-button>
        <t-button variant="outline" :disabled="!detail.job.is_current || ['succeeded','superseded','canceled'].includes(detail.job.status)" @click="choose('cancel')">{{ t('processing.cancel') }}</t-button>
        <t-button variant="outline" :disabled="detail.job.retirement_state !== 'retained'" @click="choose('pin')">{{ t(detail.job.rollback_pin ? 'processing.unpin' : 'processing.pin') }}</t-button>
        <t-button variant="outline" :disabled="detail.job.is_published || detail.job.retirement_state !== 'retained'" @click="choose('rollback')">{{ t('processing.rollback') }}</t-button>
        <t-button variant="outline" :disabled="detail.job.is_current || detail.job.is_published || detail.job.rollback_pin" @click="choose('retire')">{{ t('processing.retire') }}</t-button>
      </div>
      <p class="muted">{{ t('processing.operatorNotice') }}</p>
      <div class="table-wrap"><table><thead><tr><th>{{ t('processing.stage') }}</th><th>{{ t('processing.status') }}</th><th>{{ t('processing.attempt') }}</th><th>{{ t('processing.nextRetry') }}</th><th>{{ t('processing.heartbeat') }}</th><th>{{ t('processing.error') }}</th><th>{{ t('processing.detail') }}</th></tr></thead><tbody>
        <tr v-for="item in visibleSteps" :key="item.id"><td>{{ item.stage }}<small>{{ item.unit_key }}</small><small v-if="item.required_for_ready">{{ t('processing.readyRequired') }}</small><small v-if="item.required_for_completion">{{ t('processing.completionRequired') }}</small></td><td>{{ label(item.status) }}</td><td>{{ item.step_attempt }}<small>{{ t('processing.retryBudget') }}: {{ item.retry_count }} / {{ item.max_retries }}</small></td><td>{{ time(item.next_run_at) }}<small>{{ t('processing.deadline') }}: {{ time(item.deadline_at) }}</small></td><td>{{ time(item.heartbeat_at) }}</td><td>{{ item.error_code }}<small>{{ item.error_message }}</small></td><td>
          <t-button v-if="canManage && ['blocked','failed','retry_wait'].includes(item.status) && item.error_code !== 'EXPORT_START_UNCERTAIN'" size="small" variant="outline" @click="choose('retry', item)">{{ t('processing.retry') }}</t-button>
          <t-button v-if="canManage && item.error_code === 'EXPORT_START_UNCERTAIN'" size="small" variant="outline" @click="choose('resolve-export', item)">{{ t('processing.resolve') }}</t-button>
          <small v-if="item.result?.retained != null">{{ t('processing.retainedVersions') }}: {{ item.result.retained }}</small>
        </td></tr>
      </tbody></table></div>
      <div v-if="detail.steps.length > 50" class="toolbar"><t-button :disabled="stepPage === 0" variant="outline" @click="stepPage--">{{ t('processing.previous') }}</t-button><span>{{ t('processing.stepPage', { page: stepPage + 1, pages: Math.ceil(detail.steps.length / 50), count: detail.steps.length }) }}</span><t-button :disabled="(stepPage + 1) * 50 >= detail.steps.length" variant="outline" @click="stepPage++">{{ t('processing.next') }}</t-button></div>
      <form v-if="canManage && action" class="action-form" @submit.prevent="submit">
        <h4>{{ label(action === 'resolve-export' ? 'resolve' : action === 'pin' && detail.job.rollback_pin ? 'unpin' : action) }}</h4>
        <label>{{ t('processing.reason') }}<textarea v-model="reason" required maxlength="160" :disabled="!!submitted || busy" rows="2" /></label>
        <template v-if="action === 'resolve-export'">
          <p>{{ t('processing.exportNotice') }}</p><code>{{ intent?.file_id }} · {{ intent?.source_revision }}</code>
          <label><input v-model="notStarted" type="checkbox" :disabled="!!submitted || busy" /> {{ t('processing.notStarted') }}</label>
          <label v-if="!notStarted">{{ t('processing.taskId') }}<input v-model="taskId" required maxlength="256" :disabled="!!submitted || busy" /></label>
          <label>{{ t('processing.evidence') }}<input v-model="evidence" required maxlength="160" :disabled="!!submitted || busy" /></label>
        </template>
        <div class="toolbar"><t-button type="submit" :loading="busy">{{ t('processing.submit') }}</t-button><t-button variant="text" :disabled="busy" @click="action = undefined">{{ t('processing.dismiss') }}</t-button></div>
      </form>
      <h4>{{ t('processing.attempt_timeline') }}</h4><p class="muted">{{ t('processing.timelineWindow') }}</p>
      <div v-if="events" class="table-wrap"><table><thead><tr><th>{{ t('processing.snapshot', { time: '' }) }}</th><th>{{ t('processing.stage') }}</th><th>{{ t('processing.status') }}</th><th>{{ t('processing.detail') }}</th></tr></thead><tbody><tr v-for="event in events.rows" :key="event.row_id"><td>{{ time(event.observed_at) }}</td><td>{{ event.stage }}</td><td>{{ label(event.status) }}</td><td>{{ event.event_type }} {{ event.error_code }} {{ event.resolution_type }}</td></tr></tbody></table></div>
      <t-button v-if="events?.has_more" :loading="loading" variant="outline" @click="moreEvents">{{ t('processing.more') }}</t-button>
    </template>
  </section>
</template>

<style scoped>
.processing-detail { display: grid; gap: 16px; min-width: 0; color: var(--td-text-color-primary); }
.toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; } h3 { margin: 0; flex: 1; } h4, p { margin: 0; }
.facts { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin: 0; } dt, .muted, small { color: var(--td-text-color-secondary); } dd { margin: 0; overflow-wrap: anywhere; }
.table-wrap { max-width: 100%; overflow: auto; } table { border-collapse: collapse; width: 100%; font-size: 13px; } th, td { text-align: left; vertical-align: top; padding: 10px; border-bottom: 1px solid var(--td-component-stroke); } small { display: block; margin-top: 4px; overflow-wrap: anywhere; } th { white-space: nowrap; }
.table-wrap td:nth-child(2) { white-space: nowrap; }
.action-form { display: grid; gap: 12px; padding: 16px; background: var(--td-bg-color-secondarycontainer); border-radius: 8px; } label { display: grid; gap: 6px; } input:not([type=checkbox]), textarea { font: inherit; padding: 8px; color: inherit; background: var(--td-bg-color-container); border: 1px solid var(--td-component-stroke); border-radius: 4px; } code { overflow-wrap: anywhere; }
@media (max-width: 650px) { .facts { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
</style>
