import { get, post } from '@/utils/request'
export { appendHistoryPage } from '@/utils/processingHistory'

export type HistoryView = 'current_run_progress' | 'current_document_lifecycle' | 'stage_retry_queue' | 'unresolved_incidents' | 'attempt_timeline' | 'lifecycle_inconsistencies'
export interface HistoryRow {
  row_id: string; job_id: string; knowledge_id: string; knowledge_base_id: string; datasource_id: string
  title: string; status: string; stage: string; generation?: number; revision?: number; evidence_basis?: string
  [key: string]: unknown
}
export interface HistoryPage {
  snapshot_id: string; filter_digest: string; view: HistoryView; total: number; created_at: string; expires_at: string
  rows: HistoryRow[]; next_after: number; has_more: boolean
}
export interface ProcessingStep {
  id: string; stage: string; unit_key: string; phase: string; status: string; step_attempt: number; retry_count: number
  next_run_at?: string; heartbeat_at?: string; error_code?: string; error_message?: string; max_retries: number; deadline_at?: string
  required_for_ready: boolean; required_for_completion: boolean; result?: Record<string, unknown>
}
export interface JobDetail {
  job: { id: string; tenant_id: number; knowledge_base_id: string; revision: number; generation: number; status: string; readiness: string; completeness: string; is_current: boolean; is_published: boolean; retirement_state: string; rollback_pin: boolean; source_revision: string; publication_epoch: number }
  document: { title: string; file_id: string; kind: string }
  steps: ProcessingStep[]
  external_intents: { step_id: string; file_id: string; source_revision: string; request_digest: string }[]
}
const base = (kb?: string) => kb ? `/api/v1/knowledge-bases/${encodeURIComponent(kb)}/processing/jobs` : '/api/v1/processing/jobs'
export const getProcessingHistory = (kb: string | undefined, params: Record<string, unknown>) => get<HistoryPage>(base(kb), { params })
export const getProcessingHistoryPage = (kb: string | undefined, page: HistoryPage) => get<HistoryPage>(`${base(kb)}/snapshots/${encodeURIComponent(page.snapshot_id)}`, { params: { after: page.next_after, filter_digest: page.filter_digest, limit: 50 } })
export const getProcessingJob = (kb: string | undefined, job: string) => get<JobDetail>(`${base(kb)}/${encodeURIComponent(job)}`)
export type ProcessingAction = 'retry' | 'rebuild' | 'cancel' | 'pin' | 'rollback' | 'retire' | 'resolve-export'
export const controlProcessingJob = (kb: string | undefined, job: string, action: ProcessingAction, body: Record<string, unknown>) => post<{ job_id?: string; accepted?: boolean }>(`${base(kb)}/${encodeURIComponent(job)}/${action}`, body)
