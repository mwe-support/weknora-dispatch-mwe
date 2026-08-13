import assert from 'node:assert/strict'
import test from 'node:test'

import type { DataSource } from '@/api/datasource'
import {
  isDataSourceSyncRunning,
  reconcilePendingSyncs,
  shouldPollDataSources,
  type PendingSync,
} from './datasourceSyncPolling'

function dataSource(
  id: string,
  latestSyncLog?: DataSource['latest_sync_log'],
): DataSource {
  return {
    id,
    tenant_id: 10000,
    knowledge_base_id: 'kb-1',
    name: id,
    type: 'tencent_docs',
    config: {},
    sync_schedule: '',
    sync_mode: 'incremental',
    status: 'active',
    conflict_strategy: 'overwrite',
    sync_deletions: false,
    last_sync_at: null,
    last_sync_result: null,
    error_message: '',
    created_at: '',
    updated_at: '',
    latest_sync_log: latestSyncLog,
  }
}

function syncLog(id: string, status: 'running' | 'success') {
  return {
    id,
    data_source_id: 'ds-1',
    status,
    started_at: '2026-08-13T00:00:00Z',
    finished_at: status === 'running' ? null : '2026-08-13T00:00:01Z',
    items_total: 0,
    items_created: 0,
    items_updated: 0,
    items_deleted: 0,
    items_skipped: 0,
    items_failed: 0,
    error_message: '',
  } as const
}

test('keeps polling while a triggered sync log is not visible yet', () => {
  const pending = new Map<string, PendingSync>([
    ['ds-1', { previousLogId: 'log-old', deadlineAt: 20_000 }],
  ])
  const sources = [dataSource('ds-1', syncLog('log-old', 'success'))]

  const next = reconcilePendingSyncs(sources, pending, 10_000)

  assert.equal(next.has('ds-1'), true)
  assert.equal(isDataSourceSyncRunning(sources[0], next), true)
  assert.equal(shouldPollDataSources(sources, next), true)
})

test('hands polling over to a newly visible running log', () => {
  const pending = new Map<string, PendingSync>([
    ['ds-1', { previousLogId: 'log-old', deadlineAt: 20_000 }],
  ])
  const sources = [dataSource('ds-1', syncLog('log-new', 'running'))]

  const next = reconcilePendingSyncs(sources, pending, 10_000)

  assert.equal(next.has('ds-1'), false)
  assert.equal(isDataSourceSyncRunning(sources[0], next), true)
  assert.equal(shouldPollDataSources(sources, next), true)
})

test('stops optimistic polling when the new log is terminal or the grace period expires', () => {
  const terminalPending = new Map<string, PendingSync>([
    ['ds-1', { previousLogId: 'log-old', deadlineAt: 20_000 }],
  ])
  const terminalSources = [dataSource('ds-1', syncLog('log-new', 'success'))]
  const terminalNext = reconcilePendingSyncs(terminalSources, terminalPending, 10_000)
  assert.equal(terminalNext.size, 0)
  assert.equal(shouldPollDataSources(terminalSources, terminalNext), false)

  const expiredPending = new Map<string, PendingSync>([
    ['ds-1', { previousLogId: undefined, deadlineAt: 9_999 }],
  ])
  const expiredNext = reconcilePendingSyncs([dataSource('ds-1')], expiredPending, 10_000)
  assert.equal(expiredNext.size, 0)
})
