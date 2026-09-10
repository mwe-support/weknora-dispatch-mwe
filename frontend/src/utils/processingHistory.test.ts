import assert from 'node:assert/strict'
import test from 'node:test'
import { appendHistoryPage } from './processingHistory.ts'
import type { HistoryPage, HistoryRow } from '../api/processing'

test('frozen pages keep their total and reject mixed identity, gaps and duplicates', () => {
  const first: HistoryPage = { snapshot_id: 'a', filter_digest: 'f', view: 'unresolved_incidents', total: 2, created_at: '', expires_at: '', rows: [{ row_id: '1' } as HistoryRow], next_after: 1, has_more: true }
  const second: HistoryPage = { ...first, rows: [{ row_id: '2' } as HistoryRow], next_after: 2, has_more: false }
  assert.deepEqual(appendHistoryPage(first, second).rows.map(row => row.row_id), ['1', '2'])
  for (const change of [{ snapshot_id: 'b' }, { filter_digest: 'new-filter' }, { total: 3 }, { next_after: 3 }, { rows: first.rows }]) assert.throws(() => appendHistoryPage(first, { ...second, ...change }), /SNAPSHOT_CHANGED/)
})
