import type { HistoryPage } from '../api/processing'

// Reject a mixed/expired response instead of silently presenting a false total.
export function appendHistoryPage(previous: HistoryPage, next: HistoryPage): HistoryPage {
  if (previous.snapshot_id !== next.snapshot_id || previous.filter_digest !== next.filter_digest || previous.total !== next.total || next.next_after !== previous.next_after + next.rows.length) throw new Error('SNAPSHOT_CHANGED')
  const ids = new Set(previous.rows.map(row => row.row_id))
  for (const row of next.rows) { if (ids.has(row.row_id)) throw new Error('SNAPSHOT_CHANGED'); ids.add(row.row_id) }
  return { ...next, rows: [...previous.rows, ...next.rows] }
}
