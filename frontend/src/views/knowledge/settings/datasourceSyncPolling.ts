import type { DataSource } from '@/api/datasource'

export const SYNC_LOG_VISIBILITY_GRACE_MS = 30_000

export interface PendingSync {
  previousLogId?: string
  deadlineAt: number
}

export function reconcilePendingSyncs(
  dataSources: DataSource[],
  pendingSyncs: ReadonlyMap<string, PendingSync>,
  now: number,
): Map<string, PendingSync> {
  const next = new Map(pendingSyncs)

  for (const [dataSourceId, pending] of next) {
    const dataSource = dataSources.find(item => item.id === dataSourceId)
    const visibleLogId = dataSource?.latest_sync_log?.id

    // A different log proves that the backend accepted this trigger. From this
    // point the normal running/terminal status is authoritative.
    if (visibleLogId && visibleLogId !== pending.previousLogId) {
      next.delete(dataSourceId)
      continue
    }

    // Do not leave a permanently disabled "syncing" button if the enqueue
    // response succeeded but no new log ever becomes visible.
    if (now >= pending.deadlineAt) {
      next.delete(dataSourceId)
    }
  }

  return next
}

export function isDataSourceSyncRunning(
  dataSource: DataSource,
  pendingSyncs: ReadonlyMap<string, PendingSync>,
): boolean {
  return dataSource.latest_sync_log?.status === 'running' || pendingSyncs.has(dataSource.id)
}

export function shouldPollDataSources(
  dataSources: DataSource[],
  pendingSyncs: ReadonlyMap<string, PendingSync>,
): boolean {
  return pendingSyncs.size > 0 || dataSources.some(ds => ds.latest_sync_log?.status === 'running')
}
