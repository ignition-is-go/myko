import {
  getHostId,
  // GetSyncValues,
  GetSyncValuesByIds,
  GetSyncValuesByQuery,
  LiveSyncValue,
  MykoQueryHandler,
  MykoReportHandler,
  PeerReport,
  repo,
  reportBus,
  resolveForecast,
  SyncValue,
  SyncValueUpdates,
  updateFactories,
  type MLiveReportResult,
  type MQueryHandler,
  type MReportHandler,
  type SyncUpdate,
} from '@myko/core'
import { DateTime } from 'luxon'
import {
  catchError,
  EMPTY,
  filter,
  interval,
  map,
  Observable,
  of,
  startWith,
  switchMap,
} from 'rxjs'

@MykoQueryHandler(GetSyncValuesByQuery)
export class GetSyncValuesByQueryHandler
  implements MQueryHandler<GetSyncValuesByQuery>
{
  execute(query: GetSyncValuesByQuery): Observable<SyncValue[]> {
    return repo(SyncValue).watch(query.partial)
  }
}

@MykoQueryHandler(GetSyncValuesByIds)
export class GetSyncValuesByIdsHandler
  implements MQueryHandler<GetSyncValuesByIds>
{
  execute(query: GetSyncValuesByIds): Observable<SyncValue[]> {
    return repo(SyncValue).watchIds(query.ids)
  }
}

@MykoReportHandler(SyncValueUpdates)
export class SyncValueUpdatesHandler
  implements MReportHandler<SyncValueUpdates>
{
  execute(query: SyncValueUpdates): MLiveReportResult<SyncValueUpdates> {
    const syncValue = repo(SyncValue).watchId(query.id)

    return syncValue.pipe(
      switchMap((sync) => {
        if (!sync) {
          return EMPTY
        }

        if (sync.serverId !== getHostId()) {
          return reportBus.watch(new PeerReport(query, sync.serverId))
        }

        const ctx = sync.context

        const updates = updateFactories.get(ctx.code)

        if (!updates) {
          throw new Error('No updates found')
        }

        return updates(ctx).pipe(
          switchMap((update) => {
            const lastUpdated = DateTime.fromISO(update.lastUpdated).toMillis()

            return interval(sync.syncFrequency).pipe(
              startWith(undefined),
              map(() => {
                const now = DateTime.utc().toMillis()

                const elapsed = now - lastUpdated

                const resolved = resolveForecast({
                  deltaMs: elapsed,
                  forecaster: update.forecaster,
                  value: update.value,
                })

                return {
                  lastUpdated: DateTime.utc().toISO(),
                  forecaster: resolved.forecaster,
                  value: resolved.value,
                  valueId: update.valueId,
                } satisfies SyncUpdate
              }),
              catchError(() => {
                return of(undefined)
              }),
              filter((x) => !!x),
            )
          }),
        )
      }),
    )
  }
}

@MykoReportHandler(LiveSyncValue)
export class LiveSyncValueHandler implements MReportHandler<LiveSyncValue> {
  execute(query: LiveSyncValue): MLiveReportResult<LiveSyncValue> {
    const syncValue = reportBus.watch(new SyncValueUpdates(query.id))

    const live = syncValue.pipe(
      switchMap((update) => {
        const lastUpdated = DateTime.fromISO(update.lastUpdated).toMillis()

        if (update.forecaster.type === 'null') {
          return of(update.value)
        }

        return interval(1000 / update.forecaster.sampleRate).pipe(
          startWith(undefined),
          map((_) => {
            const now = DateTime.utc().toMillis()
            const elapsed = now - lastUpdated

            const resolved = resolveForecast({
              deltaMs: elapsed,
              forecaster: update.forecaster,
              value: update.value,
            })
            return resolved.value
          }),
        )
      }),
    )

    return live
  }
}
