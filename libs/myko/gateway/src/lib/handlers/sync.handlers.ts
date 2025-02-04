import {
  eventBus,
  forecasters,
  getHostId,
  // GetSyncValues,
  GetSyncValuesByIds,
  GetSyncValuesByQuery,
  LiveSyncValue,
  MykoCommandHandler,
  MykoLogger,
  MykoQueryHandler,
  MykoReportHandler,
  PeerReport,
  repo,
  reportBus,
  resolveForecast,
  Server,
  SyncValue,
  SyncValueUpdates,
  UpdateSyncValue,
  type Forecaster,
  type ID,
  type MCommandHandler,
  type MLiveReportResult,
  type MQueryHandler,
  type MReportHandler,
  type SyncUpdate,
} from '@myko/core'
import { DateTime } from 'luxon'
import {
  EMPTY,
  filter,
  interval,
  map,
  Observable,
  startWith,
  Subject,
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

const syncBus = new Subject<SyncUpdate>()
const syncCache = new Map<ID, SyncUpdate>()

@MykoCommandHandler(UpdateSyncValue)
export class UpdateSyncValueHandler
  implements MCommandHandler<UpdateSyncValue>
{
  async execute(command: UpdateSyncValue) {
    const existing = await repo(SyncValue).getId(command.id)

    if (existing && existing.serverId !== getHostId()) {
      new MykoLogger('SyncValue').warn('Trying to update a foreign value')

      const server = await repo(Server).getId(existing.serverId)

      if (server) {
        new MykoLogger('SyncValue').warn('Contoller Found - ignoring update')
        return
      }

      new MykoLogger('SyncValue').warn(
        'Cannot find live value owner, taking control',
      )
      eventBus.publishSet(
        new SyncValue({
          ...existing,
          serverId: getHostId(),
        }),
        command.tx,
      )
    }

    if (!existing) {
      eventBus.publishSet(
        new SyncValue({
          id: command.id,
          serverId: getHostId(),
          syncFrequency: 1000,
        }),
        command.tx,
      )
    }

    const lastUpdate = syncCache.get(command.id)

    if (!lastUpdate) {
      new MykoLogger('SyncValue').warn(
        'No last update - starting starting from best guess',
      )

      const validate = forecasters.safeParse(command.forecaster)

      if (validate.success === false) {
        new MykoLogger('SyncValue').warn('Invalid forecaster')
        return
      }

      const update = {
        lastUpdated: DateTime.utc().toISO(),
        value: 0,
        forecaster: validate.data as Forecaster,
        valueId: command.id,
      }

      syncBus.next(update)
      syncCache.set(command.id, update)

      return
    }

    const REF_TIME = DateTime.fromISO(lastUpdate.lastUpdated)

    const NOW = DateTime.utc()

    const inflectionPoint = resolveForecast({
      deltaMs: NOW.diff(REF_TIME).as('milliseconds'),
      forecaster: lastUpdate.forecaster,
      value: lastUpdate.value,
    })

    const validate = forecasters.safeParse(command.forecaster)

    if (validate.success === false) {
      new MykoLogger('SyncValue').warn('Invalid forecaster')
      return
    }

    const update = {
      lastUpdated: NOW.toISO(),
      value: inflectionPoint.value,
      forecaster: validate.data as Forecaster,
      valueId: command.id,
    }

    syncBus.next(update)
    syncCache.set(command.id, update)
  }
}

@MykoReportHandler(SyncValueUpdates)
export class SyncValueUpdatesHandler
  implements MReportHandler<SyncValueUpdates>
{
  execute(query: SyncValueUpdates): MLiveReportResult<SyncValueUpdates> {
    const syncValue = repo(SyncValue).watchId(query.id)

    const updates = syncBus.pipe(
      filter((u) => u.valueId === query.id),
      startWith(syncCache.get(query.id)),
      filter(Boolean),
    )

    return syncValue.pipe(
      switchMap((sync) => {
        if (!sync) {
          return EMPTY
        }

        if (sync.serverId !== getHostId()) {
          return reportBus.watch(new PeerReport(query, sync.serverId))
        }

        return updates.pipe(
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

        return interval(1000 / query.sampleRate).pipe(
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
