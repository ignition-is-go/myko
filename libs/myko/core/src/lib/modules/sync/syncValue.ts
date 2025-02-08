import {
  ballance,
  doc,
  MykoItem,
  MykoQuery,
  MykoReport,
} from '../../decorators'
import { MItem, MQuery, MReport, type ID } from '../../types'

import { DateTime } from 'luxon'
import { MD5 } from 'object-hash'
import { filter, Observable, ReplaySubject, scan, share } from 'rxjs'
import * as Z from 'zod'
import { eventBus, reportBus } from '../../busses'
import { getHostId, repo } from '../../registry'
import { Server } from '../server'
import { resolveForecast, type Forecaster } from './forecasters'

@MykoItem({
  doc: 'A tracked value that syncs fast between server, and implements forecasting to account for lag',
})
export class SyncValue
  extends MItem<SyncValue>
  implements Z.infer<typeof syncValueSchema>
{
  @doc('the leader for this value')
  @ballance(Server)
  serverId: ID

  @doc('info for creating sync value updates')
  context: SyncValueContextBase<string>
}

const syncValueSchema = Z.object({
  serverId: Z.string(),
})

@MykoQuery(SyncValue)
export class GetSyncValuesByQuery extends MQuery<SyncValue> {
  constructor(readonly partial: Partial<SyncValue>) {
    super()
  }
}

@MykoQuery(SyncValue)
export class GetSyncValuesByIds extends MQuery<SyncValue> {
  constructor(readonly ids: ID[]) {
    super()
  }
}

@MykoReport()
export class SyncValueUpdates extends MReport<SyncUpdate> {
  constructor(readonly id: ID) {
    super()
  }
}

// @MykoCommand()
// export class UpdateSyncValue extends MCommand {
//   constructor(
//     readonly id: ID,
//     readonly forecaster: Partial<Forecaster>,
//   ) {
//     super()
//   }
// }

@MykoReport()
export class LiveSyncValue extends MReport<number> {
  constructor(readonly id: ID) {
    super()
  }
}

export type SyncUpdate = {
  valueId: ID
  lastUpdated: string // ISO datetime
  value: number
  forecaster: Forecaster
}

export const syncValueKey = <T extends SyncValueContextBase<string>>(
  ctx: T extends SyncValueContextBase<infer Code>
    ? SyncValueContextBase<Code>
    : never,
) => {
  return MD5(ctx)
}

export const updateFactories = new Map<
  string,
  (
    ctx: any,
    init?: Pick<SyncUpdate, 'lastUpdated' | 'value'>,
  ) => Observable<SyncUpdate>
>()

export const useSyncValue = <T extends SyncValueContextBase<string>>(
  code: T['code'],
  makeUpdates: (ctx: T) => Observable<{
    forecast: Forecaster
    value?: number
  }>,
) => {
  const updateObsFactory = (
    ctx: T,
    init?: Pick<SyncUpdate, 'lastUpdated' | 'value'>,
  ) => {
    const key = syncValueKey(ctx)

    const obs = makeUpdates(ctx as T).pipe(
      scan(
        (prev, update) => {
          const NOW = DateTime.utc()

          if (!prev) {
            return {
              valueId: key,
              forecaster: update.forecast,
              lastUpdated: init?.lastUpdated ?? NOW.toISO(),
              value: update.value ?? init?.value ?? 0,
            }
          }

          const PREV = DateTime.fromISO(prev.lastUpdated)

          const deltaMs = NOW.toMillis() - PREV.toMillis()

          const forecasted = resolveForecast({
            deltaMs,
            forecaster: prev.forecaster,
            value: prev.value,
          })

          return {
            valueId: key,
            forecaster: update.forecast,
            lastUpdated: NOW.toISO(),
            value: forecasted.value,
          } satisfies SyncUpdate
        },
        undefined as SyncUpdate | undefined,
      ),
      filter((x) => !!x),
      share({
        connector: () => new ReplaySubject(1),
      }),
    )

    return obs
  }

  updateFactories.set(code, updateObsFactory)

  return (ctx: T) => {
    const key = syncValueKey(ctx)
    console.log('setting up sync value', key)

    repo(SyncValue)
      .getId(key)
      .then((sync) => {
        if (!sync) {
          const syncValue = new SyncValue({
            context: ctx,
            id: key,
            serverId: getHostId(),
          })

          eventBus.publishSet(syncValue, crypto.randomUUID())
        }
      })

    return reportBus.watch(new LiveSyncValue(key))
  }
}

export type SyncValueContextBase<Code extends string> = {
  code: Code
}
