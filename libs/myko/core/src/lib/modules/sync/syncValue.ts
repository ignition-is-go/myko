import {
  doc,
  MykoCommand,
  MykoItem,
  MykoQuery,
  MykoReport,
} from '../../decorators'
import { MCommand, MItem, MQuery, MReport, type ID } from '../../types'

import { switchMap } from 'rxjs'
import * as Z from 'zod'
import { commandBus, reportBus } from '../../busses'
import { type Forecaster } from './forecasters'

@MykoItem({
  doc: 'A tracked value that syncs fast between server, and implements forecasting to account for lag',
})
export class SyncValue
  extends MItem<SyncValue>
  implements Z.infer<typeof syncValueSchema>
{
  @doc('the leader for this value')
  serverId: ID

  @doc('The frequency (in ms) to sync the value and forecast')
  syncFrequency: number
}

const syncValueSchema = Z.object({
  serverId: Z.string(),
  syncFrequency: Z.number(),
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

export type SyncUpdate = {
  valueId: ID
  lastUpdated: string // ISO datetime
  value: number
  forecaster: Forecaster
}

@MykoReport()
export class SyncValueUpdates extends MReport<SyncUpdate> {
  constructor(readonly id: ID) {
    super()
  }
}

@MykoCommand()
export class UpdateSyncValue extends MCommand {
  constructor(
    readonly id: ID,
    readonly forecaster: Partial<Forecaster>,
  ) {
    super()
  }
}

@MykoReport()
export class LiveSyncValue extends MReport<number> {
  constructor(
    readonly id: ID,
    readonly sampleRate: number,
  ) {
    super()
  }
}

export const syncValue = <T>(
  build: (value: T) => {
    key: string
    forecast: Forecaster
    sampleRate: number
  },
) =>
  switchMap((v: T) => {
    const data = build(v)

    commandBus.execute(new UpdateSyncValue(data.key, data.forecast))

    return reportBus.watch(new LiveSyncValue(data.key, data.sampleRate)).pipe()
  })
