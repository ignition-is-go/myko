import { DateTime } from 'luxon'
import { filter } from 'rxjs'
import { MItem } from './item'
import { ID } from './base'
import { MykoItem, MykoQuery, doc } from '../decorators'
import { MQuery } from './query'
import { MYKO_ITEM_TYPE } from '../constants'
import type { MEvent as WASMEvent } from 'myko-wasm'

export enum MEventType {
  SET = 'SET',
  DEL = 'DEL',
}

type IMEvent = Omit<WASMEvent, 'free' | 'item' | 'changeType'>

export type MEvent<
  T extends MItem = MItem,
  C extends MEventType = MEventType,
> = IMEvent & {
  readonly item: T
  readonly changeType: C
}

export const makeSet = <T extends MItem>(item: T, tx: ID) =>
  makeMykoEvent(item, MEventType.SET, tx)

export const makeDel = <T extends MItem>(item: T, tx: ID) =>
  makeMykoEvent(item, MEventType.DEL, tx)

const makeMykoEvent = <T extends MItem, U extends MEventType>(
  item: T,
  changeType: U,
  tx: ID,
  overrideType?: string,
  sourceId?: ID,
): MEvent<T, U> => {
  const metadataType = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  const itemType = overrideType ?? metadataType

  if (!itemType) {
    throw new Error('Item not decorated with type')
  }
  return {
    changeType,
    item,
    itemType,
    createdAt: DateTime.utc().toString(),
    tx,
    sourceId,
  }
}

export const ofItems = <T extends MItem>(
  ...filterTypes: (new (...args: any[]) => T)[]
) =>
  filter((event: MEvent<T, MEventType>) =>
    filterTypes.some(
      (filterType) =>
        Reflect.getMetadata(MYKO_ITEM_TYPE, filterType) === event.itemType,
    ),
  )

export const ofType = <T extends MItem, C extends MEventType>(filterType: C) =>
  filter((event: MEvent<T, C>) => filterType === event.changeType)

@MykoItem({
  doc: 'A container for events to allow them to be queried',
  deprecated: true,
  preventDoc: true,
})
export class EventContainer extends MItem<EventContainer> {
  @doc(undefined, 'MEvent')
  readonly event: MEvent
}
@MykoQuery('event-log:get-events', EventContainer)
export class GetEventLog extends MQuery<EventContainer> {
  constructor(readonly time: string) {
    super()
  }
}
