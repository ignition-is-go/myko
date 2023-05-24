import { DateTime } from 'luxon'
import { filter } from 'rxjs'
import { MItem, MYKO_ITEM_TYPE } from './item'
import { ID } from './base'

export enum MEventType {
  SET = 'SET',
  DEL = 'DEL',
}

export const MYKO_EVENT_HANDLER = '__MYKO_EVENT_HANDLER__'
export const MYKO_EVENT = '__MYKO_EVENT__'

export type MEvent<
  T extends MItem = MItem,
  C extends MEventType = MEventType,
> = {
  readonly item: T
  readonly changeType: C
  readonly itemType: string
  readonly createdAt: string
  readonly tx: string
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
