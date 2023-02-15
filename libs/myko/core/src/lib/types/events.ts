import { filter } from 'rxjs'
import { IMykoItem, MYKO_ITEM_TYPE } from './item'

export enum MykoEventType {
  SET = 'SET',
  DEL = 'DEL',
}

export const MYKO_EVENT_HANDLER = '__MYKO_EVENT_HANDLER__'
export const MYKO_EVENT = '__MYKO_EVENT__'

export type IMykoEvent<T extends IMykoItem, C extends MykoEventType> = {
  item: T
  changeType: C
  itemType: string
}

export const makeSet = <T extends IMykoItem>(item: T) =>
  makeMykoEvent(item, MykoEventType.SET)

export const makeDel = <T extends IMykoItem>(item: T) =>
  makeMykoEvent(item, MykoEventType.DEL)

const makeMykoEvent = <T extends IMykoItem, U extends MykoEventType>(
  item: T,
  changeType: U,
): IMykoEvent<T, U> => {
  const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemType) {
    throw new Error('Item not decorated with type')
  }
  return {
    changeType,
    item,
    itemType,
  }
}

export const ofItems = <T extends IMykoItem>(
  ...filterTypes: (new (...args: any[]) => T)[]
) =>
  filter((event: IMykoEvent<T, MykoEventType>) =>
    filterTypes.some(
      (filterType) =>
        Reflect.getMetadata(MYKO_ITEM_TYPE, filterType) === event.itemType,
    ),
  )

export const ofType = <T extends IMykoItem>(...filterTypes: MykoEventType[]) =>
  filter((event: IMykoEvent<T, MykoEventType>) =>
    filterTypes.some((filterType) => filterType === event.changeType),
  )
