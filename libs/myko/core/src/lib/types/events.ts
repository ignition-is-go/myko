import { IMykoItem, MYKO_ITEM_TYPE } from './item'

export enum MykoEventType {
  SET = 'SET',
  DEL = 'DEL',
}

export const MYKO_EVENT_HANDLER = '__MYKO_EVENT_HANDLER__'
export const MYKO_EVENT = '__MYKO_EVENT__'

export type MykoEvent<T extends IMykoItem, C extends MykoEventType> = {
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
): MykoEvent<T, U> => {
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

export interface IMykoEventHandler<
  T extends IMykoItem,
  C extends MykoEventType,
> {
  handle(event: MykoEvent<T, C>): Promise<void>
}
