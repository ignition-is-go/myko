import { MYKO_ITEM_TYPE } from '../decorators'
import { IMykoItem } from './item'

export enum ChangeType {
  SET = 'SET',
  DEL = 'DEL',
}

export type Change<T extends IMykoItem, C extends ChangeType> = {
  item: T
  changeType: C
  itemType: string
}

export const makeSet = <T extends IMykoItem>(item: T) =>
  makeChange(item, ChangeType.SET)

export const makeDel = <T extends IMykoItem>(item: T) =>
  makeChange(item, ChangeType.DEL)

const makeChange = <T extends IMykoItem, U extends ChangeType>(
  item: T,
  changeType: U,
): Change<T, U> => {
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
