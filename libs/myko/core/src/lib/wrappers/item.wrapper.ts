import { MEvent, MItem, MYKO_ITEM_TYPE } from '../types'

export interface MWrappedItem {
  item: MItem
  itemType: string
}

export const wrapItem = (item: MItem): MWrappedItem => {
  const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, item)
  if (!itemType) {
    throw new Error('Could not get item type from metadata')
  }
  return {
    item,
    itemType,
  }
}

export const unwrapItem = (wrappedItem: MWrappedItem | MEvent): MItem => {
  const { item, itemType } = wrappedItem
  Reflect.defineMetadata(MYKO_ITEM_TYPE, itemType, item)
  return item
}
