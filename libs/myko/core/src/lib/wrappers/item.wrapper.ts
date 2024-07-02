import { MYKO_ITEM_TYPE } from '../constants'
import type { MEvent, MItem } from '../types'

/**
 * Represents a wrapped item.
 */
export interface MWrappedItem {
  item: MItem // The wrapped item.
  itemType: string // The type of the wrapped item.
}

/**
 * Wraps an item with its type.
 * @param item - The item to be wrapped.
 * @returns The wrapped item.
 * @throws Error if the item type cannot be retrieved from metadata.
 */
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

/**
 * Unwraps a wrapped item and updates its type in metadata.
 * @param wrappedItem - The wrapped item to be unwrapped.
 * @returns The unwrapped item.
 */
export const unwrapItem = (wrappedItem: MWrappedItem | MEvent): MItem => {
  const { item, itemType } = wrappedItem
  Reflect.defineMetadata(MYKO_ITEM_TYPE, itemType, item)
  return item
}
