import {
  MItem,
  MQuery,
  MQueryHandler,
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_QUERY_ID_KEY,
  MYKO_QUERY_ITEM_TYPE_KEY,
  MYKO_ITEM_TYPE,
} from '../types'
import {} from '../types'

export const MykoQuery =
  <U extends MItem>(commandId: string, item: new (...args: any[]) => U) =>
  <T extends MQuery>(target: new (...args: any[]) => T) => {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, item)
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_QUERY_ID_KEY, commandId, typed)
      Reflect.defineMetadata(MYKO_QUERY_ITEM_TYPE_KEY, itemType, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_QUERY_ID_KEY, commandId, withType)
    Reflect.defineMetadata(MYKO_QUERY_ITEM_TYPE_KEY, itemType, withType)
    return withType
  }

export const MykoQueryHandler = <T extends MQuery>(
  command: new (...args: any[]) => T,
) => {
  return (target: new (...args: any[]) => MQueryHandler<T>) => {
    const commandId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, command)
    Reflect.defineMetadata(MYKO_HANDLER_QUERY_ID_KEY, commandId, target)
  }
}
