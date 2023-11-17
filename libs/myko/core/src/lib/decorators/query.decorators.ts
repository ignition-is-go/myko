import {
  MYKO_ITEM_TYPE,
  MYKO_QUERY_ID_KEY,
  MYKO_QUERY_ITEM_TYPE_KEY,
  MYKO_HANDLER_QUERY_ID_KEY,
} from '../constants'
import { addQueryDoc } from '../registry'
import { MItem, MQuery, MQueryHandler } from '../types'

export const MykoQuery =
  <U extends MItem>(queryId: string, item: new (...args: any[]) => U) =>
  <T extends MQuery>(target: new (...args: any[]) => T) => {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, item)
    const original: any = target

    const queryName = Object.getOwnPropertyDescriptors(original)?.name.value

    const paramtypes =
      Reflect.getMetadata('design:paramtypes', original)?.map((x) => x.name) ??
      []

    addQueryDoc(
      {
        queryId,
        queryName,
        queryReturnType: itemType,
        ctor: original,
      },
      paramtypes,
    )

    if (!queryId) {
      throw new Error('commandId is undefined')
    }

    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_QUERY_ID_KEY, queryId, typed)
      Reflect.defineMetadata(MYKO_QUERY_ITEM_TYPE_KEY, itemType, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_QUERY_ID_KEY, queryId, withType)
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
