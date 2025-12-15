import { queryBus } from '../busses'
import {
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_ITEM_TYPE,
  MYKO_QUERY_ID_KEY,
  MYKO_QUERY_ITEM_TYPE_KEY,
} from '../constants'
import { addQueryDoc, queries, queryHandlers } from '../registry'
import type {
  IMItem,
  MItem,
  MQuery,
  MQueryConstructor,
  MQueryHandlerConstructor,
} from '../types'

/**
 * Decorator for defining a Myko query.
 * @param {string} queryId - The unique identifier for the query.
 * @returns {Function} - The decorator function.
 */
export const MykoQuery: <U extends MItem<IMItem>>(
  item: new (...args: any[]) => U,
) => <T extends MQuery<MItem<IMItem>>>(target: MQueryConstructor<T>) => any =
  <U extends MItem>(item: new (...args: any[]) => U) =>
  <T extends MQuery>(target: MQueryConstructor<T>) => {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, item)
    const original: any = target

    const queryName = Object.getOwnPropertyDescriptors(original)?.['name'].value

    const paramtypes =
      Reflect.getMetadata('design:paramtypes', original)?.map((x: { name: string }) => x.name) ??
      []

    const queryId = queryName

    queries.add(queryId)

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

export const MykoQueryHandler: <T extends MQuery<MItem<IMItem>>>(
  query: MQueryConstructor<T>,
) => (handlerClass: MQueryHandlerConstructor<T>) => void = <T extends MQuery>(
  query: MQueryConstructor<T>,
) => {
  const queryId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, query)

  if (!queryId) {
    throw new Error('queryId is undefined')
  }

  queryHandlers.add(queryId)

  return (target: MQueryHandlerConstructor<T>) => {
    Reflect.defineMetadata(MYKO_HANDLER_QUERY_ID_KEY, queryId, target)
    queryBus.registerHandler(target)
  }
}
