import { IMykoItem, MYKO_QUERY, MYKO_QUERY_HANDLER, QueryFor } from '@myko/core'
import { uuid as v4 } from 'uuid'

export const QueryHandler = <T extends IMykoItem | IMykoItem[]>(
  query: QueryFor<T> | (new (...args: any[]) => QueryFor<T>),
): ClassDecorator => {
  return (target: object) => {
    if (!Reflect.hasOwnMetadata(MYKO_QUERY, query)) {
      Reflect.defineMetadata(MYKO_QUERY, { id: v4() }, query)
    }
    Reflect.defineMetadata(MYKO_QUERY_HANDLER, query, target)
  }
}
