import type { Observable } from 'rxjs'
import type { IMItem, MItem } from './item'

import { MYKO_QUERY_ID_KEY, MYKO_QUERY_ITEM_TYPE_KEY } from '../constants'
import type { MWrappedQuery } from '../wrappers'
import { WithContext } from './base'

/**
 * Represents a query object used for retrieving data.
 * @template T - The type of the query result.
 */
export class MQuery<T extends MItem = MItem> extends WithContext {
  /**
   * The result of the query.
   */
  $queryResult!: T[]

  wrap(): MWrappedQuery {
    const queryId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, this)
    const queryItemType = Reflect.getMetadata(MYKO_QUERY_ITEM_TYPE_KEY, this)
    if (!queryId) {
      console.log(Reflect.getMetadataKeys(this))
      throw new Error('Could not get query ID from Metadata')
    }

    return {
      query: this,
      queryId,
      queryItemType,
    }
  }

  static fromWrappedQuery<R extends MItem>(
    wrappedQuery: MWrappedQuery,
  ): MQuery<R> {
    const { query: queryInner, queryId } = wrappedQuery
    const query = new MQuery<R>()

    Object.assign(query, queryInner)

    Reflect.defineMetadata(MYKO_QUERY_ID_KEY, queryId, query)
    Reflect.defineMetadata(MYKO_QUERY_ITEM_TYPE_KEY, queryId, query)

    return query
  }

  getTag(): string {
    return Reflect.getMetadata(MYKO_QUERY_ID_KEY, this)
  }
}

/**
 * Represents the result type of a query.
 * @template Q - The type of the query.
 */
export type MQueryResult<Q> = Q extends MQuery<infer R> ? Promise<R[]> : never

/**
 * Represents the live query result type of a query.
 * @template Q - The type of the query.
 */
export type MLiveQueryResult<Q> =
  Q extends MQuery<infer R> ? Observable<R[]> : never

/**
 * Represents a query handler that can execute a query and return a live query result.
 * @template H - The type of the query.
 */
export interface MQueryHandler<H> {
  /**
   * Executes the specified query and returns a live query result.
   * @param query - The query to execute.
   * @returns The live query result as an Observable.
   */
  execute(query: H): MLiveQueryResult<H>
}

export type MQueryHandlerConstructor<T extends MQuery<MItem<IMItem>>> = new (
  ...args: any[]
) => MQueryHandler<T>

export type MQueryConstructor<T extends MQuery<MItem<IMItem>>> = new (
  ...args: any[]
) => T
