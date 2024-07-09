import type { Observable } from 'rxjs'
import type { IMItem, MItem } from './item'

import { v4 as uuid } from 'uuid'

/**
 * Represents a query object used for retrieving data.
 * @template T - The type of the query result.
 */
export class MQuery<T extends MItem = MItem> {
  /**
   * The result of the query.
   */
  $queryResult: T[]

  /**
   * The transaction ID associated with the query.
   */
  readonly tx: string

  /**
   * Creates a new instance of the MQuery class.
   */
  constructor() {
    this.tx = uuid()
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
