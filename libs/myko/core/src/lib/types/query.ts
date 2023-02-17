import { MItem } from './item'

import { v4 as uuid } from 'uuid'
import { Observable } from 'rxjs'

export const MYKO_HANDLER_QUERY_ID_KEY = '__MYKI_HANDLER_QUERY_ID_KEY__'
export const MYKO_QUERY_ID_KEY = '__MYKO_QUERY_ID_KEY__'
export const MYKO_QUERY_ITEM_TYPE_KEY = '__MYKO_QUERY_ITEM_TYPE_KEY__'

export class MQuery<T extends MItem = MItem> {
  $result: T[]
  readonly tx: string
  constructor() {
    this.tx = uuid()
  }
}

export type MQueryResult<Q> = Q extends MQuery<infer R> ? Promise<R[]> : never

export type MLiveQueryResult<Q> = Q extends MQuery<infer R>
  ? Observable<R[]>
  : never

export interface MQueryHandler<H> {
  execute(query: H): MLiveQueryResult<H>
}
