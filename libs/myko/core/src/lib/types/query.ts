import { MItem } from './item'

export const MYKO_HANDLER_QUERY_ID_KEY = '__MYKI_HANDLER_QUERY_ID_KEY__'
export const MYKO_QUERY_ID_KEY = '__MYKO_QUERY_ID_KEY__'

export type MQueryable = MItem | MItem[]

export abstract class MQuery<T extends MQueryable = MQueryable> {
  $result: T
}

export type MQueryResult<Q> = Q extends MQuery<infer R> ? Promise<R> : never

export interface MQueryHandler<H> {
  execute(query: H): MQueryResult<H>
}
