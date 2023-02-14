import { IMykoItem } from './item'

export const MYKO_HANDLER_QUERY_ID_KEY = '__MYKI_HANDLER_QUERY_ID_KEY__'
export const MYKO_QUERY_ID_KEY = '__MYKO_QUERY_ID_KEY__'

export abstract class IMykoQuery<T extends IMykoItem | IMykoItem[]> {
  $result: T
}

export type MykoQueryResult<Q> = Q extends IMykoQuery<infer R>
  ? Promise<R>
  : never

export interface IMykoQueryHandler<H> {
  execute(query: H): MykoQueryResult<H>
}
