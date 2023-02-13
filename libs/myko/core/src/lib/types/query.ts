import { IMykoItem } from './item'

export const MYKO_QUERY_HANDLER = '__MYKO_QUERY_HANDLER__'
export const MYKO_QUERY = '__MYKO_QUERY__'

export type QueryFor<T extends IMykoItem | IMykoItem[]> = {
  $result: T
}
