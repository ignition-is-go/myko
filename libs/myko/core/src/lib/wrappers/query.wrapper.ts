import { MYKO_QUERY_ID_KEY, MYKO_QUERY_ITEM_TYPE_KEY } from '../constants'
import { MQuery } from '../types'

export interface MWrappedQuery {
  queryItemType: string
  queryId: string
  query: MQuery
}

export const wrapQuery = (query: MQuery): MWrappedQuery => {
  const queryId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, query)
  const queryItemType = Reflect.getMetadata(MYKO_QUERY_ITEM_TYPE_KEY, query)
  if (!queryId || !queryItemType) {
    throw new Error('Could not get query ID from Metadata')
  }

  return {
    query,
    queryId,
    queryItemType,
  }
}

export const unwrapQuery = (wrappedQuery: MWrappedQuery): MQuery => {
  const { query, queryId } = wrappedQuery
  Reflect.defineMetadata(MYKO_QUERY_ID_KEY, queryId, query)
  Reflect.defineMetadata(MYKO_QUERY_ITEM_TYPE_KEY, queryId, query)
  return query
}
