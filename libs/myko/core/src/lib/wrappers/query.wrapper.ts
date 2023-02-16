import { MQuery, MYKO_QUERY_ID_KEY } from '../types'

export interface MWrappedQuery {
  query: MQuery
  queryId: string
}

export const wrapQuery = (query: MQuery): MWrappedQuery => {
  const queryId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, query)
  if (!queryId) {
    throw new Error('Could not get query ID from Metadata')
  }

  return {
    query,
    queryId,
  }
}

export const unwrapQuery = (wrappedQuery: MWrappedQuery): MQuery => {
  const { query, queryId } = wrappedQuery
  Reflect.defineMetadata(MYKO_QUERY_ID_KEY, queryId, query)
  return query
}
