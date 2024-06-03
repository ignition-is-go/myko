import { MYKO_QUERY_ID_KEY, MYKO_QUERY_ITEM_TYPE_KEY } from '../constants'
import { MQuery } from '../types'

/**
 * Represents a wrapped query object.
 */
export interface MWrappedQuery {
  /**
   * The type of the query item.
   */
  queryItemType: string
  /**
   * The ID of the query.
   */
  queryId: string
  /**
   * The query object.
   */
  query: MQuery
}

/**
 * Wraps a query object with additional metadata.
 * @param query - The query object to wrap.
 * @returns The wrapped query object.
 * @throws Error if the query ID or query item type cannot be retrieved from metadata.
 */
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

/**
 * Unwraps a wrapped query object and restores the original query object.
 * @param wrappedQuery - The wrapped query object to unwrap.
 * @returns The original query object.
 */
export const unwrapQuery = (wrappedQuery: MWrappedQuery): MQuery => {
  const { query, queryId } = wrappedQuery
  Reflect.defineMetadata(MYKO_QUERY_ID_KEY, queryId, query)
  Reflect.defineMetadata(MYKO_QUERY_ITEM_TYPE_KEY, queryId, query)
  return query
}
