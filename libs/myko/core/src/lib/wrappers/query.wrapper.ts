import 'reflect-metadata'
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
 * @deprecated use MQuery.wrap instead.
 */
export const wrapQuery = (query: MQuery): MWrappedQuery => {
  return query.wrap()
}

/**
 * Unwraps a wrapped query object and restores the original query object.
 * @param wrappedQuery - The wrapped query object to unwrap.
 * @returns The original query object.
 * @deprecated use MQuery.fromWrappedQuery instead.
 */
export const unwrapQuery = (wrappedQuery: MWrappedQuery): MQuery => {
  return MQuery.fromWrappedQuery(wrappedQuery)
}
