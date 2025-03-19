import type { Observable } from 'rxjs'
import { type DeepPartial, type MEvent, type MItem } from '../types'

export abstract class HistoryProvider {
  constructor() {}

  abstract getEntityHistory(
    id: string,
    start?: string,
    end?: string,
  ): Observable<MEvent[]>

  abstract getItemAsOfTime<T extends MItem>(
    id: string,
    itemType: string,
    time: string,
  ): Promise<T | null>

  abstract getItemsByQueryAsOfTime<T extends MItem>(
    query: DeepPartial<T>,
    itemType: string,
    time: string,
  ): Promise<T[]>

  abstract getAllItemsAsOfTime<T extends MItem>(
    itemType: string,
    time: string,
  ): Promise<T[]>

  abstract getEventsForTransaction(id: string): Promise<MEvent[]>

  abstract getAllTransactions(): Promise<string[]>

  abstract getTransactionsInTimeRange(
    excludeEntities: string[],
    start: string,
    end?: string,
  ): Observable<string[]>

  abstract getEventsInTimeRange(start: string, end?: string): Promise<MEvent[]>

  abstract init(): Promise<void>

  abstract recordUserTransaction(
    userId: string,
    transactionId: string,
  ): Promise<void>
}
