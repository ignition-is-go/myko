import type { Observable } from 'rxjs'
import { type MEvent } from '../types'

export abstract class HistoryProvider {
  constructor() {}

  abstract getEntityHistory(id: string): Promise<MEvent[]>

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
