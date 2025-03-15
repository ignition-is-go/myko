import {
  AllTransactions,
  EventsForEntity,
  EventsForTransaction,
  EventsInRange,
  getHistoryProvider,
  MykoReportHandler,
  TransactionsInRange,
  type MEvent,
  type MReportHandler,
} from '@myko/core'
import { from, type Observable } from 'rxjs'

@MykoReportHandler(TransactionsInRange)
export class TransactionsInRangeHandler
  implements MReportHandler<TransactionsInRange>
{
  execute(report: TransactionsInRange): Observable<string[]> {
    const history = getHistoryProvider()

    return history.getTransactionsInTimeRange(
      report.excludeEntities,
      report.start,
      report.end,
    )
  }
}

@MykoReportHandler(EventsInRange)
export class EventsInRangeHandler implements MReportHandler<EventsInRange> {
  execute(report: EventsInRange): Observable<MEvent[]> {
    const history = getHistoryProvider()

    return from(history.getEventsInTimeRange(report.start, report.end))
  }
}

@MykoReportHandler(AllTransactions)
export class AllTransactionsHandler implements MReportHandler<AllTransactions> {
  execute(report: AllTransactions): Observable<string[]> {
    const history = getHistoryProvider()

    return from(history.getAllTransactions())
  }
}

@MykoReportHandler(EventsForTransaction)
export class EventsForTransactionHandler
  implements MReportHandler<EventsForTransaction>
{
  execute(report: EventsForTransaction): Observable<MEvent[]> {
    const history = getHistoryProvider()

    return from(history.getEventsForTransaction(report.transactionId))
  }
}

@MykoReportHandler(EventsForEntity)
export class EventsForEntityHandler implements MReportHandler<EventsForEntity> {
  execute(report: EventsForEntity): Observable<MEvent[]> {
    const history = getHistoryProvider()

    return history.getEntityHistory(report.entityId, report.start, report.end)
  }
}
