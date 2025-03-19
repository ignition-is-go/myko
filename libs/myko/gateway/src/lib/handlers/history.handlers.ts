import {
  AllTransactions,
  EventContainer,
  EventsForEntity,
  EventsForTransaction,
  EventsInRange,
  getHistoryProvider,
  MykoQueryHandler,
  MykoReportHandler,
  TransactionsInRange,
  type MEvent,
  type MLiveQueryResult,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import { from, map, type Observable } from 'rxjs'

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
  execute(_report: AllTransactions): Observable<string[]> {
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

@MykoQueryHandler(EventsForEntity)
export class EventsForEntityHandler implements MQueryHandler<EventsForEntity> {
  execute(report: EventsForEntity): MLiveQueryResult<EventsForEntity> {
    const history = getHistoryProvider()

    return history
      .getEntityHistory(report.entityId, report.start, report.end)
      .pipe(
        map((events) => {
          return events.map(
            (x) =>
              new EventContainer({
                event: x,
                id: `${x.tx}-${x.itemType}-${x.item.id}`,
              }),
          )
        }),
      )
  }
}
