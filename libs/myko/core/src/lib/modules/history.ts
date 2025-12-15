import { MykoQuery, MykoReport } from '../decorators'
import { EventContainer, MQuery, MReport, type ID, type MEvent } from '../types'

@MykoReport()
export class TransactionsInRange extends MReport<ID[]> {
  constructor(
    readonly excludeEntities: string[] = [],
    readonly start: string,
    readonly end?: string,
  ) {
    super()
  }
}

@MykoReport()
export class EventsInRange extends MReport<MEvent[]> {
  constructor(
    readonly start: string,
    readonly end: string,
  ) {
    super()
  }
}

@MykoReport()
export class AllTransactions extends MReport<ID[]> {
}

@MykoReport()
export class EventsForTransaction extends MReport<MEvent[]> {
  constructor(readonly transactionId: string) {
    super()
  }
}

@MykoQuery(EventContainer)
export class EventsForEntity extends MQuery<EventContainer> {
  constructor(
    readonly entityId: string,
    readonly start?: string,
    readonly end?: string,
  ) {
    super()
  }
}
