import { MykoReport } from '../decorators'
import { MReport, type ID, type MEvent } from '../types'

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
  constructor() {
    super()
  }
}

@MykoReport()
export class EventsForTransaction extends MReport<MEvent[]> {
  constructor(readonly transactionId: string) {
    super()
  }
}

@MykoReport()
export class EventsForEntity extends MReport<MEvent[]> {
  constructor(
    readonly entityId: string,
    readonly start?: string,
    readonly end?: string,
  ) {
    super()
  }
}
