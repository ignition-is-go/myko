import { MykoCommand, MykoQuery, MykoReport } from '../decorators'
import {
  type ID,
  MCommand,
  MItem,
  MQuery,
  MReport,
  type MReportType,
} from '../types'

@MykoQuery(MItem)
export class PeerQuery extends MQuery<MItem> {
  constructor(
    readonly query: MQuery,
    readonly peerId: ID,
  ) {
    super()
  }
}

@MykoCommand()
export class PeerCommand extends MCommand {
  constructor(
    readonly command: MCommand,
    readonly peerId: ID,
  ) {
    super()
  }
}

@MykoReport()
export class PeerReport<T extends MReport<unknown>> extends MReport<
  MReportType<T>
> {
  constructor(
    readonly report: T,
    readonly peerId: ID,
  ) {
    super()
  }
}
