import { doc, MykoItem, MykoQuery, MykoReport } from '../decorators'
import { MItem, MQuery, MReport, type ID } from '../types'
export enum LogLevel {
  INFO = 'INFO',
  WARN = 'WARN',
  ERROR = 'ERROR',
  LOG = 'LOG',
}
@MykoItem()
export class Log extends MItem<Log> {
  @doc()
  text: string
  @doc()
  level: LogLevel
  @doc()
  data: any
  @doc()
  timestamp: string // ISO8601
  @doc()
  serverId: ID
  @doc()
  loggerName: string
}

@MykoQuery(Log)
export class GetLogs extends MQuery<Log> {
  constructor(readonly serverId: ID) {
    super()
  }
}

@MykoReport()
export class Loggers extends MReport<string[]> {
  constructor() {
    super()
  }
}
