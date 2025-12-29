import { doc, MykoCommand, MykoItem, MykoQuery, MykoReport } from '../decorators'
import { MCommand, MItem, MQuery, MReport, type ID } from '../types'
import type { LogLevel } from './logLevel.type'

@MykoItem({
  preventLogs: true,
})
export class Log extends MItem<Log> {
  @doc()
  text!: string
  @doc()
  level!: LogLevel
  @doc()
  data: any
  @doc()
  timestamp!: string // ISO8601
  @doc()
  serverId!: ID
  @doc()
  loggerName!: string
}

@MykoQuery(Log)
export class GetLogs extends MQuery<Log> {
  constructor(readonly serverId: ID) {
    super()
  }
}

@MykoReport()
export class Loggers extends MReport<string[]> {}

@MykoReport()
export class ServerLogLevel extends MReport<LogLevel> {
  constructor(readonly serverId: ID) {
    super()
  }
}

@MykoCommand()
export class SetLogLevel extends MCommand {
  constructor(
    readonly serverId: ID,
    readonly level: LogLevel,
  ) {
    super()
  }
}
