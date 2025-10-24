import { DateTime } from 'luxon'
import { v4 } from 'uuid'
import { getHostId } from '../registry'
import type { ID } from '../types'
import { Log } from './log.type'
import { levelShouldPrint, LogLevel } from './logLevel.type'
import { addName, logFilter, longestName } from './registry'

export class MykoLogger {
  constructor(private name: string = '') {
    addName(name)

    if (name.length > longestName.get()) {
      longestName.set(name.length)
    }
  }

  private fmt(level: LogLevel, args: string) {
    return [
      new Date().toLocaleDateString(),
      new Date().toLocaleTimeString(),
      '|',
      level.padEnd(5),
      '|',
      this.name ? `${this.name.padEnd(longestName.get())}` : ``,
      '|',
      args,
    ].join(' ')
  }

  error(message: string, data?: any, tx?: ID) {
    if (levelShouldPrint(LogLevel.ERROR, logFilter.get())) {
      console.error(this.fmt(LogLevel.ERROR, message))
      if (data) {
        console.error(data)
      }
    }
    const log = new Log({
      data,
      id: v4(),
      level: LogLevel.ERROR,
      text: message,
      serverId: getHostId(),
      timestamp: DateTime.utc().toISO(),
      loggerName: this.name,
    })

    // eventBus.publishSet(log, tx ?? v4())
  }

  warn(message: string, data?: any, tx?: ID) {
    if (levelShouldPrint(LogLevel.WARN, logFilter.get())) {
      console.warn(this.fmt(LogLevel.WARN, message))
    }
    const log = new Log({
      data,
      id: v4(),
      level: LogLevel.WARN,
      text: message,
      serverId: getHostId(),
      timestamp: DateTime.utc().toISO(),
      loggerName: this.name,
    })

    // eventBus.publishSet(log, tx ?? v4())
  }

  info(message: string, data?: any, tx?: ID) {
    if (levelShouldPrint(LogLevel.INFO, logFilter.get())) {
      console.info(this.fmt(LogLevel.INFO, message))
    }
    const log = new Log({
      data,
      id: v4(),
      level: LogLevel.INFO,
      text: message,
      serverId: getHostId(),
      timestamp: DateTime.utc().toISO(),
      loggerName: this.name,
    })

    // eventBus.publishSet(log, tx ?? v4())
  }

  debug(message: string, data?: any, tx?: ID) {
    if (levelShouldPrint(LogLevel.DEBUG, logFilter.get())) {
      console.debug(this.fmt(LogLevel.DEBUG, message))
    }

    const log = new Log({
      data,
      id: v4(),
      level: LogLevel.DEBUG,
      text: message,
      serverId: getHostId(),
      timestamp: DateTime.utc().toISO(),
      loggerName: this.name,
    })

    // eventBus.publishSet(log, tx ?? v4())
  }
}
