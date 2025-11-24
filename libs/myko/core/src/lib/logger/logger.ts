import { DateTime } from 'luxon'
import { v4 } from 'uuid'
import { getHostId } from '../registry'
import type { ID } from '../types'
import { Log } from './log.type'
import { levelShouldPrint, LogLevel } from './logLevel.type'
import { addName, logFilter, longestName } from './registry'

const colorForLevel = (lvl: LogLevel): string => {
  switch (lvl) {
    case LogLevel.ERROR:
      return '\x1b[31m' // red
    case LogLevel.WARN:
      return '\x1b[33m' // yellow
    case LogLevel.INFO:
      return '\x1b[36m' // cyan
    case LogLevel.DEBUG:
      return '\x1b[35m' // magenta
    case LogLevel.VERBOSE:
      return '\x1b[90m' // gray
    default:
      return ''
  }
}

// Colorize console output based on log level by returning early with a colored string.
// This prevents the uncolored fallback return below from executing.
const supportsColor =
  typeof process !== 'undefined' &&
  !!process.stdout &&
  typeof process.stdout.isTTY === 'boolean' &&
  process.stdout.isTTY

const addColor = (str: string, level: LogLevel): string => {
  if (!supportsColor) {
    return str
  }

  const levelColor = colorForLevel(level)
  return levelColor + str + '\x1b[0m'
}

export class MykoLogger {
  constructor(private name: string = '') {
    addName(name)

    if (name.length > longestName.get()) {
      longestName.set(name.length)
    }
  }

  private fmt(level: LogLevel, args: string) {
    const dateStr = new Date().toLocaleDateString()
    const timeStr = new Date().toLocaleTimeString()
    const nameStr = this.name ? `${this.name.padEnd(longestName.get())}` : ``
    const coloredLevel = addColor(level.padEnd(5), level)

    return [dateStr, timeStr, '|', coloredLevel, '|', nameStr, '|', args].join(
      ' ',
    )
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

  verbose(message: string, data?: any, tx?: ID) {
    if (levelShouldPrint(LogLevel.VERBOSE, logFilter.get())) {
      console.log(this.fmt(LogLevel.VERBOSE, message))
      if (data) {
        console.dir(data)
      }
    }

    const log = new Log({
      data,
      id: v4(),
      level: LogLevel.VERBOSE,
      text: message,
      serverId: getHostId(),
      timestamp: DateTime.utc().toISO(),
      loggerName: this.name,
    })

    // eventBus.publishSet(log, tx ?? v4())
  }
}
