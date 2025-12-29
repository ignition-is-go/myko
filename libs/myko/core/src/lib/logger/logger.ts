import { DateTime } from 'luxon'
import { v4 } from 'uuid'
import { getHostId } from '../registry'
import type { ID } from '../types'
import { Log } from './log.type'
import { levelShouldPrint, LogLevel } from './logLevel.type'
import { addName, logFilter, logFormat, longestName } from './registry'

<<<<<<< HEAD
/**
 * Structured logger with configurable output format.
 *
 * Supports two output formats:
 * - 'human': Human-readable format with aligned columns (default)
 * - 'json': Structured JSON format for log aggregation
 *
 * Configure via:
 * - Environment variable: LOG_FORMAT=json
 * - Programmatically: logFormat.set('json')
 *
 * @example
 * ```ts
 * const logger = new MykoLogger('MyService')
 * logger.info('Server started', { port: 3000 })
 *
 * // Human output: 11/29/2024 10:30:00 | INFO  | MyService | Server started
 * // JSON output:  {"timestamp":"2024-11-29T10:30:00.000Z","level":"INFO","logger":"MyService","message":"Server started","data":{"port":3000}}
 * ```
 */
=======
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

>>>>>>> origin/dev
export class MykoLogger {
  constructor(private name: string = '') {
    addName(name)

    if (name.length > longestName.get()) {
      longestName.set(name.length)
    }
  }

<<<<<<< HEAD
  /**
   * Format log entry for human-readable output.
   */
  private fmtHuman(level: LogLevel, message: string): string {
    return [
      new Date().toLocaleDateString(),
      new Date().toLocaleTimeString(),
      '|',
      level.padEnd(5),
      '|',
      this.name ? `${this.name.padEnd(longestName.get())}` : ``,
      '|',
      message,
    ].join(' ')
=======
  private fmt(level: LogLevel, args: string) {
    const dateStr = new Date().toLocaleDateString()
    const timeStr = new Date().toLocaleTimeString()
    const nameStr = this.name ? `${this.name.padEnd(longestName.get())}` : ``
    const coloredLevel = addColor(level.padEnd(5), level)

    return [dateStr, timeStr, '|', coloredLevel, '|', nameStr, '|', args].join(
      ' ',
    )
>>>>>>> origin/dev
  }

  /**
   * Format log entry as structured JSON.
   */
  private fmtJson(level: LogLevel, message: string, data?: unknown): string {
    const entry: Record<string, unknown> = {
      timestamp: new Date().toISOString(),
      level,
      message,
    }
    if (this.name) {
      entry.logger = this.name
    }
    if (data !== undefined) {
      entry.data = data
    }
    return JSON.stringify(entry)
  }

  /**
   * Get formatted output based on current log format setting.
   */
  private fmt(level: LogLevel, message: string, data?: unknown): string {
    const format = logFormat.get()
    if (format === 'json') {
      return this.fmtJson(level, message, data)
    }
    return this.fmtHuman(level, message)
  }

  /**
   * Output data in the appropriate format.
   * In JSON mode, data is included in the main log line.
   * In human mode, data is output separately for readability.
   */
  private outputData(data: unknown): void {
    const format = logFormat.get()
    if (format === 'human' && data !== undefined) {
      console.dir(data, { depth: 4 })
    }
    // In JSON mode, data is already included in the main log line
  }

  error(message: string, data?: unknown, _tx?: ID) {
    if (levelShouldPrint(LogLevel.ERROR, logFilter.get())) {
      console.error(this.fmt(LogLevel.ERROR, message, data))
      this.outputData(data)
    }
    const _log = new Log({
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

  warn(message: string, data?: unknown, _tx?: ID) {
    if (levelShouldPrint(LogLevel.WARN, logFilter.get())) {
      console.warn(this.fmt(LogLevel.WARN, message, data))
      this.outputData(data)
    }
    const _log = new Log({
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

  info(message: string, data?: unknown, _tx?: ID) {
    if (levelShouldPrint(LogLevel.INFO, logFilter.get())) {
      console.info(this.fmt(LogLevel.INFO, message, data))
      this.outputData(data)
    }
    const _log = new Log({
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

  debug(message: string, data?: unknown, _tx?: ID) {
    if (levelShouldPrint(LogLevel.DEBUG, logFilter.get())) {
      console.debug(this.fmt(LogLevel.DEBUG, message, data))
      this.outputData(data)
    }

    const _log = new Log({
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

  verbose(message: string, data?: unknown, _tx?: ID) {
    if (levelShouldPrint(LogLevel.VERBOSE, logFilter.get())) {
      console.log(this.fmt(LogLevel.VERBOSE, message, data))
      this.outputData(data)
    }

    const _log = new Log({
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
