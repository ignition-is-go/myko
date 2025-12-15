import { DateTime } from 'luxon'
import { v4 } from 'uuid'
import { getHostId } from '../registry'
import type { ID } from '../types'
import { Log } from './log.type'
import { levelShouldPrint, LogLevel } from './logLevel.type'
import { addName, logFilter, logFormat, longestName } from './registry'

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
export class MykoLogger {
  constructor(private name: string = '') {
    addName(name)

    if (name.length > longestName.get()) {
      longestName.set(name.length)
    }
  }

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
