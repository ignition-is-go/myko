import { BehaviorSubject, type Observable, shareReplay } from 'rxjs'
import { LogLevel } from './logLevel.type'

const names = new Map<string, {}>()

const namesSubject = new BehaviorSubject(names)

export const getNames = () => namesSubject.value

export const nameStream = namesSubject.pipe(shareReplay(1))

export const addName = (name: string) => {
  names.set(name, {})
  namesSubject.next(names)
}

export class Cell<T> {
  #subj: BehaviorSubject<T>

  constructor(public value: T) {
    this.#subj = new BehaviorSubject<T>(this.value)
  }

  set(value: T) {
    this.value = value
    this.#subj.next(value)
  }

  get() {
    return this.value
  }

  stream(): Observable<T> {
    return this.#subj.asObservable()
  }
}

export const longestName = new Cell<number>(0)

export const logsPreventedEntities = new Set<string>()

/**
 * Log output format.
 * - 'human': Human-readable format with aligned columns (default for development)
 * - 'json': Structured JSON format (default for production)
 */
export type LogFormat = 'human' | 'json'

/**
 * Configure the log output format.
 * Can also be set via LOG_FORMAT environment variable.
 */
export const logFormat = new Cell<LogFormat>(
  typeof process !== 'undefined' && process.env?.['LOG_FORMAT'] === 'json'
    ? 'json'
    : 'human',
)
const getDefaultLogLevel = (): LogLevel => {
  const envLevel =
    (typeof process !== 'undefined' &&
      process.env?.['MYKO_INITIAL_LOG_LEVEL']) ||
    undefined
  if (!envLevel) return LogLevel.INFO

  const normalized = envLevel.toUpperCase()
  switch (normalized) {
    case 'ERROR':
      return LogLevel.ERROR
    case 'WARN':
      return LogLevel.WARN
    case 'INFO':
      return LogLevel.INFO
    case 'DEBUG':
      return LogLevel.DEBUG
    case 'VERBOSE':
      return LogLevel.VERBOSE
    default:
      console.warn(
        `Invalid MYKO_INITIAL_LOG_LEVEL="${envLevel}", defaulting to INFO. Valid values: ERROR, WARN, INFO, DEBUG, VERBOSE`,
      )
      return LogLevel.INFO
  }
}

export const logFilter = new Cell<LogLevel>(getDefaultLogLevel())
