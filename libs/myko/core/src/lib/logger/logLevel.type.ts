export enum LogLevel {
  INFO = 'INFO',
  WARN = 'WARN',
  ERROR = 'ERROR',
  DEBUG = 'DEBUG',
}

export const LOG_RANK = [
  LogLevel.ERROR,
  LogLevel.WARN,
  LogLevel.INFO,
  LogLevel.DEBUG,
]

export const levelShouldPrint = (level: LogLevel, filterLevel: LogLevel) => {
  return LOG_RANK.indexOf(level) <= LOG_RANK.indexOf(filterLevel)
}
