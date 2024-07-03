import colors from 'colors'
import { longestName, names } from './registry'

export enum LogLevel {
  INFO = 'INFO',
  WARN = 'WARN',
  ERROR = 'ERROR',
  LOG = 'LOG',
}

export class MykoLogger {
  constructor(private name?: string) {
    names.set(name, {})

    if (name.length > longestName.get()) {
      longestName.set(name.length)
    }
  }

  private fmt(level: LogLevel, args: any[]) {
    const colorLevel =
      level == LogLevel.ERROR
        ? colors.red
        : level == LogLevel.WARN
          ? colors.yellow
          : colors.green

    return [
      colors.gray(new Date().toLocaleDateString()),
      colors.gray(new Date().toLocaleTimeString()),
      colors.gray('|'),
      colorLevel(level),
      colors.gray('|'),
      this.name ? `${this.name.padEnd(longestName.get())}` : ``,
      colors.gray('|'),
      args.join(' '),
    ].join(' ')
  }

  error(...message: any[]) {
    console.error(this.fmt(LogLevel.ERROR, message))
  }

  warn(...message: any[]) {
    console.warn(this.fmt(LogLevel.WARN, message))
  }

  info(...message: any[]) {
    console.info(this.fmt(LogLevel.INFO, message))
  }
}
