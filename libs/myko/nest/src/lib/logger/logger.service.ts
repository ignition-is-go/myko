import { MYKO_ITEM_TYPE, log } from '@myko/core'
import { LoggerService } from '@nestjs/common'

export class MykoLogger implements LoggerService {
  private l: typeof log

  log(message: any, ...optionalParams: any[]) {
    this.l.info(message, optionalParams)
  }
  error(message: any, ...optionalParams: any[]) {
    this.l.error(message, optionalParams)
  }
  warn(message: any, ...optionalParams: any[]) {
    this.l.warn(message, optionalParams)
  }
  debug?(message: any, ...optionalParams: any[]) {
    this.l.debug(message, optionalParams)
  }
  verbose?(message: any, ...optionalParams: any[]) {
    this.l.silly(message, optionalParams)
  }
  fatal?(message: any, ...optionalParams: any[]) {
    this.l.fatal(message, optionalParams)
  }

  static forMykoItem(ctor: new (...args: any[]) => any) {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, ctor)

    if (!itemType) {
      throw new Error('Cant Create Logger: No item type found')
    }
    return new MykoLogger(itemType)
  }

  constructor(itemType: string) {
    this.l = log.getSubLogger({ name: itemType })
  }
}
