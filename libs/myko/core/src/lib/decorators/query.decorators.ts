import 'reflect-metadata'

import {
  IMykoItem,
  IMykoQuery,
  IMykoQueryHandler,
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_QUERY_ID_KEY,
} from '../types'

export const MykoQuery =
  (commandId: string) =>
  <T extends IMykoQuery<IMykoItem | IMykoItem[]>>(
    target: new (...args: any[]) => T,
  ) => {
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_QUERY_ID_KEY, commandId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_QUERY_ID_KEY, commandId, withType)
    return withType
  }

export const MykoQueryHandler = <T extends IMykoQuery<IMykoItem | IMykoItem[]>>(
  command: new (...args: any[]) => T,
) => {
  return (target: new () => IMykoQueryHandler<T>) => {
    const commandId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, command)
    Reflect.defineMetadata(MYKO_HANDLER_QUERY_ID_KEY, commandId, target)
  }
}
