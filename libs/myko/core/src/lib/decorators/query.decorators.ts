import {
  MItem,
  MQuery,
  MQueryHandler,
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_QUERY_ID_KEY,
} from '../types'

export const MykoQuery =
  (commandId: string) =>
  <T extends MQuery<MItem | MItem[]>>(target: new (...args: any[]) => T) => {
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_QUERY_ID_KEY, commandId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_QUERY_ID_KEY, commandId, withType)
    return withType
  }

export const MykoQueryHandler = <T extends MQuery<MItem | MItem[]>>(
  command: new (...args: any[]) => T,
) => {
  return (target: new () => MQueryHandler<T>) => {
    const commandId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, command)
    Reflect.defineMetadata(MYKO_HANDLER_QUERY_ID_KEY, commandId, target)
  }
}
