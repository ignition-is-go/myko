import 'reflect-metadata'

import {
  IMykoCommand,
  IMykoCommandHandler,
  MYKO_COMMAND_ID_KEY,
  MYKO_HANDLER_COMMAND_ID_KEY,
} from '../types'

export const MykoCommand =
  (commandId: string) =>
  <T extends IMykoCommand>(target: new (...args: any[]) => T) => {
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, withType)
    return withType
  }

export const MykoCommandHandler = <T extends IMykoCommand>(
  command: new (...args: any[]) => T,
) => {
  return (target: new (...args: any[]) => IMykoCommandHandler<T>) => {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)
    Reflect.defineMetadata(MYKO_HANDLER_COMMAND_ID_KEY, commandId, target)
  }
}
