/**
 * Decorators for Myko commands and command handlers.
 * @module command.decorators
 */

import { commandBus } from '../busses'
import { MYKO_COMMAND_ID_KEY, MYKO_HANDLER_COMMAND_ID_KEY } from '../constants'
import { addCommandDoc, commandHandlers, commands, noAuthCommands } from '../registry'
import { allowedDuringWindback } from '../registry/windback.registry'
import type { MCommand, MCommandHandler, MCommandResponse } from '../types'

/**
 * Decorator for defining a Myko command.
 * @param {string} commandId - The unique identifier for the command.
 * @returns {Function} - The decorator function.
 */
export const MykoCommand: (opts?: {
  noHandler?: boolean
  allowDuringWindback?: boolean
  noAuth?: boolean
}) => <T extends MCommand<MCommandResponse<T>>>(target: new (...args: any[]) => T) => any =
  (opts) =>
  <T extends MCommand<MCommandResponse<T>>>(target: new (...args: any[]) => T) => {
    const original: any = target

    const commandName = Object.getOwnPropertyDescriptors(original)?.name.value

    const commandId = commandName

    if (!opts?.noHandler) {
      commands.add(commandId)
    }

    if (opts?.noAuth) {
      noAuthCommands.add(commandId)
    }

    if (opts?.allowDuringWindback) {
      allowedDuringWindback.add(commandId)
    }

    const paramtypes = Reflect.getMetadata('design:paramtypes', original)?.map((x: { name: string }) => x.name) ?? []

    addCommandDoc(
      {
        commandId,
        commandName,
        ctor: original,
      },
      paramtypes,
    )

    if (!commandId) {
      throw new Error('commandId is undefined')
    }

    // Use regular function (not arrow) to be callable with `new`
    function withType(this: any, ...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, withType)
    return withType
  }

/**
 * Decorator for defining a Myko command handler.
 * @param {Function} command - The command class.
 * @returns {Function} - The decorator function.
 */
export const MykoCommandHandler: <T extends MCommand<MCommandResponse<T>>>(
  command: new (...args: any[]) => T,
) => (target: new (...args: any[]) => MCommandHandler<T>) => void = <T extends MCommand<MCommandResponse<T>>>(
  command: new (...args: any[]) => T,
) => {
  return (target: new (...args: any[]) => MCommandHandler<T>) => {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)

    commandHandlers.add(commandId)
    Reflect.defineMetadata(MYKO_HANDLER_COMMAND_ID_KEY, commandId, target)

    commandBus.bind(new target(), commandId)
  }
}
