import { MYKO_COMMAND_ID_KEY } from '../constants'
import { CommandResponse, MCommand } from '../types'
import { CommandUnwrapError } from '../types/errors'

export interface MWrappedCommand {
  command: MCommand<unknown>
  commandId: string
}

export const wrapCommand = <T extends MCommand<unknown>>(
  command: T,
): MWrappedCommand => {
  const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)

  if (!commandId) {
    throw new CommandUnwrapError()
  }

  return {
    command,
    commandId,
  }
}

export const unwrapCommand = <T extends MCommand<CommandResponse<T>>>(
  wrappedCommand: MWrappedCommand,
): MCommand<CommandResponse<T>> => {
  const { command, commandId } = wrappedCommand
  Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, command)
  return command as T
}
