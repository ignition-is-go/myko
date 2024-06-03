import { MYKO_COMMAND_ID_KEY } from '../constants'
import { MCommand, MCommandResponse } from '../types'
import { CommandUnwrapError } from '../types/errors'

/**
 * Represents a wrapped command.
 */
export interface MWrappedCommand {
  command: MCommand<unknown>
  commandId: string
}

/**
 * Wraps a command with its associated command ID.
 * @param command The command to wrap.
 * @returns The wrapped command.
 * @throws {CommandUnwrapError} If the command does not have a command ID.
 */
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

/**
 * Unwraps a wrapped command and restores its command ID.
 * @param wrappedCommand The wrapped command to unwrap.
 * @returns The unwrapped command.
 */
export const unwrapCommand = <T extends MCommand<MCommandResponse<T>>>(
  wrappedCommand: MWrappedCommand,
): MCommand<MCommandResponse<T>> => {
  const { command, commandId } = wrappedCommand
  Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, command)
  return command as T
}
