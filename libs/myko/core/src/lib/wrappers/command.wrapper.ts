import { MCommand, type MCommandResponse } from '../types'

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
 * @deprecated use MCommand.wrap instead.
 */
export const wrapCommand = <T extends MCommand<unknown>>(
  command: T,
): MWrappedCommand => {
  return command.wrap()
}

/**
 * Unwraps a wrapped command and restores its command ID.
 * @param wrappedCommand The wrapped command to unwrap.
 * @returns The unwrapped command.
 * @deprecated use MCommand.fromWrappedCommand instead.
 */
export const unwrapCommand = <T extends MCommand<MCommandResponse<T>>>(
  wrappedCommand: MWrappedCommand,
): MCommand<MCommandResponse<T>> => {
  return MCommand.fromWrappedCommand(wrappedCommand)
}
