import 'reflect-metadata'
import { filter, type OperatorFunction } from 'rxjs'

import { MYKO_COMMAND_ID_KEY } from '../constants'
import type { MWrappedCommand } from '../wrappers'
import { WithContext } from './base'

/**
 * The base class for Myko commands.
 */
export class MCommand<T = void> extends WithContext {
  /**
   * The result of the command execution.
   */
  $commandResult!: T

  /**
   * The user token associated with the command.
   */
  userToken: string | undefined

  /**
   * Wraps the command in a command wrapper.
   * @returns The wrapped command.
   */
  wrap(): MWrappedCommand {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, this)
    if (!commandId) {
      throw new Error('Could not get command ID from Metadata')
    }

    return {
      command: this,
      commandId,
    }
  }

  /**
   * Creates a command from a wrapped command.
   * @param wrappedCommand The wrapped command to unwrap.
   * @returns The unwrapped command.
   */
  static fromWrappedCommand<R>(wrappedCommand: MWrappedCommand): MCommand<R> {
    const { command: commandInner, commandId } = wrappedCommand
    const command = new MCommand<R>()

    Object.assign(command, commandInner)

    Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, command)

    return command
  }

  getTag(): string {
    return Reflect.getMetadata(MYKO_COMMAND_ID_KEY, this)
  }
}

/**
 * Extracts the response type from a command type.
 */
export type MCommandResponse<T> = T extends MCommand<infer R> ? R : never

/**
 * Represents a command handler that can execute a specific command.
 */
export interface MCommandHandler<T extends MCommand<MCommandResponse<T>>> {
  /**
   * Executes the given command and returns the result.
   * @param command The command to execute.
   * @returns A promise that resolves to the result of the command execution.
   */
  execute(command: T): Promise<MCommandResponse<T>>
}

/**
 * Creates an operator function that filters commands of a specific type.
 * @param filterCommand The command class to filter.
 * @returns An operator function that filters commands of the specified type.
 */
export const ofCommand: <T extends MCommand<void>>(
  filterCommand: new (...args: any[]) => T,
) => OperatorFunction<MCommand<void>, T> = <T extends MCommand>(
  filterCommand: new (...args: any[]) => T,
) =>
  filter(
    (command: MCommand): command is T =>
      Reflect.getMetadata(MYKO_COMMAND_ID_KEY, filterCommand) ===
      Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command),
  )
