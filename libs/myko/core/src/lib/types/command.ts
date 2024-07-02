import { OperatorFunction, filter } from 'rxjs'

import { DateTime } from 'luxon'
import { v4 as uuid } from 'uuid'
import { MYKO_COMMAND_ID_KEY } from '../constants'
import type { ID } from './base'

/**
 * The base class for Myko commands.
 */
export class MCommand<T = void> {
  /**
   * The result of the command execution.
   */
  $commandResult: T
  /**
   * The transaction ID associated with the command.
   */
  readonly tx: string
  /**
   * The user token associated with the command.
   */
  userToken: string | undefined
  /**
   * The timestamp when the command was created.
   */
  readonly createdAt: string

  constructor() {
    this.tx = uuid()
    this.createdAt = DateTime.utc().toString()
  }

  /**
   * Sets the transaction ID for the command.
   * @param tx The transaction ID.
   * @returns The modified command instance.
   */
  withTransaction(tx: ID): this {
    Reflect.set(this, 'tx', tx)
    return this
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
