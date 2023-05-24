import { filter } from 'rxjs'

import { v4 as uuid } from 'uuid'
import { DateTime } from 'luxon'
import { ID } from './base'

export class MCommand<T = void> {
  $result: T
  readonly tx: string
  userToken: string | undefined
  readonly createdAt: string
  constructor() {
    this.tx = uuid()
    this.createdAt = DateTime.utc().toString()
  }

  withTransaction(tx: ID) {
    Reflect.set(this, 'tx', tx)
    return this
  }
}

export type CommandResponse<T> = T extends MCommand<infer R> ? R : never

export const MYKO_HANDLER_COMMAND_ID_KEY = '__MYKO_HANDLER_COMMAND_KEY__'
export const MYKO_COMMAND_ID_KEY = '__MYKO_COMMAND_ID_KEY__'

export interface MCommandHandler<T extends MCommand<CommandResponse<T>>> {
  execute(command: T): Promise<CommandResponse<T>>
}

export const ofCommand = <T extends MCommand>(
  filterCommand: new (...args: any[]) => T,
) =>
  filter(
    (command: MCommand): command is T =>
      Reflect.getMetadata(MYKO_COMMAND_ID_KEY, filterCommand) ===
      Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command),
  )
