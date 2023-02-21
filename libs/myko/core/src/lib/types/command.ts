import { filter } from 'rxjs'
import { v4 as uuid } from 'uuid'
export class MCommand {
  readonly tx: string
  constructor() {
    this.tx = uuid()
  }
}

export const MYKO_HANDLER_COMMAND_ID_KEY = '__MYKO_HANDLER_COMMAND_KEY__'
export const MYKO_COMMAND_ID_KEY = '__MYKO_COMMAND_ID_KEY__'

export interface MCommandHandler<T extends MCommand> {
  execute(command: T): Promise<void>
}

export const ofCommand = <T extends MCommand>(
  filterCommand: new (...args: any[]) => T,
) =>
  filter(
    (command: T) =>
      Reflect.getMetadata(MYKO_COMMAND_ID_KEY, filterCommand) ===
      Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command),
  )
