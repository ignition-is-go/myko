import { ID } from './base'

export interface IMykoCommand {}

export const MYKO_HANDLER_COMMAND_ID_KEY = '__MYKO_HANDLER_COMMAND_KEY__'
export const MYKO_COMMAND_ID_KEY = '__MYKO_COMMAND_ID_KEY__'

export interface IMykoCommandHandler<T extends IMykoCommand> {
  execute(command: T): Promise<void>
}
