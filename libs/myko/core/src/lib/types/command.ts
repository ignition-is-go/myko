export interface MCommand {}

export const MYKO_HANDLER_COMMAND_ID_KEY = '__MYKO_HANDLER_COMMAND_KEY__'
export const MYKO_COMMAND_ID_KEY = '__MYKO_COMMAND_ID_KEY__'

export interface MCommandHandler<T extends MCommand> {
  execute(command: T): Promise<void>
}
