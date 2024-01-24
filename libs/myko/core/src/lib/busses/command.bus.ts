import 'reflect-metadata'
import { Type, MCommand, MCommandHandler, MCommandResponse } from '../types'
import { ObservableBus } from './observable.bus'
import { MYKO_COMMAND_ID_KEY } from '../constants'

export type MCommandHandlerType = Type<MCommandHandler<MCommand<unknown>>>

export abstract class AMykoCommandBus extends ObservableBus<MCommand> {
  constructor() {
    super()
  }

  async execute<T extends MCommand<MCommandResponse<T>>>(
    command: T,
  ): Promise<MCommandResponse<T>> {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)
    const handler = this.handlers.get(commandId)

    const err = `Handler not Provided for ${command.constructor.name} [${commandId}]. Check your module's providers array, and that the command is decorated with @MykoCommand(id: string)`

    if (!handler) {
      console.error(err, command)
      throw err
    }
    console.log(commandId)
    return (await handler.execute(command)) as MCommandResponse<T>
  }

  protected handlers = new Map<string, MCommandHandler<MCommand<unknown>>>()

  bind<T extends MCommand<MCommandResponse<T>>>(
    handler: MCommandHandler<T>,
    id: string,
  ): void {
    this.handlers.set(id, handler)
  }

  register(handlers: MCommandHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  protected abstract registerHandler(handler: MCommandHandlerType): void
}
