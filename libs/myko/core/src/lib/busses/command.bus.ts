import 'reflect-metadata'
import { Type, MCommand, MCommandHandler, MYKO_COMMAND_ID_KEY } from '../types'
import { ObservableBus } from './observable.bus'

export type MCommandHandlerType = Type<MCommandHandler<MCommand>>

export abstract class AMykoCommandBus extends ObservableBus<MCommand> {
  constructor() {
    super()
  }

  execute<T extends MCommand>(command: T): Promise<void> {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)
    const handler = this.handlers.get(commandId)

    const err = `Handler not Provided for ${command.constructor.name} [${commandId}]. Check your module's providers array, and that the command is decorated with @MykoCommand(id: string)`

    if (!handler) {
      console.error(err, command)
      throw err
    }

    return handler.execute(command)
  }

  protected handlers = new Map<string, MCommandHandler<MCommand>>()

  bind<T extends MCommand>(handler: MCommandHandler<T>, id: string): void {
    this.handlers.set(id, handler)
  }

  register(handlers: MCommandHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  protected abstract registerHandler(handler: MCommandHandlerType): void
}
