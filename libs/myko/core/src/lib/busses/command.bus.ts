import 'reflect-metadata'
import { Constructor, IMykoCommand, IMykoCommandHandler } from '../types'
import { ObservableBus } from './observable.bus'

export type MykoCommandHandlerType = Constructor<
  IMykoCommandHandler<IMykoCommand>
>

export abstract class AMykoCommandBus extends ObservableBus<IMykoCommand> {
  constructor() {
    super()
  }

  protected handlers = new Map<string, IMykoCommandHandler<IMykoCommand>>()

  abstract execute<T extends IMykoCommand>(command: T): Promise<void>

  bind<T extends IMykoCommand>(
    handler: IMykoCommandHandler<T>,
    id: string,
  ): void {
    this.handlers.set(id, handler)
  }

  register(handlers: MykoCommandHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  protected abstract registerHandler(handler: MykoCommandHandlerType): void
}
