import 'reflect-metadata'
import { Type, MItem, MQuery, MQueryHandler, MQueryResult } from '../types'
import { ObservableBus } from './observable.bus'

export type MykoQueryHandlerType = Type<MQueryHandler<MQuery>>

export abstract class AMykoQueryBus extends ObservableBus<MQuery> {
  constructor() {
    super()
  }

  protected handlers = new Map<string, MQueryHandler<MQuery>>()

  protected bind<T>(handler: MQueryHandler<MQuery>, id: string): void {
    this.handlers.set(id, handler)
  }

  register(handlers: MykoQueryHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  abstract execute<T extends MQuery>(query: T): MQueryResult<T>

  protected abstract registerHandler(handler: MykoQueryHandlerType): void
}
