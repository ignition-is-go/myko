import 'reflect-metadata'
import {
  Constructor,
  IMykoItem,
  IMykoQuery,
  IMykoQueryHandler,
  MykoQueryResult,
} from '../types'
import { ObservableBus } from './observable.bus'

export type MykoQueryable = IMykoItem | IMykoItem[]
export type MykoQueryHandlerType = Constructor<
  IMykoQueryHandler<IMykoQuery<MykoQueryable>>
>

export abstract class AMykoQueryBus extends ObservableBus<
  IMykoQuery<MykoQueryable>
> {
  constructor() {
    super()
  }

  protected handlers = new Map<
    string,
    IMykoQueryHandler<IMykoQuery<MykoQueryable>>
  >()

  protected bind<T>(
    handler: IMykoQueryHandler<IMykoQuery<MykoQueryable>>,
    id: string,
  ): void {
    this.handlers.set(id, handler)
  }

  register(handlers: MykoQueryHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  abstract execute<T extends IMykoQuery<MykoQueryable>>(
    query: T,
  ): MykoQueryResult<T>

  protected abstract registerHandler(handler: MykoQueryHandlerType): void
}
