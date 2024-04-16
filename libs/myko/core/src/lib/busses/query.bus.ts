import 'reflect-metadata'
import { firstValueFrom } from 'rxjs'
import {
  MLiveQueryResult,
  MQuery,
  MQueryHandler,
  MQueryResult,
  Type,
} from '../types'
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

  abstract watch<T extends MQuery>(query: T): MLiveQueryResult<T>

  execute<T extends MQuery>(query: T): MQueryResult<T> {
    // console.log(wrapQuery(query).queryId)
    return firstValueFrom(this.watch(query)) as MQueryResult<T>
  }

  protected abstract registerHandler(handler: MykoQueryHandlerType): void
}
