import 'reflect-metadata'
import { firstValueFrom } from 'rxjs'
import type {
  MLiveQueryResult,
  MQuery,
  MQueryHandler,
  MQueryResult,
  Type,
} from '../types'
import { ObservableBus } from './observable.bus'

export type MykoQueryHandlerType = Type<MQueryHandler<MQuery>>

/**
 * Abstract class representing a query bus in the Myko framework.
 * A query bus is responsible for handling queries and returning query results.
 * @template MQuery The type of the query.
 */
export abstract class AMykoQueryBus extends ObservableBus<MQuery> {
  constructor() {
    super()
  }

  protected handlers: Map<string, MQueryHandler<MQuery>> = new Map()

  /**
   * Binds a query handler to an identifier.
   * @param handler The query handler to bind.
   * @param id The identifier for the query handler.
   */
  protected bind<T>(handler: MQueryHandler<MQuery>, id: string): void {
    this.handlers.set(id, handler)
  }

  /**
   * Registers multiple query handlers.
   * @param handlers An array of query handlers to register.
   */
  register(handlers: MykoQueryHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  /**
   * Watches a query and returns a live query result.
   * @param query The query to watch.
   * @returns The live query result.
   * @template T The type of the query.
   */
  abstract watch<T extends MQuery>(query: T): MLiveQueryResult<T>

  /**
   * Executes a query and returns the query result.
   * @param query The query to execute.
   * @returns The query result.
   * @template T The type of the query.
   */
  execute<T extends MQuery>(query: T): MQueryResult<T> {
    // console.log(wrapQuery(query).queryId)
    return firstValueFrom(this.watch(query)) as MQueryResult<T>
  }

  /**
   * Registers a query handler.
   * @param handler The query handler to register.
   */
  protected abstract registerHandler(handler: MykoQueryHandlerType): void
}
