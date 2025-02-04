import {
  finalize,
  firstValueFrom,
  map,
  ReplaySubject,
  share,
  throwError,
} from 'rxjs'
import { MYKO_HANDLER_QUERY_ID_KEY, MYKO_QUERY_ID_KEY } from '../constants'
import type {
  MItem,
  MLiveQueryResult,
  MQuery,
  MQueryHandler,
  MQueryHandlerConstructor,
  MQueryResult,
} from '../types'
import { ObservableBus } from './observable.bus'

/**
 * Abstract class representing a query bus in the Myko framework.
 * A query bus is responsible for handling queries and returning query results.
 * @template MQuery The type of the query.
 */
export abstract class AMykoQueryBus extends ObservableBus<MQuery> {
  constructor(private readonly options?: { disableCache?: boolean }) {
    super()
  }

  protected handlers: Map<string, MQueryHandler<MQuery>> = new Map()

  /**
   * Binds a query handler to an identifier.
   * @param handler The query handler to bind.
   * @param id The identifier for the query handler.
   */
  protected bind<T extends MItem>(
    handler: MQueryHandler<MQuery<T>>,
    id: string,
  ): void {
    this.handlers.set(id, handler)
  }

  /**
   * Registers multiple query handlers.
   * @param handlers An array of query handlers to register.
   */
  register(handlers: MQueryHandlerConstructor<MQuery<MItem>>[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  /**
   * Watches a query and returns a live query result.
   * @param query The query to watch.
   * @returns The live query result.
   * @template T The type of the query.
   */
  private cache = new Map<string, MLiveQueryResult<MQuery>>()

  watch<T extends MQuery>(query: T): MLiveQueryResult<T> {
    const queryId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, query)
    const handler = this.handlers.get(queryId)

    const err = `Handler not Provided for [${queryId}]`

    if (!handler) {
      return throwError(() => new Error(err)) as unknown as MLiveQueryResult<T>
    }

    let clone: MQuery = {
      ...query,
      tx: undefined,
    }

    const hash = JSON.stringify(clone)

    const cacheKey = `${queryId}:${hash}`

    if (this.cache.has(cacheKey) && !this.options?.disableCache) {
      return this.cache.get(cacheKey) as MLiveQueryResult<T>
    }

    const obs = handler.execute(query).pipe(
      share({
        connector: () => new ReplaySubject(1),
      }),
      // clone the array so subsequent mutations dont ruin it for everyone else
      map((x) => x.slice()),
      finalize(() => {
        this.cache.delete(cacheKey)
      }),
    ) as MLiveQueryResult<T>

    this.cache.set(cacheKey, obs)

    return obs
  }

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
  abstract registerHandler(
    handler: MQueryHandlerConstructor<MQuery<MItem>>,
  ): void
}

export class MQueryBus extends AMykoQueryBus {
  registerHandler(handler: MQueryHandlerConstructor<MQuery<MItem>>): void {
    const queryId = Reflect.getMetadata(MYKO_HANDLER_QUERY_ID_KEY, handler)

    this.bind(new handler(), queryId)
  }
}

export const queryBus: MQueryBus = new MQueryBus()
