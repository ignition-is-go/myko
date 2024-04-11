import { Injectable } from '@nestjs/common'
import {
  AMykoQueryBus,
  MQuery,
  MykoQueryHandlerType,
  MLiveQueryResult,
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_QUERY_ID_KEY,
  wrapQuery,
  ID,
} from '@myko/core'
import { ModuleRef } from '@nestjs/core'
import { LoggerService } from '@rship/logging'
import { firstValueFrom, of, shareReplay } from 'rxjs'

@Injectable()
export class MykoQueryBus extends AMykoQueryBus {
  constructor(
    private moduleRef: ModuleRef,
    private logger: LoggerService,
  ) {
    super()
  }

  cache = new Map<string, MLiveQueryResult<MQuery>>()

  watch<T extends MQuery>(query: T): MLiveQueryResult<T> {
    const queryId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, query)
    const handler = this.handlers.get(queryId)

    const err = `Handler not Provided for ${query.constructor.name} [${queryId}]. Check your module's providers array, and that the command is decorated with @MykoQuery(id: string)`

    if (!handler) {
      this.logger
        .getLogger('MykoQueryBus')
        .dev.error({ message: err, data: query })
      throw new Error(err)
    }

    let clone: MQuery = {
      ...query,
      tx: undefined,
    }

    const txKey = `${queryId}:${query.tx}`
    const hash = JSON.stringify(clone)

    const cacheKey = `${queryId}:${hash}`

    if (this.cache.has(cacheKey)) {
      console.log('returning cached query for', cacheKey)
      return this.cache.get(cacheKey) as MLiveQueryResult<T>
    }

    const obs = handler.execute(query) as MLiveQueryResult<T>

    this.cache.set(cacheKey, obs.pipe(shareReplay(1)))

    console.time(txKey)
    firstValueFrom(obs).then((res) => {
      console.timeEnd(txKey)
    })

    return obs
    // console.log(queryId)
  }

  protected registerHandler(handler: MykoQueryHandlerType): void {
    const instance = this.moduleRef.get(handler, {
      strict: false,
    })

    if (!instance) {
      throw new Error(`Cannot find instance of ${handler.constructor.name}`)
    }
    const queryId = Reflect.getMetadata(MYKO_HANDLER_QUERY_ID_KEY, handler)

    this.bind(instance, queryId)
  }
}
