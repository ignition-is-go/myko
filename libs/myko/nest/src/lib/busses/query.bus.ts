import { Injectable } from '@nestjs/common'
import {
  AMykoQueryBus,
  MQuery,
  MykoQueryHandlerType,
  MLiveQueryResult,
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_QUERY_ID_KEY,
} from '@myko/core'
import { ModuleRef } from '@nestjs/core'
import { LoggerService } from '@rship/logging'

@Injectable()
export class MykoQueryBus extends AMykoQueryBus {
  constructor(private moduleRef: ModuleRef, private logger: LoggerService) {
    super()
  }

  watch<T extends MQuery>(query: T): MLiveQueryResult<T> {
    const queryId = Reflect.getMetadata(MYKO_QUERY_ID_KEY, query)
    const handler = this.handlers.get(queryId)

    const err = `Handler not Provided for ${query.constructor.name} [${queryId}]. Check your module's providers array, and that the command is decorated with @MykoQuery(id: string)`

    if (!handler) {
      this.logger
        .getLogger('MykoQueryBus')
        .dev.error({ message: err, data: query })
      return
    }

    return handler.execute(query) as MLiveQueryResult<T>
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
