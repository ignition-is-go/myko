import { Module, OnModuleInit } from '@nestjs/common'
import { MykoCommandBus } from './busses/command.bus'
import { ExplorerService } from './services'
import { LoggerModule, LoggerService } from '@rship/logging'
import { MykoQueryBus } from './busses'
import { MykoEventBus } from './busses/event.bus'
import { RedisPersisterFactory } from './redis/redis.persisterFactory'
import { SocketRegistry } from '../types'
import * as handlers from './myko.handlers'

@Module({
  imports: [LoggerModule.forModule({ moduleName: 'Myko' })],
  providers: [
    SocketRegistry,
    ExplorerService,
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    RedisPersisterFactory,
    ...Object.values(handlers),
  ],
  exports: [
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    RedisPersisterFactory,
    SocketRegistry,
  ],
})
export class MykoModule implements OnModuleInit {
  constructor(
    private explorer: ExplorerService,
    private readonly commandBus: MykoCommandBus,
    private readonly queryBus: MykoQueryBus,
    private readonly eventBus: MykoEventBus,
    private logger: LoggerService,
  ) {}

  onModuleInit() {
    const { commands, queries, sagas } = this.explorer.explore()
    this.commandBus.register(commands)
    this.queryBus.register(queries)
    this.eventBus.registerSagas(sagas)
    const log = this.logger.getLogger('MykoModule')

    this.eventBus.subject$.subscribe((e) => {
      log.dev.debug(e.changeType, e.itemType)
    })
  }
}
