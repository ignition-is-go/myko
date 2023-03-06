import { Module, OnModuleInit } from '@nestjs/common'
import { MykoCommandBus } from './busses/command.bus'
import { ExplorerService } from './services'
import { LoggerModule } from '@rship/logging'
import { MykoQueryBus } from './busses'
import { MykoEventBus } from './busses/event.bus'
import { MykoGateway } from './ws/myko.gateway'
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
    MykoGateway,
    RedisPersisterFactory,
    ...Object.values(handlers),
  ],
  exports: [MykoCommandBus, MykoQueryBus, MykoEventBus, RedisPersisterFactory],
})
export class MykoModule implements OnModuleInit {
  constructor(
    private explorer: ExplorerService,
    private readonly commandBus: MykoCommandBus,
    private readonly queryBus: MykoQueryBus,
    private readonly eventBus: MykoEventBus,
  ) {}

  onModuleInit() {
    const { commands, queries, sagas } = this.explorer.explore()
    this.commandBus.register(commands)
    this.queryBus.register(queries)
    this.eventBus.registerSagas(sagas)
  }
}
