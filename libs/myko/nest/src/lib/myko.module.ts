import { Module, OnModuleInit } from '@nestjs/common'
import { MykoCommandBus } from './busses/command.bus'
import { ExplorerService } from './services'
import { LoggerModule, LoggerService } from '@rship/logging'
import { MykoQueryBus } from './busses'
import { MykoEventBus } from './busses/event.bus'
import { RedisPersisterFactory } from './redis/redis.persisterFactory'
import * as handlers from './myko.handlers'
import { Client, ClientRepo, ofItems } from '@myko/core'
import { bufferTime, filter, groupBy, mergeMap } from 'rxjs'
import { SocketRegistry } from './registry/socket.registry'

@Module({
  imports: [LoggerModule.forModule({ moduleName: 'Myko' })],
  providers: [
    SocketRegistry,
    ExplorerService,
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    RedisPersisterFactory,
    {
      provide: ClientRepo,

      useFactory: (events: MykoEventBus) => {
        return new ClientRepo(Client, {
          stream: events.subject$.pipe(ofItems(Client)),
        })
      },
      inject: [MykoEventBus],
    },

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

    if (process.env.LOG_LEVEL?.toLocaleLowerCase() === 'debug') {
      this.eventBus.subject$
        .pipe(
          groupBy((e) => e.itemType),
          mergeMap((obss) =>
            obss.pipe(
              bufferTime(1000),
              filter((x) => x.length > 0),
            ),
          ),
        )
        .subscribe((e) => {
          log.dev.debug(`${e[0].itemType}:${e[0].changeType} [${e.length}]`)
        })
    }
  }
}
