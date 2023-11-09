import { Module, OnModuleInit, forwardRef } from '@nestjs/common'
import { MykoCommandBus } from './busses/command.bus'
import { ExplorerService } from './services'
import { LoggerModule, LoggerService } from '@rship/logging'
import { MykoQueryBus } from './busses'
import { MykoEventBus } from './busses/event.bus'
import { RedisPersisterFactory } from './persisters/redis.persisterFactory'
import { bufferTime, filter, groupBy, mergeMap } from 'rxjs'
import { SocketRegistry } from './registry/socket.registry'
import { ConfigModule } from '@nestjs/config'
import { KafkaPersisterFactory } from './persisters'
import { MykoGatewayModule } from './myko.gateway.module'
import { PeerRegistry } from './registry/peer.registry'
import { MykoReportBus } from './busses/report.bus'

@Module({
  imports: [
    LoggerModule.forModule({ moduleName: 'Myko' }),
    ConfigModule,
    forwardRef(() => MykoGatewayModule),
  ],
  providers: [
    SocketRegistry,
    PeerRegistry,
    ExplorerService,
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    MykoReportBus,
    RedisPersisterFactory,
    KafkaPersisterFactory,
  ],
  exports: [
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    MykoReportBus,
    RedisPersisterFactory,
    KafkaPersisterFactory,
    SocketRegistry,
    PeerRegistry,
  ],
})
export class MykoModule implements OnModuleInit {
  constructor(
    private explorer: ExplorerService,
    private readonly commandBus: MykoCommandBus,
    private readonly queryBus: MykoQueryBus,
    private readonly eventBus: MykoEventBus,
    private readonly reportBus: MykoReportBus,
    private logger: LoggerService,
  ) {}

  onModuleInit() {
    const { commands, queries, sagas, reports } = this.explorer.explore()
    this.commandBus.register(commands)
    this.queryBus.register(queries)
    this.reportBus.register(reports)
    this.eventBus.registerSagas(sagas)

    if (process.env.LOG_LEVEL?.toLocaleLowerCase() === 'debug') {
      this.eventBus.subject$
        .pipe(
          groupBy((e) => `${e.itemType}:${e.changeType}`),
          mergeMap((obss) =>
            obss.pipe(
              bufferTime(1000),
              filter((x) => x.length > 0),
            ),
          ),
        )
        .subscribe((e) => {
          this.logger
            .getLogger(e[0].itemType)
            .dev.debug(`${e[0].changeType} [${e.length}]`)
        })
    }
  }
}
