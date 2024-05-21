import { Module, OnModuleInit, forwardRef } from '@nestjs/common'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { LoggerModule, LoggerService } from '@rship/logging'
import { bufferTime, filter, groupBy, mergeMap } from 'rxjs'
import { MykoQueryBus } from './busses'
import { MykoCommandBus } from './busses/command.bus'
import { MykoEventBus } from './busses/event.bus'
import { MykoReportBus } from './busses/report.bus'
import { MykoDocsService } from './myko.docs.service'
import { MykoGatewayModule } from './myko.gateway.module'
import { KafkaPersisterFactory } from './persisters'
import { RedisPersisterFactory } from './persisters/redis.persisterFactory'
import { PeerClientRegistry } from './registry/peer.registry'
import { SocketRegistry } from './registry/socket.registry'
import { ExplorerService } from './services'

@Module({
  imports: [
    LoggerModule.forModule({ moduleName: 'Myko' }),
    ConfigModule,
    forwardRef(() => MykoGatewayModule),
  ],
  providers: [
    SocketRegistry,
    PeerClientRegistry,
    ExplorerService,
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    MykoReportBus,
    RedisPersisterFactory,
    KafkaPersisterFactory,
    MykoDocsService,
  ],
  exports: [
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    MykoReportBus,
    RedisPersisterFactory,
    KafkaPersisterFactory,
    SocketRegistry,
    PeerClientRegistry,
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
    private config: ConfigService,
    private docs: MykoDocsService,
  ) {}

  onModuleInit() {
    const doc = this.config.get('MYKO_DOC')

    if (doc) {
      this.docs.writeDocs('.')
    }

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
