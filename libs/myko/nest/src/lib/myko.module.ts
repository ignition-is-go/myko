import { Module, OnModuleInit, forwardRef } from '@nestjs/common'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { bufferTime, filter, groupBy, mergeMap } from 'rxjs'
import { MykoQueryBus, PeerEventBus } from './busses'
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
  imports: [ConfigModule, forwardRef(() => MykoGatewayModule)],
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
    PeerEventBus,
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
    PeerEventBus,
  ],
})
export class MykoModule implements OnModuleInit {
  constructor(
    private explorer: ExplorerService,
    private readonly commandBus: MykoCommandBus,
    private readonly queryBus: MykoQueryBus,
    private readonly eventBus: MykoEventBus,
    private readonly reportBus: MykoReportBus,
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
          console.log(e[0].itemType, e[0].changeType, e.length)
        })
    }
  }
}
