import {
  Inject,
  Module,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
  forwardRef,
} from '@nestjs/common'
import { MykoModule } from './myko.module'
import { MykoGateway } from './ws/myko.gateway'
import { LoggerModule, LoggerService } from '@rship/logging'
import { MykoEventBus } from './busses'
import {
  Client,
  ClientRepo,
  Server,
  ServerRepo,
  makeDel,
  ofItems,
  watchInit,
} from '@myko/core'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { KafkaPersisterFactory } from './persisters'
import * as handlers from './myko.gateway.handlers'
import { SERVER_TOKEN } from '../types'
import { MykoAuthService } from './services'
import { v4 as uuid } from 'uuid'
import { DateTime } from 'luxon'

@Module({
  imports: [
    forwardRef(() => MykoModule),
    ConfigModule,
    LoggerModule.forModule({ moduleName: 'MykoGateway' }),
  ],
  providers: [
    MykoGateway,
    {
      provide: ClientRepo,

      useFactory: (events: MykoEventBus, persisters: KafkaPersisterFactory) => {
        const p = persisters.getPersister<Client>(Client)

        events.subject$.pipe(ofItems(Client)).subscribe((e) => p.persist(e))
        return new ClientRepo(Client, {
          stream: p.output.pipe(),
        })
      },
      inject: [MykoEventBus, KafkaPersisterFactory],
    },
    {
      provide: ServerRepo,
      useFactory: (events: MykoEventBus, persisters: KafkaPersisterFactory) => {
        const p = persisters.getPersister<Server>(Server)

        events.subject$.pipe(ofItems(Server)).subscribe((e) => p.persist(e))

        return new ServerRepo(Server, {
          stream: p.output.pipe(),
        })
      },
      inject: [MykoEventBus, KafkaPersisterFactory],
    },
    {
      provide: SERVER_TOKEN,
      useFactory: (config: ConfigService) => {
        const address = config.get('HOST_ADDRESS')
        const port = Number(config.get('MYKO_EXTERNAL_PORT'))
        const version = config.get('VERSION')
        const groupId = config.get('MYKO_GROUP')

        if (!address) {
          throw new Error('HOST_ADDRESS is required')
        }

        if (!port) {
          throw new Error('MYKO_EXTERNAL_PORT is required')
        }

        if (!version) {
          throw new Error('VERSION is required')
        }

        if (!groupId) {
          throw new Error('MYKO_GROUP is required')
        }

        return new Server({
          id: uuid(),
          address,
          port,
          groupId: groupId,
          version,
          startedAt: DateTime.utc().toISO(),
        })
      },
      inject: [ConfigService],
    },
    ...Object.values(handlers),
  ],
  exports: [SERVER_TOKEN, ServerRepo, ClientRepo],
})
export class MykoGatewayModule implements OnModuleInit {
  constructor(
    private events: MykoEventBus,
    @Inject(SERVER_TOKEN) private server: Server,
    private servers: ServerRepo,
    @Optional() @Inject(MykoAuthService) private auth: MykoAuthService,
    private logger: LoggerService,
    private clients: ClientRepo,
  ) {}

  async onModuleInit() {
    watchInit((entity, registered, inited) => {
      this.logger.getLogger(entity).dev.info(`Init: ${inited}/${registered}`)
    })

    this.events.setServerId(this.server.id)
    this.events.publishSet(this.server, 'server:init')

    this.servers
      .watchFilter(
        (that) =>
          that.address === this.server.address &&
          that.port === this.server.port &&
          that.startedAt < this.server.startedAt,
      )
      .subscribe((s) => {
        this.events.publishAll(s.map((y) => makeDel(y, 'server:delete')))
      })
  }
}
