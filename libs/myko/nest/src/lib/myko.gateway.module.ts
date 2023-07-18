import { Inject, Module, OnModuleInit, Optional } from '@nestjs/common'
import { MykoModule } from './myko.module'
import { MykoGateway } from './ws/myko.gateway'
import { LoggerModule, LoggerService } from '@rship/logging'
import { MykoEventBus } from './busses'
import { Client, ClientRepo, Server, ServerRepo, ofItems } from '@myko/core'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { RedisPersisterFactory } from './redis'
import * as handlers from './myko.gateway.handlers'
import { SERVER_TOKEN } from '../types'
import { peerRegistry } from './registry/peer.registry'
import { MykoAuthService } from './services'

@Module({
  imports: [
    MykoModule,
    ConfigModule,
    LoggerModule.forModule({ moduleName: 'MykoGateway' }),
  ],
  providers: [
    MykoGateway,
    {
      provide: ClientRepo,

      useFactory: (events: MykoEventBus, persisters: RedisPersisterFactory) => {
        const p = persisters.getPersister<Client>(Client)

        events.subject$.pipe(ofItems(Client)).subscribe((e) => p.persist(e))
        return new ClientRepo(Client, {
          stream: p.output.pipe(),
        })
      },
      inject: [MykoEventBus, RedisPersisterFactory],
    },
    {
      provide: ServerRepo,
      useFactory: (events: MykoEventBus, persisters: RedisPersisterFactory) => {
        const p = persisters.getPersister<Server>(Server)

        events.subject$.pipe(ofItems(Server)).subscribe((e) => p.persist(e))

        return new ServerRepo(Server, {
          stream: p.output.pipe(),
        })
      },
      inject: [MykoEventBus, RedisPersisterFactory],
    },
    {
      provide: SERVER_TOKEN,
      useFactory: (config: ConfigService) => {
        const address = config.get('HOST_ADDRESS')
        const port = Number(config.get('MYKO_PORT'))
        const version = config.get('VERSION')

        if (!address) {
          throw new Error('HOST_ADDRESS is required')
        }

        if (!port) {
          throw new Error('MYKO_PORT is required')
        }

        if (!version) {
          throw new Error('VERSION is required')
        }

        const id = `${address}:${port}:${version}`

        return new Server({
          address,
          port,
          version,
          id,
        })
      },
      inject: [ConfigService],
    },
    ...Object.values(handlers),
  ],
})
export class MykoGatewayModule implements OnModuleInit {
  constructor(
    private events: MykoEventBus,
    @Inject(SERVER_TOKEN) private server: Server,
    private servers: ServerRepo,
    @Optional() @Inject(MykoAuthService) private auth: MykoAuthService,
    private logger: LoggerService,
  ) {}

  async onModuleInit() {
    this.events.setServerId(this.server.id)
    this.events.publishSet(this.server, 'server:init')

    const token = await this.auth.getPeerToken()

    this.servers.watch({}).subscribe((servers) => {
      servers
        .filter((x) => x.id !== this.server.id)
        .forEach((server) => {
          peerRegistry.assertPeer(server, this.server, token, {
            onConnect: () => {
              this.logger
                .getLogger('OnInit')
                .dev.info(`Connected to ${server.id}`)
            },
            onDisconnect: () => {
              this.logger
                .getLogger('OnInit')
                .dev.info(`Disconnected from ${server.id}`)
            },
          })
        })
    })
  }
}
