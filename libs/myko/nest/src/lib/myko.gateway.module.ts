import { Inject, Module, OnModuleInit, Optional } from '@nestjs/common'
import { MykoModule } from './myko.module'
import { MykoGateway } from './ws/myko.gateway'
import { LoggerModule, LoggerService } from '@rship/logging'
import { MykoEventBus } from './busses'
import {
  Client,
  ClientRepo,
  Server,
  ServerRepo,
  makeSet,
  ofItems,
  onInit,
  watchInit,
} from '@myko/core'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { KafkaPersisterFactory } from './persisters'
import * as handlers from './myko.gateway.handlers'
import { SERVER_TOKEN } from '../types'
import { peerRegistry } from './registry/peer.registry'
import { MykoAuthService } from './services'
import { v4 as uuid } from 'uuid'
import { map } from 'rxjs'
import { groupBy } from 'ramda'
import { DateTime } from 'luxon'

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

    const token = await this.auth.getPeerToken()

    const selfKey = `${this.server.address}:${this.server.port}`

    onInit(['Server'], () => {
      this.servers
        .watchFilter((s) => {
          const sKey = `${s.address}:${s.port}`
          return (
            sKey !== selfKey &&
            s.version === this.server.version &&
            this.server.groupId === s.groupId
          )
        })

        .pipe(
          map((servers) => {
            const groups = groupBy((s) => `${s.address}:${s.port}`, servers)

            return Object.values(groups).map((servers) => {
              const server = servers.reduce((a, b) => {
                return a.startedAt > b.startedAt ? a : b
              })

              return server
            })
          }),
        )
        .subscribe((servers) => {
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
                  const clients = this.clients.get({
                    serverId: server.id,
                  })

                  this.events.publishAll(
                    clients.map((client) =>
                      makeSet(
                        new Client({ ...client, connected: false }),
                        'peer-offline',
                      ),
                    ),
                  )

                  this.logger
                    .getLogger('OnInit')
                    .dev.info(`Disconnected from ${server.id}`)
                },
              })
            })
        })
    })
  }
}
