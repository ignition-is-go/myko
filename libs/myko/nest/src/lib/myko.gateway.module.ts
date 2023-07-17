import { DynamicModule, Inject, Module, OnModuleInit } from '@nestjs/common'
import { MykoModule } from './myko.module'
import { MykoGateway } from './ws/myko.gateway'
import { LoggerModule } from '@rship/logging'
import { MykoEventBus, MykoQueryBus } from './busses'
import { Server, ServerRepo, ofItems } from '@myko/core'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { RedisPersisterFactory } from './redis'
import * as handlers from './myko.gateway.handlers'
import { SERVER_TOKEN } from '../types'

@Module({
  imports: [
    MykoModule,
    ConfigModule,
    LoggerModule.forModule({ moduleName: 'MykoGateway' }),
  ],
  providers: [
    MykoGateway,
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
    private query: MykoQueryBus,
    private config: ConfigService,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}

  async onModuleInit() {
    this.events.publishSet(this.server, 'server:init')
  }
}
