import { Module, OnModuleInit } from '@nestjs/common'
import { MykoCommandBus } from './busses/command.bus'
import { ExplorerService } from './services'
import { LoggerModule } from '@rship/logging'
import { MykoQueryBus } from './busses'
import { MykoEventBus } from './busses/event.bus'
import { MykoGateway } from './ws/myko.gateway'

@Module({
  imports: [LoggerModule.forModule({ moduleName: 'Myko' })],
  providers: [
    ExplorerService,
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    MykoGateway,
  ],
  exports: [MykoCommandBus, MykoQueryBus, MykoEventBus],
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
