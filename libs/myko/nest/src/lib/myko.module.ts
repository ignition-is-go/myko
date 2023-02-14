import { Module, OnModuleInit } from '@nestjs/common'
import { MykoCommandBus } from './busses/command.bus'
import { ExplorerService } from './services'
import { LoggerModule } from '@rship/logging'
import { MykoQueryBus } from './busses'

@Module({
  imports: [LoggerModule.forModule({ moduleName: 'Myko' })],
  providers: [ExplorerService, MykoCommandBus, MykoQueryBus],
  exports: [MykoCommandBus, MykoQueryBus],
})
export class MykoModule implements OnModuleInit {
  constructor(
    private explorer: ExplorerService,
    private readonly commandBus: MykoCommandBus,
    private readonly queryBus: MykoQueryBus,
  ) {}

  onModuleInit() {
    const { commands, queries } = this.explorer.explore()
    this.commandBus.register(commands)
    this.queryBus.register(queries)
  }
}
