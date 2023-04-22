import { Module } from '@nestjs/common'
import { MykoModule } from './myko.module'
import { MykoGateway } from './ws/myko.gateway'
import { LoggerModule } from '@rship/logging'

@Module({
  imports: [MykoModule, LoggerModule.forModule({ moduleName: 'MykoGateway' })],
  providers: [MykoGateway],
})
export class MykoGatewayModule {}
