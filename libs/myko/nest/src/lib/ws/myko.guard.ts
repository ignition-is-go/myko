import { CommandUnwrapError, unwrapCommand } from '@myko/core'
import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  OnModuleInit,
  Optional,
} from '@nestjs/common'
import { LoggerService } from '@rship/logging'
import { CommandNotAuthorized } from '../../types'
import { MykoAuthService } from '../services'

@Injectable()
export class MykoGuard implements CanActivate, OnModuleInit {
  constructor(
    private logger: LoggerService,
    @Optional() @Inject(MykoAuthService) private auth: MykoAuthService,
  ) {}

  onModuleInit() {
    if (typeof this.auth !== 'object') {
      this.logger
        .getLogger('MykoGateway')
        .dev.warn(
          'NOT SECURED!. Please provide an implementation of MykoAuthService globally in the app to handle authentication',
        )
    }
  }

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const data = context.switchToWs().getData()

    try {
      const c = unwrapCommand(data)

      const token = c.userToken
      const canActivate = await this.auth.canActivate(token)
      if (!canActivate) {
        throw new CommandNotAuthorized(c.tx)
      }
    } catch (e) {
      if (e instanceof CommandNotAuthorized) {
        throw e
      }
      if (e instanceof CommandUnwrapError) {
        return true
      }
    }
    return true
  }
}
