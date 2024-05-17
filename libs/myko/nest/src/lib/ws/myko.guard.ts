import {
  CommandUnwrapError,
  MWrappedCommand,
  isAllInit,
  unwrapCommand,
} from '@myko/core'
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
  authorizedClients = new Set<any>()

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
    if (!isAllInit().done) {
      this.logger
        .getLogger('MykoGuard')
        .dev.warn('Connection Refused: Server not initialized')
      const client = context.switchToWs().getClient()
      client.close(1002, 'Server not initialized')
      return false
    }

    const data = context.switchToWs().getData()
    const client = context.switchToWs().getClient()

    if ((data as MWrappedCommand).commandId === undefined) {
      return true
    }

    try {
      const c = unwrapCommand(data)

      if (this.authorizedClients.has(client)) {
        return true
      }

      const token = c.userToken
      const canActivate = await this.auth.canActivate(token)

      if (!canActivate) {
        throw new CommandNotAuthorized(c.tx)
      }

      this.authorizedClients.add(client)
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
