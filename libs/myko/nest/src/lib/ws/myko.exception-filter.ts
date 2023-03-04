import { ArgumentsHost, Catch, ExceptionFilter } from '@nestjs/common'
import { WsException } from '@nestjs/websockets'

@Catch(WsException)
export class WsExceptionFilter implements ExceptionFilter {
  catch(exception: any, host: ArgumentsHost) {
    host.switchToWs().getClient().send(JSON.stringify(exception))
  }
}
