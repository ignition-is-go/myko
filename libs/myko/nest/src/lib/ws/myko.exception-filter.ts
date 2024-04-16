import { ArgumentsHost, Catch, ExceptionFilter } from '@nestjs/common'
import { WsException } from '@nestjs/websockets'

import { MykoProtocol } from '@myko/core'
import { clientProtocols, encoders } from '../registry/client.protocols'

@Catch(WsException)
export class WsExceptionFilter implements ExceptionFilter {
  constructor() {}
  catch(exception: any, host: ArgumentsHost) {
    const client = host.switchToWs().getClient()

    const protocol = clientProtocols.get(client)

    const encoder = encoders.get(protocol ?? MykoProtocol.JSON)

    client.send(encoder(exception))
  }
}
