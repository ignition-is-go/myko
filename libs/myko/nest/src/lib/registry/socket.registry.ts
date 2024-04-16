import { Client, ID, Server, SetClientId, isAllInit } from '@myko/core'
import { wrapCommandWS } from '@myko/ws'
import { Inject, Injectable } from '@nestjs/common'
import { LoggerService } from '@rship/logging'
import { v4 as uuid } from 'uuid'
import type { WebSocket } from 'ws'
import { SERVER_TOKEN } from '../../types'
import { MykoCommandBus, MykoEventBus, MykoQueryBus } from '../busses'
@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> {
  constructor(
    private events: MykoEventBus,
    private query: MykoQueryBus,
    private logger: LoggerService,
    private command: MykoCommandBus,
    @Inject(SERVER_TOKEN) private me: Server,
  ) {
    super()
  }

  register(socket: WebSocket) {
    if (!isAllInit().done) {
      throw new Error('Server not initialized')
    }

    const id = uuid()

    const serverId = this.me.id

    socket.on('close', () => {
      this.events.publishDel(
        new Client({ id, serverId }),
        'client-disconnected',
      )
      this.delete(id)
    })
    const c = new Client({ id, serverId })
    this.logger.getLogger('SocketRegistry').dev.info(`Client Connected`)
    this.events.publishSet(c, 'client-connected')

    const ws = wrapCommandWS(new SetClientId(id))
    socket.send(JSON.stringify(ws))
    this.set(id, socket)
  }
}
