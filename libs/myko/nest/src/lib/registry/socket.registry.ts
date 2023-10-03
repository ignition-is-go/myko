import { ID, Client, DeleteClientsByServerId } from '@myko/core'
import { Injectable } from '@nestjs/common'
import { MykoCommandBus, MykoEventBus, MykoQueryBus } from '../busses'
import type { WebSocket } from 'ws'
import { LoggerService } from '@rship/logging'

@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> {
  constructor(
    private events: MykoEventBus,
    private query: MykoQueryBus,
    private logger: LoggerService,
    private command: MykoCommandBus,
  ) {
    super()
  }

  register(id: ID, socket: WebSocket, serverId: ID) {
    if (this.has(id) && socket === this.get(id)) {
      return
    }

    if (this.has(id)) {
      throw new Error('duplicate-connection')
    }
    socket.on('close', () => {
      this.events.publishDel(
        new Client({ id, serverId }),
        'client-disconnected',
      )
      this.delete(id)
      this.command.execute(new DeleteClientsByServerId(id))
    })
    const c = new Client({ id, serverId })
    this.logger.getLogger('SocketRegistry').dev.info(`Client Connected`)
    this.events.publishSet(c, 'client-connected')
    this.set(id, socket)
  }
}
