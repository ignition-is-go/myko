import { ID, Client, MEvent } from '@myko/core'
import { Injectable } from '@nestjs/common'
import { MykoEventBus, MykoQueryBus } from '../busses'
import type { WebSocket } from 'ws'
import { LoggerService } from '@rship/logging'

@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> {
  constructor(private events: MykoEventBus, private logger: LoggerService) {
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
      this.events.publishSet(
        new Client({ connected: false, id, serverId }),
        'client-disconnected',
      )
      this.delete(id)
    })
    this.events.publishSet(
      new Client({ id, connected: true, serverId }),
      'client-connected',
    )
    this.set(id, socket)
  }
}
