import { ID, Client } from '@myko/core'
import { Injectable } from '@nestjs/common'
import { MykoEventBus, MykoQueryBus } from '../busses'
import type { WebSocket } from 'ws'

@Injectable()
export class SocketRegistry extends Map<ID, Set<WebSocket>> {
  constructor(private events: MykoEventBus) {
    super()
  }

  register(id: ID, socket: WebSocket, serverId: ID) {
    if (!this.has(id)) {
      this.set(id, new Set())
    }

    if (this.has(id) && this.get(id).has(socket)) {
      return
    }
    socket.on('close', () => {
      this.events.publishSet(
        new Client({ connected: false, id, serverId }),
        'client-disconnected',
      )
      this.get(id).delete(socket)
    })
    this.events.publishSet(
      new Client({ id, connected: true, serverId }),
      'client-connected',
    )
    this.get(id).add(socket)
    console.log('registered', id, this.get(id).size)
  }
}
