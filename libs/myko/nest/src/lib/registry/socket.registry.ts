import { Client, ID, Server, SetClientId, isAllInit } from '@myko/core'
import { wrapCommandWS } from '@myko/ws'
import { Inject, Injectable } from '@nestjs/common'
import { v4 as uuid } from 'uuid'
import type { WebSocket } from 'ws'
import { SERVER_TOKEN } from '../../types'
import { MykoCommandBus, MykoEventBus, MykoQueryBus } from '../busses'
@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> {
  reverse: Map<WebSocket, ID> = new Map()

  constructor(
    private events: MykoEventBus,
    private query: MykoQueryBus,
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
      this.reverse.delete(socket)
    })
    const c = new Client({ id, serverId })
    console.log('Client Connected', c)
    this.events.publishSet(c, 'client-connected')

    const ws = wrapCommandWS(new SetClientId(id))
    socket.send(JSON.stringify(ws))
    this.set(id, socket)
    this.reverse.set(socket, id)
  }

  getClientIdFromSocket(socket: WebSocket): ID {
    const id = this.reverse.get(socket)
    if (!id) {
      console.error('No client found for socket')
    }

    return id
  }
}
