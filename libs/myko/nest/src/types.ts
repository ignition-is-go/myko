import { Client, ID } from '@myko/core'
import { WSMCommandError } from '@myko/ws'
import { Injectable } from '@nestjs/common'
import { WsException } from '@nestjs/websockets'
import * as WebSocket from 'ws'
import { MykoEventBus } from './lib/busses'

@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> {
  constructor(private events: MykoEventBus) {
    super()
  }

  register(id: ID, socket: WebSocket) {
    if (this.has(id) && this.get(id) !== socket) {
      throw new Error('Socket Collision')
    }

    if (this.has(id) && this.get(id) === socket) {
      return
    }
    socket.on('close', () => {
      this.events.publishSet(
        new Client({ connected: false, id }),
        'client-disconnected',
      )
      this.delete(id)
    })
    this.events.publishSet(
      new Client({ id, connected: true }),
      'client-connected',
    )
    this.set(id, socket)
  }
}

export class CommandNotAuthorized
  extends WsException
  implements WSMCommandError
{
  constructor(readonly tx: ID) {
    super('Command Not Authorized')
    this.event = 'ws:m:command-error'
  }
  event: 'ws:m:command-error'
}

export class MykoCommandError extends WsException implements WSMCommandError {
  constructor(readonly tx: ID, error: string) {
    super(error)
    this.event = 'ws:m:command-error'
  }
  event: 'ws:m:command-error'
}
