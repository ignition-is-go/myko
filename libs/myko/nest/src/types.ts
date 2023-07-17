import { ID } from '@myko/core'
import { WSMCommandError } from '@myko/ws'
import { WsException } from '@nestjs/websockets'

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

export const SERVER_TOKEN = 'SERVER_TOKEN'
