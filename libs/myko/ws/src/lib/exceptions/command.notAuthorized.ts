import { ID } from '@myko/core'
import { WSMCommandError } from '../types'
import { MykoCommandError } from './command.error'

export class CommandNotAuthorized
  extends MykoCommandError
  implements WSMCommandError
{
  constructor(readonly tx: ID) {
    super(tx, 'Command Not Authorized')
    this.event = 'ws:m:command-error'
  }
  message: string
  event: 'ws:m:command-error'
}
