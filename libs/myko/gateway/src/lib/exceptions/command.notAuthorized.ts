import { MykoCommandError, type ID } from '@myko/core'

export class CommandNotAuthorized extends MykoCommandError {
  constructor(readonly tx: ID) {
    super(tx, 'Command Not Authorized')
  }
}
