import { MykoCommand } from '../decorators'
import { MCommand, type ID } from '../types'

@MykoCommand()
export class ReballanceItem extends MCommand {
  constructor(
    readonly entityType: string,
    readonly id: ID,
  ) {
    super()
  }
}
