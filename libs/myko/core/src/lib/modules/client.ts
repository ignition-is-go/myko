import { MykoCommand, MykoItem, MykoQuery, belongsTo } from '../decorators'
import { MCommand } from '../types'
import { MItem } from '../types/item'
import { MQuery } from '../types/query'
import { Server } from './server'

@MykoItem({
  doc: 'A Myko Client connected to a Server',
})
export class Client extends MItem<Client> {
  @belongsTo(Server)
  readonly serverId: string
}

@MykoQuery(Client)
export class GetClientsByIds extends MQuery<Client> {
  constructor(public ids: string[]) {
    super()
  }
}

@MykoQuery(Client)
export class GetClientsByQuery extends MQuery<Client> {
  constructor(public partial: Partial<Client>) {
    super()
  }
}

@MykoCommand()
export class DeleteClientsByServerId extends MCommand {
  constructor(public serverId: string) {
    super()
  }
}
