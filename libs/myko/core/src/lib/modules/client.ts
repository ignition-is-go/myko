import { Repo } from '../aggregates'
import { MykoCommand, MykoItem, MykoQuery, belongsTo } from '../decorators'
import { MCommand } from '../types'
import { MItem } from '../types/item'
import { MQuery } from '../types/query'
import { Server } from './server'

@MykoItem('Client')
export class Client extends MItem<Client> {
  @belongsTo(Server)
  readonly serverId: string
}

@MykoQuery('clients:getByIds', Client)
export class GetClientsByIds extends MQuery<Client> {
  constructor(public ids: string[]) {
    super()
  }
}

@MykoQuery('clients:getByQuery', Client)
export class GetClientsByQuery extends MQuery<Client> {
  constructor(public partial: Partial<Client>) {
    super()
  }
}

@MykoCommand('clients:deleteByServerId')
export class DeleteClientsByServerId extends MCommand {
  constructor(public serverId: string) {
    super()
  }
}

export class ClientRepo extends Repo<Client> {}
