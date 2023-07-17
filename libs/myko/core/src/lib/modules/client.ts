import { Repo } from '../aggregates'
import { MykoItem, MykoQuery, belongsTo } from '../decorators'
import { MItem } from '../types/item'
import { MQuery } from '../types/query'
import { Server } from './server'

@MykoItem('Client')
export class Client extends MItem<Client> {
  readonly connected: boolean

  @belongsTo(Server)
  readonly serverId: string
}

@MykoQuery('clients:getByIds', Client)
export class GetClientsByIds extends MQuery<Client> {
  constructor(public ids: string[]) {
    super()
  }
}

export class ClientRepo extends Repo<Client> {}
