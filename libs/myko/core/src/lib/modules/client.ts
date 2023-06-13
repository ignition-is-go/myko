import { Repo } from '../aggregates'
import { MykoItem, MykoQuery } from '../decorators'
import { MItem } from '../types/item'
import { MQuery } from '../types/query'

@MykoItem('Client')
export class Client extends MItem<Client> {
  readonly connected: boolean
}

@MykoQuery('clients:getByIds', Client)
export class GetClientsByIds extends MQuery<Client> {
  constructor(public ids: string[]) {
    super()
  }
}

export class ClientRepo extends Repo<Client> {}
