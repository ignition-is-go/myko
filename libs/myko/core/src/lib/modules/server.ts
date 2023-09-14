import { Repo } from '../aggregates'
import { MykoItem, MykoQuery } from '../decorators'
import { MItem, MQuery } from '../types'

@MykoItem('Server')
export class Server extends MItem<Server> {
  version: string
  address: string
  port: number
  startedAt: string // ISO DateTime
}

@MykoQuery('servers:getConnectedServer', Server)
export class GetConnectedServer extends MQuery<Server> {
  constructor() {
    super()
  }
}

@MykoQuery('servers:getPeerServers', Server)
export class GetPeerServers extends MQuery<Server> {
  constructor() {
    super()
  }
}

@MykoQuery('servers:getByClientIds', Server)
export class GetServersByClientIds extends MQuery<Server> {
  constructor(public clientIds: string[]) {
    super()
  }
}

@MykoQuery('servers:getByQuery', Server)
export class GetServersByQuery extends MQuery<Server> {
  constructor(public query: Partial<Server>) {
    super()
  }
}

@MykoQuery('servers:get', Server)
export class GetServers extends MQuery<Server> {}

export class ServerRepo extends Repo<Server> {}
