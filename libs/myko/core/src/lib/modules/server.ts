import { Repo } from '../aggregates'
import { MykoItem, MykoQuery, MykoReport, doc } from '../decorators'
import { ID, MEvent, MItem, MQuery, MReport } from '../types'

@MykoItem({
  doc: 'A Myko Server ',
})
export class Server extends MItem<Server> {
  @doc()
  version: string
  @doc('xxx.xxx.xxx.xxx, where it can be reached publically')
  address: string
  @doc('The port the server is listening on')
  port: number
  @doc('ISO DateTime string')
  startedAt: string // ISO DateTime
  @doc("The server's group id")
  groupId: string
}

@MykoReport('server:eventlog')
export class ServerEventLog extends MReport<MEvent> {
  constructor(readonly serverId: ID) {
    super()
  }
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

@MykoReport('servers:groupLeader')
export class GroupLeader extends MReport<Server> {
  constructor(readonly groupId: ID) {
    super()
  }
}

@MykoReport('server:isLeader')
export class IsLeader extends MReport<boolean> {
  constructor(readonly serverId: ID) {
    super()
  }
}

@MykoReport('server:connectedToLeader')
export class ConnectedToLeader extends MReport<boolean> {
  constructor() {
    super()
  }
}

@MykoReport('peer:alive')
export class PeerAlive extends MReport<number | false> {
  constructor(readonly peerId: ID) {
    super()
  }
}

@MykoReport('peer:last-seen')
export class PeerLastSeen extends MReport<string> {
  constructor(readonly peerId: ID) {
    super()
  }
}

export class ServerRepo extends Repo<Server> {}
