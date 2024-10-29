import { MykoItem, MykoQuery, MykoReport, doc } from '../decorators'
import {
  MItem,
  MQuery,
  MReport,
  getItemName,
  type ID,
  type MEvent,
  type MItemConstructor,
} from '../types'

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

@MykoReport()
export class ServerEventLog extends MReport<MEvent> {
  constructor() {
    super()
  }
}

@MykoQuery(Server)
export class GetConnectedServer extends MQuery<Server> {
  constructor() {
    super()
  }
}

@MykoQuery(Server)
export class GetPeerServers extends MQuery<Server> {
  constructor() {
    super()
  }
}

@MykoQuery(Server)
export class GetServersByClientIds extends MQuery<Server> {
  constructor(public clientIds: string[]) {
    super()
  }
}

@MykoQuery(Server)
export class GetServersByQuery extends MQuery<Server> {
  constructor(public query: Partial<Server>) {
    super()
  }
}

@MykoQuery(Server)
export class GetServers extends MQuery<Server> {}

// @MykoReport()
// export class GroupLeader extends MReport<Server> {
//   constructor(readonly groupId: ID) {
//     super()
//   }
// }

// @MykoReport()
// export class IsLeader extends MReport<boolean> {
//   constructor(readonly serverId: ID) {
//     super()
//   }
// }

// @MykoReport()
// export class ConnectedToLeader extends MReport<boolean> {
//   constructor() {
//     super()
//   }
// }

@MykoReport()
export class PeerAlive extends MReport<number | false> {
  constructor(readonly peerId: ID) {
    super()
  }
}

@MykoReport()
export class PeerLastSeen extends MReport<string> {
  constructor(readonly peerId: ID) {
    super()
  }
}

@MykoReport()
export class EntitySearch<T extends MItem> extends MReport<T[]> {
  readonly entityType: string
  constructor(
    readonly query: string,
    item: MItemConstructor<T>,
    readonly opts?: {
      showAllOnEmpty?: boolean
    },
  ) {
    super()
    this.entityType = getItemName(item)
  }
}
