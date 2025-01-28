import { MykoItem, MykoQuery, MykoReport, doc } from '../decorators'
import {
  MItem,
  MQuery,
  MReport,
  getItemName,
  type DeepPartial,
  type ID,
  type MEvent,
  type MItemConstructor,
} from '../types'

import Z from 'zod'

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
}

export const serverSchema = Z.object({
  id: Z.string().uuid(),
  hash: Z.string(),
  version: Z.string(),
  address: Z.string(),
  port: Z.number().positive(),
  startedAt: Z.string().datetime(),
})

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
    readonly filter?: DeepPartial<T>,
  ) {
    super()
    this.entityType = getItemName(item)
  }
}
