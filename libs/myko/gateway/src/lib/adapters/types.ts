import type { ID } from '@myko/core'
import type { WSMMessage } from '@myko/ws'
import type { ReplaySubject, Subject } from 'rxjs'

export type MykoWsAdapterOptions = {
  port: number
  tx: Subject<{ clientId: ID; data: WSMMessage }>
  rx: Subject<{ clientId: ID; data: WSMMessage }>
  clients: ReplaySubject<ID[]>
  serverId: ID
}

export type MykoWsAdapter = (args: MykoWsAdapterOptions) => void
