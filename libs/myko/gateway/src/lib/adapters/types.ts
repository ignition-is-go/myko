import type { ID } from '@myko/core'
import type { WSMMessage } from '@myko/ws'
import type { Subject } from 'rxjs'

export type MykoWsAdapterOptions = {
  port: number
  tx: Subject<{ clientId: ID; data: WSMMessage }>
  rx: Subject<{ clientId: ID; data: WSMMessage }>
  serverId: ID
}

export type MykoWsAdapterResult = {
  clientHealthCheck: (id: ID) => boolean
}

export type MykoWsAdapter = (args: MykoWsAdapterOptions) => MykoWsAdapterResult
