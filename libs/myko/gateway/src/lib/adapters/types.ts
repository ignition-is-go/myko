import type { ID } from '@myko/core'
import type { WSMMessage } from '@myko/ws'
import type { Subject } from 'rxjs'

export type MykoTlsOptions = {
  key: string // Path to private key file
  cert: string // Path to certificate file
  ca?: string // Optional path to CA certificate file
  passphrase?: string // Optional passphrase for encrypted key
}

export type MykoWsAdapterOptions = {
  port: number
  tx: Subject<{ clientId: ID; data: WSMMessage }>
  rx: Subject<{ clientId: ID; data: WSMMessage }>
  serverId: ID
  tls?: MykoTlsOptions
}

export type MykoWsAdapterResult = {
  clientHealthCheck: (id: ID) => boolean
}

export type MykoWsAdapter = (args: MykoWsAdapterOptions) => MykoWsAdapterResult
