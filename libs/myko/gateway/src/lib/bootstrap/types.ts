import type {
  HistoryProvider,
  PersisterFactory,
  PersisterOverrideData,
  RepoFactory,
  RepoOverrideData,
} from '@myko/core'
import type { MykoWsAdapter } from '../adapters'
import type { MykoAuthService } from '../auth/types'

export type MykoGatewayBootstrapOptions = {
  defaultPersister: PersisterFactory
  persisterOverrides?: PersisterOverrideData[]
  defaultRepo: RepoFactory
  repoOverrides?: RepoOverrideData[]
  historyProvider?: HistoryProvider
  version: string
  ws?: {
    host: string
    port: number
    wsAdapter: MykoWsAdapter
    authService?: MykoAuthService
  }
  modules: any[]
  docPath?: string
  tracing?:
    | {
        enabled: true
        serviceName: string
        serviceVersion: string
        endpoint: string
      }
    | {
        enabled: false
      }
}

export type StartupReportEntry = {
  module: string
  lines: string[]
}
