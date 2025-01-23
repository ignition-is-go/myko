import type {
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
  version: string
  ws?: {
    host: string
    port: number
    wsAdapter: MykoWsAdapter
    authService?: MykoAuthService
  }
  modules: any[]
  docPath?: string
}
