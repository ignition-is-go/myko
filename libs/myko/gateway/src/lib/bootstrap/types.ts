import type { PersisterFactory, PersisterOverrideData } from '@myko/core'
import type { MykoWsAdapter } from '../adapters'
import type { MykoAuthService } from '../auth/types'

export type MykoGatewayBootstrapOptions = {
  defaultPersister: PersisterFactory
  persisterOverrides?: PersisterOverrideData[]
  groupId: string
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
