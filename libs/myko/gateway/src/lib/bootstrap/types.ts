import type { PersisterFactory } from '@myko/core'
import type { MykoWsAdapter } from '../adapters'
import type { MykoAuthService } from '../auth/types'

export type MykoGatewayBootstrapOptions = {
  defaultPersister: PersisterFactory
  groupId: string
  version: string
  ws?: {
    host: string
    port: number
    wsAdapter: MykoWsAdapter
    authService?: MykoAuthService
  }
  modules: any[]
}
