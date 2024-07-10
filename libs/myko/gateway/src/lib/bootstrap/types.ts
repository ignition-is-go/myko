import type { PersisterFactory } from '@myko/core'
import type { MykoWsAdapter } from '../adapters'
import type { MykoAuthService } from '../auth/types'

export type MykoGatewayBootstrapOptions = {
  port: number
  wsAdapter: MykoWsAdapter
  defaultPersister: PersisterFactory
  authService?: MykoAuthService
  address: string
  groupId: string
  version: string
}
