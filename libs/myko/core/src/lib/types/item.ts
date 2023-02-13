import { ID } from './base'

export type IMykoItem = {
  id: ID
  name?: string
  scopeId: ID | null
  hash: string
}
