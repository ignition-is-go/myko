import type { ID } from './base'

export const MYKO_ITEM_TYPE = '__MYKO_ITEM_TYPE__'

export type IMykoItem = {
  id: ID
  name?: string
  scopeId: ID | null
  hash: string
}
