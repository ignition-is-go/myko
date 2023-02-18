import type { ID } from './base'

export const MYKO_ITEM_TYPE = '__MYKO_ITEM_TYPE__'

type IMItem = {
  id: ID
  hash: string
}

export abstract class MItem implements IMItem {
  constructor(readonly id: ID, readonly hash: string) {}
}
