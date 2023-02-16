import type { ID } from './base'

export const MYKO_ITEM_TYPE = '__MYKO_ITEM_TYPE__'

type IMItem = {
  id: ID
  name?: string
  scopeId: ID | null
  hash: string
}

export abstract class MItem<T extends IMItem = IMItem> implements IMItem {
  constructor(args: T) {
    for (let key in args) {
      Reflect.set(this, key, args[key])
    }
  }
  id: string
  name?: string
  scopeId: string
  hash: string
}
