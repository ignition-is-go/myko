import { MD5 } from 'object-hash'
import type { ID, PartialBy } from './base'

export type IMItem = {
  id: ID
  hash: string
}

export type MItemConstructor<T extends IMItem> = new (
  args: PartialBy<T, 'hash'>,
) => MItem<T>

export class MItem<T extends IMItem = IMItem> {
  readonly id: ID
  readonly hash: string

  constructor(args: PartialBy<T, 'hash'>) {
    const hashed = addMissingHash(args)
    return hashed as MItem<T>
  }
}

export const recalculateHash = <T extends IMItem = IMItem>(
  item: PartialBy<T, 'hash'>,
): T => {
  Reflect.deleteProperty(item, 'hash')
  const hash = MD5(item)
  Reflect.set(item, 'hash', hash)
  return item as T
}

export const addMissingHash = <T extends IMItem = IMItem>(
  item: PartialBy<T, 'hash'>,
): T => {
  if (!item.hash) {
    return recalculateHash(item)
  }
  return item as T
}
