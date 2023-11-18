import type { ID, PartialBy } from './base'
import { MD5 } from 'object-hash'
import { type MItem as WASMItem } from 'myko-rs'

type IMItem = Omit<WASMItem, 'free'>

export type MItemConstructor<T extends IMItem> = new (
  args: PartialBy<T, 'hash'>,
) => MItem<T>

export class MItem<T extends IMItem = IMItem> {
  readonly id: ID
  readonly hash: string

  constructor(args: PartialBy<T, 'hash'>) {
    Reflect.set(this, 'hash', MD5(args))
    return args as unknown as MItem<T>
  }
}

export const recalculateHash = (item: MItem) => {
  Reflect.deleteProperty(item, 'hash')
  const clone = { ...item }
  const hash = MD5(clone)
  Reflect.set(item, 'hash', hash)
}
