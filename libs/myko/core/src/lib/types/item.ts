import type { ID, PartialBy } from './base'
export const MYKO_ITEM_TYPE = '__MYKO_ITEM_TYPE__'

import { MD5 } from 'object-hash'

type IMItem = {
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
    args['hash'] = args.hash ?? MD5(args)
    return args as unknown as MItem<T>
  }
}
