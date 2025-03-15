import { MykoQuery, MykoReport } from '../decorators'
import {
  getItemName,
  MItem,
  MQuery,
  MReport,
  type DeepPartial,
  type ID,
  type MItemConstructor,
} from '../types'
import type { MWrappedItem } from '../wrappers'

@MykoQuery(MItem)
export class GetItemsByTypeAndIds extends MQuery<MItem> {
  constructor(
    public type: string,
    public ids: ID[],
  ) {
    super()
  }
}

@MykoReport()
export class EntitySearch<T extends MItem> extends MReport<T[]> {
  readonly entityType: string
  constructor(
    readonly query: string,
    item: MItemConstructor<T>,
    readonly opts?: {
      showAllOnEmpty?: boolean
    },
    readonly filter?: DeepPartial<T>,
  ) {
    super()
    this.entityType = getItemName(item)
  }
}

@MykoReport()
export class ChildEntities extends MReport<MWrappedItem[]> {
  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {
    super()
  }
}
