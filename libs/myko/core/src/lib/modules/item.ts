import { MykoCommand, MykoReport } from '../decorators'
import {
  getItemName,
  MCommand,
  MItem,
  MReport,
  type DeepPartial,
  type ID,
  type MItemConstructor,
} from '../types'
import type { MWrappedItem } from '../wrappers'

@MykoReport()
export class GetItemsByTypeAndIds extends MReport<MWrappedItem[]> {
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

export type MItemStub = {
  id: ID
  itemType: string
  name?: string
  hash: string
}

@MykoReport()
export class ChildEntities extends MReport<MItemStub[]> {
  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {
    super()
  }
}

@MykoReport()
export class FullChildEntities extends MReport<MItemStub[]> {
  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {
    super()
  }
}

@MykoReport()
export class ChildEntitiesAllTime extends MReport<MItemStub[]> {
  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {
    super()
  }
}

export type EntitySnapshotDifferenceData = {
  changed: MItemStub[]
  added: MItemStub[]
  removed: MItemStub[]
}
@MykoReport()
export class EntitySnapshotDifference extends MReport<EntitySnapshotDifferenceData> {
  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {
    super()
  }
}

@MykoCommand()
export class ImportItems extends MCommand {
  constructor(public items: MWrappedItem[]) {
    super()
  }
}
