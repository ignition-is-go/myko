import { Observable } from 'rxjs'
import { ID, MItem } from '../types'
import { EventContainer } from '../types'

export const inheritsRegistry = new Map<string, string>()

export type PropDocInfo = {
  entityType: string
  propName: string
  propType: string
  docString: string
  deprecated?: boolean
  type: 'prop'
}

export type ItemDocInfo = {
  entityType: string
  docString: string
  deprecated?: boolean
  extends?: string
  preventDocs?: boolean
  type: 'item'
}

export type CommandDocInfo = {
  entityType: string
  propName: string
  propType: string
  docString: string
  type: 'command'
}

export type QueryDocInfo = {
  entityType: string
  propName: string
  propType: string
  docString: string
  type: 'query'
}

export type DocTypeEntry =
  | PropDocInfo
  | ItemDocInfo
  | CommandDocInfo
  | QueryDocInfo

export const docRegistry: DocTypeEntry[] = []

export const addPropDoc = (item: Omit<PropDocInfo, 'type'>) => {
  docRegistry.push({ ...item, type: 'prop' })
}

export const addItemDoc = (info: Omit<ItemDocInfo, 'type'>) => {
  docRegistry.push({ ...info, type: 'item' })
  if (info.extends) {
    inheritsRegistry.set(info.entityType, info.extends)
  }
}

export const relationRegistry = new Set<Relation>()

export type Relation =
  | {
      type: 'belongs-to'
      foreignKey: 'id'
      foreignType: string
      localType: string
      localKey: string
    }
  | {
      type: 'owns-many'
      foreignKey: 'id'
      foreignType: string
      localKey: string
      localType: string
    }
  | {
      type: 'ensure-for'
      dependencies: {
        foreignType: string
        foreignKey: 'id'
        localKey: string
      }[]
      localType: string
      makeDefault: (...args: any) => void
    }

export const getIds = new Map<string, (ids: ID[]) => MItem[]>()
export const getFilters = new Map<
  string,
  (filterFunc: (el: MItem) => boolean) => MItem[]
>()
export const watchIds = new Map<string, (ids: ID[]) => Observable<MItem[]>>()

export const getEvents = new Map<
  string,
  (isoDateTime: string) => Observable<EventContainer[]>
>()

export const propertyDefaults = new Map<string, Map<string, any>>()
