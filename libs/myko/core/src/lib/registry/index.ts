import { Observable } from 'rxjs'
import { ID, MItem } from '../types'
import { EventContainer } from '../types'

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

export const getIds = new Map<string, (ids: ID[]) => MItem[]>()
export const getFilters = new Map<
  string,
  (filterFunc: (el: MItem) => boolean) => MItem[]
>()

export const getEvents = new Map<
  string,
  (isoDateTime: string) => Observable<EventContainer[]>
>()
