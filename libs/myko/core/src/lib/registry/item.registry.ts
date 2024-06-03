import { Observable } from 'rxjs'
import { EventContainer, ID, MItem } from '../types'

export const getIds: Map<string, (ids: ID[]) => MItem[]> = new Map()

export const getFilters: Map<
  string,
  (filterFunc: (el: MItem) => boolean) => MItem[]
> = new Map()

export const watchIds: Map<string, (ids: ID[]) => Observable<MItem[]>> =
  new Map()

export const getEvents: Map<
  string,
  (isoDateTime: string) => Observable<EventContainer[]>
> = new Map()

export const propertyDefaults: Map<string, Map<string, any>> = new Map()

export const inheritsRegistry: Map<string, string> = new Map()
