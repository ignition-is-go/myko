import { Observable } from 'rxjs'
import { EventContainer, ID, MItem } from '../types'

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

export const inheritsRegistry = new Map<string, string>()
