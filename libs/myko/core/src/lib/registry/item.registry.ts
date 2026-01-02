import type { Observable } from 'rxjs'
import type { EventContainer, MItem, MItemConstructor } from '../types'

export const getEvents: Map<
  string,
  (isoDateTime: string) => Observable<EventContainer[]>
> = new Map()

export const propertyDefaults: Map<string, Map<string, any>> = new Map()

export const inheritsRegistry: Map<string, string> = new Map()

export const clientIdPropertyRegistry: Map<string, string> = new Map()

export const constructorRegistry: Map<
  string,
  MItemConstructor<MItem>
> = new Map()
