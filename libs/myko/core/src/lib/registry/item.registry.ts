import type { Observable } from 'rxjs'
import type { EventContainer } from '../types'

export const getEvents: Map<
  string,
  (isoDateTime: string) => Observable<EventContainer[]>
> = new Map()

export const propertyDefaults: Map<string, Map<string, any>> = new Map()

export const inheritsRegistry: Map<string, string> = new Map()
