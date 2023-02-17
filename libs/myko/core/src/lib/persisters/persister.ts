import { Observable } from 'rxjs'
import { MEvent, MItem } from '../types'

export abstract class Persister<T extends MItem> {
  abstract persist(event: MEvent<T>): void

  abstract init(): Observable<MEvent<T>>
}
