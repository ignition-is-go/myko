import { MItem } from './item'
import { Observable, Subject } from 'rxjs'
import { MEvent, MEventType } from './events'

export type Stream<T extends MItem> = Observable<MEvent<T, MEventType>>
export type Publisher<T extends MItem> = Subject<MEvent<T, MEventType>>
