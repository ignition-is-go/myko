import { IMykoItem } from './item'
import { Observable, Subject } from 'rxjs'
import { MykoEvent, MykoEventType } from './events'

export type Stream<T extends IMykoItem> = Observable<
  MykoEvent<T, MykoEventType>
>
export type Publisher<T extends IMykoItem> = Subject<
  MykoEvent<T, MykoEventType>
>
