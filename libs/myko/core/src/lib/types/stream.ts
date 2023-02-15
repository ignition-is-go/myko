import { IMykoItem } from './item'
import { Observable, Subject } from 'rxjs'
import { IMykoEvent, MykoEventType } from './events'

export type Stream<T extends IMykoItem> = Observable<
  IMykoEvent<T, MykoEventType>
>
export type Publisher<T extends IMykoItem> = Subject<
  IMykoEvent<T, MykoEventType>
>
