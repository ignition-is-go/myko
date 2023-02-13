import { IMykoItem } from './item'
import { Observable, Subject } from 'rxjs'
import { Change, ChangeType } from './events'

export type Stream<T extends IMykoItem> = Observable<Change<T, ChangeType>>
export type Publisher<T extends IMykoItem> = Subject<Change<T, ChangeType>>
