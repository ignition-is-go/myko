import { Observable } from 'rxjs'
import { IMykoCommand } from './command'
import { IMykoEvent, MykoEventType } from './events'
import { IMykoItem } from './item'

export const MYKO_SAGA_METADATA = '__MYKO_SAGA__'

export type IMykoSaga<
  E extends IMykoEvent<IMykoItem, MykoEventType>,
  C extends IMykoCommand,
> = (events$: Observable<E>) => Observable<C>
