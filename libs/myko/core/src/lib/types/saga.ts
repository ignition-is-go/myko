import { Observable } from 'rxjs'
import { MCommand } from './command'
import { MEvent } from './events'

export const MYKO_SAGA_METADATA = '__MYKO_SAGA__'

export type MSaga<E extends MEvent = MEvent, C extends MCommand = MCommand> = (
  events$: Observable<E>,
) => Observable<C>
