import type { Observable } from 'rxjs'
import type { MCommand } from './command'
import type { MEvent } from './events'

/**
 * Represents a saga function that processes events and produces commands.
 *
 * @template E The type of events that the saga processes.
 * @template C The type of commands that the saga produces.
 * @param events$ An observable stream of events.
 * @returns An observable stream of commands.
 */
export type MSaga<E extends MEvent = MEvent, C extends MCommand = MCommand> = (
  events$: Observable<E>,
) => Observable<C>
