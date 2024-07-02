import type { Observable, Subject } from 'rxjs'
import type { MEvent, MEventType } from './events'
import type { MItem } from './item'

/**
 * Represents a stream of events for a specific type of item.
 * @template T - The type of item.
 */
export type Stream<T extends MItem> = Observable<MEvent<T, MEventType>>

/**
 * Represents a publisher that emits events for a specific type of item.
 * @template T - The type of item.
 */
export type Publisher<T extends MItem> = Subject<MEvent<T, MEventType>>
