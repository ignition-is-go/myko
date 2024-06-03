import { Subject } from 'rxjs'

/**
 * Represents an observable bus that allows subscribing and emitting values of type T.
 *
 * @template T - The type of values emitted by the bus.
 */
export class ObservableBus<T> {
  protected _subject$: Subject<T> = new Subject()

  constructor() {}

  /**
   * Gets the subject of the observable bus.
   *
   * @returns The subject of the observable bus.
   */
  public get subject$(): Subject<T> {
    return this._subject$
  }
}
