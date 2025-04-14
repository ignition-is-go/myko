import { Subject } from 'rxjs'
import type { ID } from '../types'

/**
 * Represents an observable bus that allows subscribing and emitting values of type T.
 *
 * @template T - The type of values emitted by the bus.
 */
export class ObservableBus<T> {
  protected _subject$: Subject<T> = new Subject()

  protected on_prepare: ((tx: ID, tag: string, callId: ID) => void) | undefined
  protected on_result: ((tx: ID, tag: string, callId: ID) => void) | undefined
  protected on_follow_up:
    | ((tx: ID, tag: string, callId: ID) => void)
    | undefined

  constructor() {}

  /**
   * Gets the subject of the observable bus.
   *
   * @returns The subject of the observable bus.
   */
  public get subject$(): Subject<T> {
    return this._subject$
  }

  onPrepare(cb: (tx: ID, tag: string, callId: ID) => void): void {
    if (this.on_prepare) {
      throw new Error('Prepare callback already set')
    }
    this.on_prepare = cb
  }

  onResult(cb: (tx: ID, tag: string, callId: ID) => void): void {
    if (this.on_result) {
      throw new Error('Result callback already set')
    }
    this.on_result = cb
  }

  onFollowUp(cb: (tx: ID, tag: string, callId: ID) => void): void {
    if (this.on_follow_up) {
      throw new Error('Result callback already set')
    }
    this.on_follow_up = cb
  }
}
