import { Subject } from 'rxjs'
import type { ID, WithContext } from '../types'

type BusCallback = (mContext: WithContext, callId: ID) => void

/**
 * Represents an observable bus that allows subscribing and emitting values of type T.
 *
 * @template T - The type of values emitted by the bus.
 */
export class ObservableBus<T> {
  protected _subject$: Subject<T> = new Subject()

  protected on_prepare_callbacks: BusCallback[] = []
  protected on_result_callbacks: BusCallback[] = []
  protected on_follow_up_callbacks: BusCallback[] = []

  /**
   * Gets the subject of the observable bus.
   *
   * @returns The subject of the observable bus.
   */
  public get subject$(): Subject<T> {
    return this._subject$
  }

  onPrepare(cb: BusCallback): void {
    this.on_prepare_callbacks.push(cb)
  }

  onResult(cb: BusCallback): void {
    this.on_result_callbacks.push(cb)
  }

  onFollowUp(cb: BusCallback): void {
    this.on_follow_up_callbacks.push(cb)
  }

  protected callPrepareCallbacks(ctx: WithContext, callId: ID): void {
    for (const cb of this.on_prepare_callbacks) {
      cb(ctx, callId)
    }
  }

  protected callResultCallbacks(ctx: WithContext, callId: ID): void {
    for (const cb of this.on_result_callbacks) {
      cb(ctx, callId)
    }
  }

  protected callFollowUpCallbacks(ctx: WithContext, callId: ID): void {
    for (const cb of this.on_follow_up_callbacks) {
      cb(ctx, callId)
    }
  }
}
