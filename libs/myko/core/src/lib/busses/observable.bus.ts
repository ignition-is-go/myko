import { Subject } from 'rxjs'

export class ObservableBus<T> {
  protected _subject$: Subject<T> = new Subject()

  constructor() {}

  public get subject$(): Subject<T> {
    return this._subject$
  }
}
