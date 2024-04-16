import { Subject } from 'rxjs'

export class ObservableBus<T> {
  protected _subject$ = new Subject<T>()

  constructor() {}

  public get subject$() {
    return this._subject$
  }
}
