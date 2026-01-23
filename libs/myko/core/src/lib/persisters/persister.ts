import { map, type Observable, Subject } from 'rxjs'
import type { MEvent, MItem } from '../types'

export type PersisterOutputEvent<T extends MItem> = {
  percent: number
  event: MEvent<T>
}

export abstract class Persister<T extends MItem> {
  constructor() {
    this.output$ = new Subject<PersisterOutputEvent<T>>()
    this.load$ = new Subject<PersisterOutputEvent<T>>()
  }

  protected output$: Subject<PersisterOutputEvent<T>>
  protected load$: Subject<PersisterOutputEvent<T>>

  get output(): Observable<MEvent<T>> {
    return this.output$.asObservable().pipe(map((x) => x.event))
  }

  get load(): Observable<PersisterOutputEvent<T>> {
    return this.load$.asObservable()
  }

  abstract persist(event: MEvent<T>): void
}

export type PersisterFactory = <O extends Record<string, any>, T extends MItem>(
  itemName: string,
  options?: O,
) => Persister<T>
