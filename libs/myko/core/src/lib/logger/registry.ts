import { BehaviorSubject, Observable, shareReplay } from 'rxjs'
import { LogLevel } from './logLevel.type'

const names = new Map<string, {}>()

const namesSubject = new BehaviorSubject(names)

export const getNames = () => namesSubject.value

export const nameStream = namesSubject.pipe(shareReplay(1))

export const addName = (name: string) => {
  names.set(name, {})
  namesSubject.next(names)
}

export class Cell<T> {
  #subj: BehaviorSubject<T>

  constructor(public value: T) {
    this.#subj = new BehaviorSubject<T>(this.value)
  }

  set(value: T) {
    this.value = value
    this.#subj.next(value)
  }

  get() {
    return this.value
  }

  stream(): Observable<T> {
    return this.#subj.asObservable()
  }
}

export const longestName = new Cell<number>(0)

export const logsPreventedEntities = new Set<string>()

export const logFilter = new Cell<LogLevel>(LogLevel.INFO)
