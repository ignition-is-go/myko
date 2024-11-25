import { BehaviorSubject, shareReplay } from 'rxjs'

const names = new Map<string, {}>()

const namesSubject = new BehaviorSubject(names)

export const getNames = () => namesSubject.value

export const nameStream = namesSubject.pipe(shareReplay(1))

export const addName = (name: string) => {
  names.set(name, {})
  namesSubject.next(names)
}

export class Cell<T> {
  constructor(public value: T) {}

  set(value: T) {
    this.value = value
  }

  get() {
    return this.value
  }
}

export const longestName = new Cell<number>(0)
