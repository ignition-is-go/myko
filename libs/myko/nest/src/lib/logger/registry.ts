export const names = new Map<string, {}>()

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
