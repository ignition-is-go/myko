export type ID = string

export interface Type<T = any> extends Function {
  new (...args: any[]): T
}
