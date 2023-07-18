import { MItem } from './item'

import { v4 as uuid } from 'uuid'
import { Observable } from 'rxjs'
import { ID } from './base'

export class MQuery<T extends MItem = MItem> {
  $result: T[]
  readonly tx: string
  constructor() {
    this.tx = uuid()
  }
}

export type MQueryResult<Q> = Q extends MQuery<infer R> ? Promise<R[]> : never

export type MLiveQueryResult<Q> = Q extends MQuery<infer R>
  ? Observable<R[]>
  : never

export interface MQueryHandler<H> {
  execute(query: H): MLiveQueryResult<H>
}
