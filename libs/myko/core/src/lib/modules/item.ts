import { MykoQuery } from '../decorators'
import { MItem, MQuery, type ID } from '../types'

@MykoQuery(MItem)
export class GetItemsByTypeAndIds extends MQuery<MItem> {
  constructor(
    public type: string,
    public ids: ID[],
  ) {
    super()
  }
}
