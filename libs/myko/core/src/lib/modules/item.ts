import { MykoQuery } from '../decorators'
import { type ID, MItem, MQuery } from '../types'

@MykoQuery('item.getByTypeAndId', MItem)
export class GetItemsByTypeAndIds extends MQuery<MItem> {
  constructor(
    public type: string,
    public ids: ID[],
  ) {
    super()
  }
}
