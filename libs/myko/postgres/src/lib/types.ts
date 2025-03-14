import type { MEvent, MEventType } from '@myko/core'

export type TXCols = {
  id: string
  user_id: string
}

export type EventCols = {
  id: string
  entity_id: string
  item_type: string
  change_type: string
  item: string
  created_at: string
  tx: string
  source_id: string
}

export const rowToEvent = (row: EventCols): MEvent => ({
  changeType: row.change_type as MEventType,
  createdAt: row.created_at,
  item: JSON.parse(row.item),
  itemType: row.item_type,
  tx: row.tx,
  sourceId: row.source_id,
})
