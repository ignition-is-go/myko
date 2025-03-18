import {
  HistoryProvider,
  MEventType,
  unwrapItem,
  type MEvent,
  type MItem,
} from '@myko/core'
import { from, map, scan, startWith, switchMap, type Observable } from 'rxjs'
import {
  create_index_myko_events_created_at,
  create_index_myko_events_entityId,
  create_index_myko_events_tx,
  create_index_userId,
  create_notify_myko_events,
  create_table_events,
  create_table_transactions,
  get_db_size,
  get_entities_as_of,
  get_event_stream,
  save_event,
  sql,
} from './postgres.sql'
import { rowToEvent, type EventCols } from './types'

import { uniq } from 'ramda'

export class PostgresHistory extends HistoryProvider {
  private isInit = false

  async init(): Promise<void> {
    await create_table_events()
    await create_index_myko_events_entityId()
    await create_index_myko_events_tx()
    await create_index_myko_events_created_at()

    await create_table_transactions()
    await create_index_userId()

    await create_notify_myko_events()

    this.isInit = true

    await get_db_size()
  }

  getEntityHistory(
    id: string,
    start?: string,
    end?: string,
  ): Observable<MEvent[]> {
    // get all events for entity

    const init =
      start && end
        ? sql<EventCols[]>`
      SELECT * FROM myko_events WHERE entity_id = ${id} AND created_at >= ${start} AND created_at <= ${end} ORDER BY created_at ASC
    `
        : end
          ? sql<EventCols[]>`
      SELECT * FROM myko_events WHERE entity_id = ${id} AND created_at <= ${end} ORDER BY created_at ASC
    `
          : start
            ? sql<EventCols[]>`
    
      SELECT * FROM myko_events WHERE entity_id = ${id} AND created_at >= ${start} ORDER BY created_at ASC
    `
            : sql<EventCols[]>`
      SELECT * FROM myko_events WHERE entity_id = ${id} ORDER BY created_at ASC
    `

    const items = init.then((x) => x.map(rowToEvent))

    const stream = get_event_stream()

    return from(items).pipe(
      switchMap((init) => {
        return stream.pipe(
          scan((acc, event) => {
            if (
              event.item.id === id &&
              start &&
              event.createdAt >= start &&
              end &&
              event.createdAt <= end
            ) {
              return [event, ...acc]
            }
            return acc
          }, init),
          startWith(init),
        )
      }),
    )
  }

  async getEntitiesAsOf<T extends MItem>(
    time: string,
    entity_type: string,
  ): Promise<T[]> {
    // get all entities as of time
    const mostRecentEventsRows = await get_entities_as_of(entity_type, time)

    if (!mostRecentEventsRows) {
      return []
    }
    const mostRecentEvents = mostRecentEventsRows.map(rowToEvent)

    const mostRecentItems = mostRecentEvents
      .filter((x) => x.changeType !== MEventType.DEL)
      .map(unwrapItem)

    return mostRecentItems as T[]
  }

  async getEntitySnapshot<T extends MItem>(
    id: string,
    entity_type: string,
    time: string,
  ): Promise<T | undefined> {
    // get most recent event before time

    const item = await sql`
      SELECT * FROM myko_events WHERE entity_id = ${id} AND item_type = ${entity_type} AND created_at < ${time}
      ORDER BY created_at DESC
      LIMIT 1
    `.then((res) => res.map(rowToEvent)[0])

    if (item?.changeType === MEventType.DEL) {
      return undefined
    }

    if (!item) {
      return undefined
    }

    return unwrapItem(item) as T
  }

  async saveEvent(event: MEvent): Promise<void> {
    if (!this.isInit) {
      throw new Error('History provider not initialized')
    }

    save_event(event)
  }

  getEventsForTransaction(id: string): Promise<MEvent[]> {
    return sql<EventCols[]>`
      SELECT * FROM myko_events WHERE tx = ${id}
    `.then((res) => res.map(rowToEvent))
  }

  getTransactionsInTimeRange(
    excludeEntities: string[],
    start: string,
    end?: string,
  ): Observable<string[]> {
    const stream = get_event_stream()

    // Build the query based on whether excludeEntities is empty
    let query: Promise<{ tx: string }[]>

    if (excludeEntities.length === 0) {
      // No exclusions needed
      query = end
        ? sql<{ tx: string }[]>`
            SELECT DISTINCT tx, created_at FROM myko_events 
            WHERE created_at >= ${start} 
            AND created_at <= ${end}
            ORDER BY created_at ASC
          `
        : sql<{ tx: string }[]>`
            SELECT DISTINCT tx, created_at FROM myko_events 
            WHERE created_at >= ${start} 
            ORDER BY created_at ASC
          `
    } else {
      // Apply exclusions
      query = end
        ? sql<{ tx: string }[]>`
            SELECT DISTINCT tx, created_at FROM myko_events 
            WHERE created_at >= ${start} 
            AND created_at <= ${end} 
            AND item_type NOT IN ${sql(excludeEntities)} 
            ORDER BY created_at ASC
          `
        : sql<{ tx: string }[]>`
            SELECT DISTINCT tx, created_at FROM myko_events 
            WHERE created_at >= ${start} 
            AND item_type NOT IN ${sql(excludeEntities)} 
            ORDER BY created_at ASC
          `
    }

    const init = query.then((res) => res.map((r) => r.tx)).then(uniq)

    return from(init)
      .pipe(
        switchMap((init) => {
          return stream.pipe(
            scan((acc, event) => {
              if (
                event.createdAt >= start &&
                (end === undefined || event.createdAt <= end) &&
                (excludeEntities.length === 0 ||
                  !excludeEntities.includes(event.itemType))
              ) {
                return [event.tx, ...acc]
              }
              return acc
            }, init),
            startWith(init),
          )
        }),
      )
      .pipe(map(uniq))
  }

  getEventsInTimeRange(start: string, end: string): Promise<MEvent[]> {
    return sql<EventCols[]>`
      SELECT * FROM myko_events WHERE created_at >= ${start} AND created_at <= ${end} ORDER BY created_at ASC
    `.then((res) => res.map(rowToEvent))
  }

  getAllTransactions(): Promise<string[]> {
    return sql<{ tx: string }[]>`
      SELECT tx, created_at from myko_events ORDER BY created_at ASC
    `
      .then((res) => res.map((r) => r.tx))
      .then(uniq)
  }

  async recordUserTransaction(
    userId: string,
    transactionId: string,
  ): Promise<void> {
    await sql`
      INSERT INTO myko_transactions (id, user_id) VALUES (${transactionId}, ${userId})
    `.catch((e) => {
      console.error('Error recording user transaction', e)
    })
  }
}
