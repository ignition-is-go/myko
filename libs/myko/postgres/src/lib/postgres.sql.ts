import type { MEvent } from '@myko/core'
import { randomUUID } from 'crypto'
import postgres from 'postgres'
import { Observable } from 'rxjs'
import { rowToEvent, type EventCols, type TXCols } from './types'

export const sql = postgres({
  host: 'b.rship.io',
  port: 5432,
  database: 'postgres',
  password: 'postgres',
  username: 'postgres',
})

export const create_table_transactions = async () =>
  await sql<TXCols[]>`CREATE TABLE IF NOT EXISTS myko_transactions(
      id TEXT NOT NULL PRIMARY KEY,
      user_id TEXT NOT NULL
      )`.catch((_e) => {})

export const create_index_userId = async () =>
  await sql`
      CREATE INDEX IF NOT EXISTS myko_transactions_user_id ON myko_transactions(user_id)`.catch(
    (_e) => {},
  )

export const get_entities_as_of = async (itemType: string, timestamp: string) =>
  await sql<EventCols[]>`
      WITH latest_events AS (
        SELECT DISTINCT ON (entity_id) *
        FROM myko_events
        WHERE item_type = ${itemType}
        AND created_at <= ${timestamp}
        ORDER BY created_at DESC
      )
      SELECT * FROM latest_events WHERE change_type = 'SET'
    `.catch((_e) => {})

export const create_table_events = async () =>
  await sql<EventCols[]>`CREATE TABLE IF NOT EXISTS myko_events(
      id SERIAL PRIMARY KEY,
      entity_id TEXT NOT NULL,
      item_type TEXT NOT NULL,
      change_type TEXT NOT NULL,
      item JSONB NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE,
      tx TEXT,
      source_id TEXT
      )`.catch((_e) => {})

export const create_index_myko_events_entityId = async () =>
  await sql`
      CREATE INDEX IF NOT EXISTS myko_events_entity_id ON myko_events(entity_id)`.catch(
    (_e) => {},
  )

export const create_index_myko_events_tx = async () =>
  await sql`
      CREATE INDEX IF NOT EXISTS myko_events_tx ON myko_events(tx)`.catch(
    (_e) => {},
  )

export const create_index_myko_events_created_at = async () =>
  await sql`
      CREATE INDEX IF NOT EXISTS myko_events_created_at ON myko_events(created_at)`.catch(
    (_e) => {},
  )

export const create_notify_myko_events = async () => {
  await sql`
      CREATE OR REPLACE FUNCTION myko_events_notify()
      RETURNS TRIGGER AS $$
      BEGIN
        PERFORM pg_notify('myko_events_notify', row_to_json(NEW)::TEXT);
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `.catch((_e) => {})

  await sql`
      DROP TRIGGER IF EXISTS myko_events_notify_trigger ON myko_events;
    `.catch((_e) => {})

  await sql`
      CREATE TRIGGER myko_events_notify_trigger
      AFTER INSERT ON myko_events
      FOR EACH ROW
      EXECUTE FUNCTION myko_events_notify();
    `.catch((_e) => {})
}

export const listen_notify_myko_events = async (
  cb: (payload: string) => void,
  onListen?: () => void,
) => {
  const d = await sql.listen('myko_events_notify', cb, onListen)

  return () => {
    d.unlisten()
  }
}

export const get_event_stream = (): Observable<MEvent> => {
  return new Observable((subscriber) => {
    const teardown = listen_notify_myko_events((payload) => {
      subscriber.next(rowToEvent(JSON.parse(payload)))
    })
  })
}

export const save_event = async (event: MEvent): Promise<void> => {
  await sql<EventCols[]>`
      INSERT INTO myko_events (
        entity_id, 
        item_type,
        change_type,
        item,
        created_at,
        tx,
        source_id
      )  VALUES (
        ${event.item.id}, 
        ${event.itemType}, 
        ${event.changeType}, 
        ${JSON.stringify(event.item)}, 
        ${event.createdAt}, 
        ${event.tx ?? randomUUID()},
        ${event.sourceId ?? ''}
      )
    `.catch((e) => {
    if (e.code === '23505') {
      // swallow duplicate key errors
      return
    }
    console.error('Error saving event', e, event)
  })

  return
}

export const get_db_size = async () => {
  const result = await sql`SELECT
      pg_database.datname,
      pg_size_pretty(pg_database_size(pg_database.datname)) 
      AS size FROM pg_database;`

  console.log(
    result
      .map((x) => ({ name: x['datname'], size: x['size'] }))
      .map((x) => `${x.name}: ${x.size}`)
      .join('\n'),
  )
}
