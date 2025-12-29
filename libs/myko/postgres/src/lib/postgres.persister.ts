import {
  getHostId,
  Persister,
  type MEvent,
  type MItem,
  type PersisterFactory,
  type PersisterOutputEvent,
} from '@myko/core'
import { Subject } from 'rxjs'
import { get_event_stream, save_event, sql } from './postgres.sql'
import { rowToEvent, type EventCols } from './types'

export class PostgrestPersister<T extends MItem> extends Persister<T> {
  constructor(private readonly item_type: string) {
    super()

    this.init()
  }

  async init(): Promise<void> {
    const rows = await sql<EventCols[]>`
      SELECT * FROM myko_events WHERE item_type = ${this.item_type} ORDER BY created_at ASC 
    `

    const events = rows.map(
      (row, index) =>
        ({
          event: rowToEvent(row) as MEvent<T>,
          percent: (index + 1) / rows.length,
        }) satisfies PersisterOutputEvent<T>,
    )

    for (const event of events) {
      this.load$.next(event)
    }

    this.load$.complete()

    get_event_stream().subscribe((event) => {
      if (event.itemType !== this.item_type) {
        return
      }

      if (event.itemType === 'Project') {
        console.log('Project event', event)
      }

      if (event.sourceId === getHostId()) {
        return
      }

      this.output$.next({ event: event as MEvent<T>, percent: 100 })
    })
  }

  protected load$: Subject<PersisterOutputEvent<T>> = new Subject()

  protected output$: Subject<PersisterOutputEvent<T>> = new Subject()

  persist(event: MEvent<T>): void {
    if (event.sourceId === undefined) {
      Reflect.set(event, 'sourceId', getHostId())
    }

    save_event(event)
    this.output$.next({ event, percent: 100 })
  }
}

export const postgresPersisterFactory: PersisterFactory = <T extends MItem>(itemName: string) => {
  return new PostgrestPersister<T>(itemName)
}
