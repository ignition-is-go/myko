import {
  addMissingHash,
  MItem,
  MykoLogger,
  Repo,
  unwrapItem,
  type DeepPartial,
  type ID,
  type MEvent,
} from '@myko/core'
import { surrealdbWasmEngines } from '@surrealdb/wasm'
import { map, Observable, Subject, tap } from 'rxjs'
import { RecordId, Surreal, Uuid } from 'surrealdb'

export class SurrealRepo<T extends MItem> extends Repo<T> {
  private db: Surreal

  constructor(entity: string, options?: any) {
    super(entity, options)

    this.db = new Surreal({
      engines: surrealdbWasmEngines(),
    })

    this.db
      .connect('mem://', {
        namespace: 'rship',
        database: 'myko',
      })
      .then((_x) => {
        return this.db
          .query(`DEFINE TABLE OVERWRITE ${this.entity} CHANGEFEED 1000y`)
          .catch((_e) => {
            console.log('Table already exists')
          })
      })
  }

  async save(event: MEvent<T>): Promise<MEvent<T>> {
    const newId = new RecordId(this.entity, event.item.id)
    if (event.changeType === 'SET') {
      const item = event.item

      addMissingHash(item)

      Reflect.set(event, 'item', item)

      this.db.upsert(newId, event).catch((_e) => {
        console.error('Error upserting')
      })
    }
    if (event.changeType === 'DEL') {
      this.db.delete(newId)
    }
    return event
  }

  watch(query: DeepPartial<T>): Observable<T[]> {
    const comparisons = Object.entries(query).map(([key, value]) => {
      return `item.${key} == "${value}"`
    })

    if (comparisons.length === 0) {
      const queryStr = `SELECT * from ${this.entity}`
      return liveQueryObs(this.db, queryStr)
    }

    const queryStr = `SELECT * from ${this.entity} WHERE ${comparisons.join(' AND ')}`
    return liveQueryObs(this.db, queryStr)
  }

  watchFilter(filterFunc: (ent: T) => boolean): Observable<T[]> {
    console.warn(
      'watchFilter is not fast in implemented for SurrealRepo',
      this.entity,
    )
    return this.watch({} as DeepPartial<T>).pipe(
      map((x) => x.filter(filterFunc)),
    )
  }

  watchIds(ids: ID[]): Observable<T[]> {
    const query = `SELECT * FROM ${this.entity} WHERE id IN (${ids.map((x) => new RecordId(this.entity, x)).join(',')})`
    return liveQueryObs<T>(this.db, query)
  }

  watchId(id: ID): Observable<T> {
    const sid = new RecordId(this.entity, id)
    const query = `SELECT * FROM ${this.entity} WHERE id == ${sid}`

    return liveQueryObs<T>(this.db, query)
      .pipe(map((x) => x[0]))
      .pipe(tap(console.log))
  }

  getSearch(_query: string): Promise<T[]> {
    throw new Error('Method not implemented, getSearch')
  }
  watchSearch(_query: string): Observable<T[]> {
    throw new Error('Method not implemented, watchSearch')
  }
  getIds(ids: ID[]): Promise<T[]> {
    const sids = ids.map((x) => new RecordId(this.entity, x))

    const query = `SELECT * FROM ${this.entity} WHERE id IN (${sids.join(',')})`

    return this.db.query(query).then((x: [MEvent[]]) => {
      return x[0].map(unwrapItem) as T[]
    })
  }
  getId(id: ID): Promise<T | null> {
    const sid = new RecordId(this.entity, id)

    const query = `SELECT * FROM ${this.entity} WHERE id == ${sid}`

    return this.db.query(query).then((x: [MEvent[]]) => {
      const wrapped = x[0][0]

      if (!wrapped) {
        return null
      }

      return unwrapItem(wrapped) as T
    })
  }
  getIndex(_index: keyof T, _value: any): Promise<T[]> {
    throw new Error('Method not implemented, getIndex')
  }
  get(query: Partial<T>): Promise<T[]> {
    const comparisons = Object.entries(query).map(([key, value]) => {
      return `item.${key} == "${value}"`
    })

    if (comparisons.length === 0) {
      const queryStr = `SELECT * from ${this.entity}`
      return this.db.query(queryStr).then((x: [MEvent[]]) => {
        return x[0].map(unwrapItem) as T[]
      })
    }

    const queryStr = `SELECT * from ${this.entity} WHERE ${comparisons.join(' AND ')}`

    return this.db.query(queryStr).then((x: [MEvent[]]) => {
      return x[0].map(unwrapItem) as T[]
    })
  }
  getFilter(filterFunc: (ent: T) => boolean): Promise<T[]> {
    console.warn(
      'getFilter is not fast in implemented for SurrealRepo',
      this.entity,
    )
    return this.get({} as Partial<T>).then((x) => x.filter(filterFunc))
  }
}

const liveQueryObs = <T extends MItem>(
  db: Surreal,
  query: string,
): Observable<T[]> => {
  const subject = new Subject<T[]>()
  const state = new Map<string, T>()

  db.query(query).then((x: [MEvent[]]) => {
    const items = x[0].map(unwrapItem) as T[]

    items.forEach((item) => {
      state.set(item.id, item)
    })
    subject.next(items)
  })

  db.query(`LIVE ${query}`).then((x) => {
    db.subscribeLive(x as unknown as Uuid, (e, r) => {
      if (e === 'CLOSE') {
        new MykoLogger('SurrealRepo').warn('Live query closed')
        subject.complete()
        return
      }

      if (e === 'DELETE') {
        console.log('DELTEE', r)
      }

      if (e === 'CREATE') {
        console.log('CREATE', r)
        const item = unwrapItem(r as MEvent) as T
        state.set(item.id, item)
      }

      if (e === 'UPDATE') {
        console.log('UPDATE', r)
        const item = unwrapItem(r as MEvent) as T
        state.set(item.id, item)
      }

      const items = [...state.values()]

      subject.next(items)
    })
  })

  return subject.asObservable()
}
