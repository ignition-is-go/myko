import {
  buildFilter,
  MEventType,
  MItem,
  Repo,
  unwrapItem,
  type DeepPartial,
  type ID,
  type MEvent,
  type RepoOptions,
} from '@myko/core'

import { Database } from 'bun:sqlite'
import { existsSync, mkdirSync, writeFileSync } from 'node:fs'

export class SQLiteRepo<T extends MItem> extends Repo<T> {
  db: Database

  tableName: string
  eventTableName: string
  constructor(entity: string, options: RepoOptions<T>) {
    super(entity, options)

    this.tableName = `${entity}_table`
    this.eventTableName = `${entity}_events`

    const dbDir = 'db'
    if (!existsSync(dbDir)) {
      mkdirSync(dbDir)
    }

    writeFileSync('db/.gitignore', '*.db\n*.db-journal')

    this.db = new Database(`db/${entity}.db`, { strict: true })

    this.db.exec('DROP TABLE IF EXISTS ' + this.tableName)
    this.db.exec('DROP TABLE IF EXISTS ' + this.eventTableName)

    this.db.exec(
      `CREATE TABLE ${this.tableName} (id TEXT PRIMARY KEY, data TEXT)`,
    )
    this.db.exec(
      `CREATE TABLE ${this.eventTableName} (id INTEGER PRIMARY KEY, tx TEXT, createdAt TEXT, sourceId TEXT, changeType TEXT, data TEXT)`,
    )
  }

  async save(event: MEvent<T>): Promise<MEvent<T>> {
    const data = JSON.stringify(event.item)

    this.db.exec(
      `INSERT INTO ${this.eventTableName} (tx, createdAt, sourceId, changeType, data) VALUES (?, ?, ?, ?, ?)`,
      [event.tx, event.createdAt, event.sourceId!, event.changeType, data],
    )
    if (event.changeType === MEventType.SET) {
      this.db.exec(
        `INSERT OR REPLACE INTO ${this.tableName} (id, data) VALUES (?, ?)`,
        [event.item.id, data],
      )
    }
    if (event.changeType === MEventType.DEL) {
      this.db.exec(`DELETE FROM ${this.tableName} WHERE id = ?`, [
        event.item.id,
      ])
    }
    return event
  }

  async getId(id: ID): Promise<T | null> {
    const sql = `SELECT * FROM ${this.tableName} WHERE id = ?`

    const prep = this.db.prepare(sql).get(id)

    if (!prep) {
      return null
    }

    return unwrap([prep], this.entity)[0] as T
  }

  async getIndex(index: keyof T, value: any): Promise<T[]> {
    return this.getFilter((el) => el[index] === value)
  }

  async get(query: DeepPartial<T>): Promise<T[]> {
    const filter = buildFilter(query)

    return this.getFilter(filter)
  }

  async getFilter(filterFunc: (ent: T) => boolean): Promise<T[]> {
    const sql = `SELECT * FROM ${this.tableName}`
    const prep = this.db.prepare(sql).all()
    return unwrap(prep, this.entity).filter(filterFunc) as T[]
  }
}

const unwrap = <T extends MItem>(stored: unknown[], entity: string): T[] => {
  return (stored as { id: ID; data: string }[])
    .map((item) => JSON.parse(item.data))
    .map((item) => {
      return unwrapItem({
        item: item,
        itemType: entity,
      })
    }) as T[]
}
