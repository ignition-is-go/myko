import { Injectable } from '@nestjs/common'
import { RedisService } from '@liaoliaots/nestjs-redis'
import {
  MItem,
  Persister,
  MEvent,
  MYKO_ITEM_TYPE,
  ID,
  MItemConstructor,
  EventContainer,
  getEvents,
  fireInit,
} from '@myko/core'
import { Observable, Subject, distinctUntilChanged, scan } from 'rxjs'
import { Redis } from 'ioredis'
import { LoggerService } from '@rship/logging'

import { decode, encode } from '@msgpack/msgpack'

export type RedisPersisterOptions = {
  enableEventLog: boolean
  autoGetAllNew: boolean
}

const optionDefaults: RedisPersisterOptions = {
  enableEventLog: true,
  autoGetAllNew: true,
}

@Injectable()
export class RedisPersisterFactory {
  constructor(private redis: RedisService, private logger: LoggerService) {}
  getPersister<T extends MItem>(
    ent: MItemConstructor<T>,
    options?: RedisPersisterOptions,
  ) {
    const client = this.redis.getClient()

    const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    if (!entity) {
      throw new Error('Cannot get Entity from Metadata')
    }
    return new RedisStreamPersister<T>(client, entity, this.logger, {
      ...optionDefaults,
      ...options,
    })
  }
}

export class RedisStreamPersister<T extends MItem> implements Persister<T> {
  streamHandles = new Map<ID, StreamListener<MEvent<T>>>()

  private newItemsKey: string

  private newItemsListener: StreamListener<ID[]>

  output: Subject<MEvent<T>>
  constructor(
    private redis: Redis,
    private entity: string,
    private logger: LoggerService,
    private readonly options: RedisPersisterOptions,
  ) {
    this.output = new Subject<MEvent<T>>()

    if (this.options.enableEventLog) {
      getEvents.set(this.entity, this.getEvents.bind(this))
    }

    if (this.options.autoGetAllNew) {
      this.newItemsKey = `${this.entity}_new`
      this.newItemsListener = new StreamListener<ID[]>(
        this.newItemsKey,
        this.redis,
        (ids) => {
          ids.forEach((id) => {
            this.assertHandler(id)
          })
        },
        (ids) => this.newItemsKey,
        () => {},
      )
    }
    this.init().then(() => fireInit(this.entity))
  }

  getEvents(isoDateTime: string): Observable<EventContainer[]> {
    const sub = new Subject<EventContainer>()

    return sub.pipe(
      scan((acc, curr) => [...acc, curr], []),
      distinctUntilChanged((x, y) => x.length === y.length),
    )
  }
  assertHandler(itemId: string): Promise<void> {
    return new Promise((resolve) => {
      if (!this.streamHandles.has(itemId)) {
        this.streamHandles.set(
          itemId,
          new StreamListener<MEvent<T>>(
            `${this.entity}:${itemId}`,
            this.redis,
            (event) => {
              this.output.next(event)
            },
            (item) => item.item.id,
            resolve,
          ),
        )
        this.newItemsListener.persist([...this.streamHandles.keys()])
      }
    })
  }

  persist(event: MEvent<T>): void {
    if (event.itemType !== this.entity) {
      return
    }

    this.assertHandler(event.item.id)

    const handlers = this.streamHandles.get(event.item.id)

    try {
      handlers.persist(event)
    } catch (e) {
      console.warn(e)
    }
  }

  async init() {
    const keys = await this.redis.keys(`${this.entity}:*`)
    this.logger.getLogger('Init').dev.info(`${this.entity} Repo Initializing`)

    const itemIds = keys.map((key) => key.replace(`${this.entity}:`, ''))

    await Promise.all(itemIds.map((itemId) => this.assertHandler(itemId)))

    fireInit(this.entity)
  }
}

class StreamListener<U> {
  constructor(
    private streamKey: string,
    private redis: Redis,
    private onEvent: (event: U) => any,
    private makeId: (item: U) => string,
    private onInit: () => void,
  ) {
    this.listen()
  }

  lastId: string | undefined

  async prime() {
    const length = await this.redis.xlen(this.streamKey)

    if (length === 0) {
      this.lastId = '0'
      this.onInit()
      return
    }

    const a = await this.redis.xrevrangeBuffer(
      this.streamKey,
      '+',
      '-',
      'COUNT',
      1,
    )
    if (!a) {
      return
    }

    if (a.length === 0) {
      return
    }

    const last = a[0]
    this.lastId = last[0].toString()

    const lastEntity = decode(Buffer.from(last[1][1]))

    this.onEvent(lastEntity as U)
    this.onInit()
  }

  async listen() {
    if (!this.lastId) {
      await this.prime()
    }

    this.redis
      .xreadBuffer('STREAMS', this.streamKey, this.lastId)
      .then((results) => {
        if (!results) {
          this.listen()
          return
        }

        const [key, messages] = results[0]

        if (messages.length === 0) {
          this.listen()
          return
        }

        messages?.forEach((aa) => {
          const [streamEventId, entityPair] = aa
          const [entityId, entityString] = entityPair
          const event = decode(entityString) as U

          this.onEvent(event)
        })
        const last = messages[messages.length - 1][0]
        this.lastId = last.toString()
        this.listen()
      })
  }

  persist(event: U) {
    this.redis.xaddBuffer(
      this.streamKey,
      '*',
      Buffer.from(this.makeId(event)),
      Buffer.from(encode(event)),
    )
  }
}
