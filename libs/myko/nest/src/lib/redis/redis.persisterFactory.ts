import { Injectable } from '@nestjs/common'
import { RedisService } from '@liaoliaots/nestjs-redis'
import {
  MItem,
  Persister,
  MEvent,
  MEventType,
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

@Injectable()
export class RedisPersisterFactory {
  constructor(private redis: RedisService, private logger: LoggerService) {}
  getPersister<T extends MItem>(
    ent: MItemConstructor<T>,
    options: {
      enableEventLog: boolean
    } = {
      enableEventLog: true,
    },
  ) {
    const client = this.redis.getClient()

    const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    if (!entity) {
      throw new Error('Cannot get Entity from Metadata')
    }
    return new RedisStreamPersister<T>(client, entity, this.logger, options)
  }
}

export class RedisStreamPersister<T extends MItem> implements Persister<T> {
  streamHandles = new Map<ID, StreamListener<T>>()

  output: Subject<MEvent<T>>
  constructor(
    private redis: Redis,
    private entity: string,
    private logger: LoggerService,
    private readonly options: {
      enableEventLog: boolean
    } = {
      enableEventLog: true,
    },
  ) {
    this.output = new Subject<MEvent<T>>()
    this.init().then(() => fireInit(this.entity))

    if (this.options.enableEventLog) {
      getEvents.set(this.entity, this.getEvents.bind(this))
    }
  }

  getEvents(isoDateTime: string): Observable<EventContainer[]> {
    const sub = new Subject<EventContainer>()

    return sub.pipe(
      scan((acc, curr) => [...acc, curr], []),
      distinctUntilChanged((x, y) => x.length === y.length),
    )
  }
  assertHandler(itemId: string) {
    if (!this.streamHandles.has(itemId)) {
      this.streamHandles.set(
        itemId,
        new StreamListener(`${this.entity}_${itemId}`, this.redis, (event) => {
          this.output.next(event)
        }),
      )
    }
  }

  persist(event: MEvent<T>): void {
    if (event.itemType !== this.entity) {
      return
    }

    this.assertHandler(event.item.id)

    const handlers = this.streamHandles.get(event.item.id)

    handlers.persist(event)
  }

  async init() {
    const keys = await this.redis.keys(`${this.entity}_*`)
    this.logger.getLogger('Init').dev.info(`${this.entity} Repo Initializing`)

    const itemIds = keys.map((key) => key.replace(`${this.entity}_`, ''))

    itemIds.forEach((itemId) => {
      this.assertHandler(itemId)
    })
  }
}

class StreamListener<T extends MItem> {
  constructor(
    private streamKey: string,
    private redis: Redis,
    private onEvent: (event: MEvent<T>) => any,
  ) {
    this.listen()
  }

  lastId: string | undefined

  async prime() {
    const length = await this.redis.xlen(this.streamKey)

    if (length === 0) {
      this.lastId = '0'
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

    this.onEvent(lastEntity as MEvent<T>)
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
          const event = decode(entityString) as MEvent<T>

          this.onEvent(event)
        })
        const last = messages[messages.length - 1][0]
        this.lastId = last.toString()
        this.listen()
      })
  }

  persist(event: MEvent<T>) {
    this.redis.xaddBuffer(
      this.streamKey,
      '*',
      Buffer.from(event.item.id),
      Buffer.from(encode(event)),
    )
  }
}
