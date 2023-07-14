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

import { DateTime } from 'luxon'
import { decode, encode } from '@msgpack/msgpack'
import { start } from 'repl'

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
    const milis = DateTime.fromISO(isoDateTime).toMillis()

    const sub = new Subject<EventContainer>()

    this.listenForMessage(
      milis.toString(),
      sub,
      (e, rid) => new EventContainer({ event: e, id: rid }),
    )

    return sub.pipe(
      scan((acc, curr) => [...acc, curr], []),
      distinctUntilChanged((x, y) => x.length === y.length),
    )
  }

  persist(event: MEvent<T>): void {
    this.redis
      .xaddBuffer(
        this.entity,
        '*',
        Buffer.from(event.item.id),
        Buffer.from(encode(event)),
      )
      .then((id) => this.saveLatest(event, id.toString()))
  }

  saveLatest(event: MEvent<T, MEventType>, streamId: ID) {
    this.redis.setBuffer(
      `${this.entity}_latest_${event.item.id}`,
      Buffer.from(
        encode({
          event: event,
          lastId: streamId,
        }),
      ),
      'GET',
    )
  }

  async init() {
    const keys = await this.redis.keys(`${this.entity}_latest_*`)
    this.logger.getLogger('Init').dev.info(`${this.entity} Repo Initializing`)

    if (keys.length === 0) {
      this.logger
        .getLogger('Init')
        .dev.info(`${this.entity} no previous keys, starting from start`)

      return this.listenForMessage()
    }

    const itemJson = await this.redis.mgetBuffer(keys)

    const snaps: EntitySnapshot<T>[] = itemJson.map((x) =>
      decode(x),
    ) as EntitySnapshot<T>[]

    const streamIds = snaps
      .map((x) => x.lastId)
      .sort()
      .reverse()
    const events = snaps.map((x) => x.event)

    events.forEach((event) => {
      this.output.next(event)
    })
    const startId = streamIds.shift()

    this.logger
      .getLogger('Init')
      .dev.info(`${this.entity} Repo Started from ${startId}`)
    await this.listenForMessage(startId)
  }

  listenForMessage = async (
    lastId = '0',
    subject?: Subject<any>,
    makeObj?: (entityEvemt: MEvent, redisStreamId: string) => any,
  ): Promise<any> => {
    const results = await this.redis.xreadBuffer('STREAMS', this.entity, lastId)

    if (!results) {
      this.listenForMessage(lastId, subject, makeObj)
      return
    }

    const [key, messages] = results[0]

    if (messages.length === 0) {
      this.listenForMessage(lastId, subject, makeObj)
    }

    messages?.forEach((aa) => {
      const [streamEventId, entityPair] = aa
      const [entityId, entityString] = entityPair
      const event = decode(entityString) as MEvent

      const built = makeObj ? makeObj(event, streamEventId.toString()) : event

      if (subject) {
        subject.next(built)
      } else {
        this.output.next(built)
      }
    })
    const last = messages[messages.length - 1][0]
    this.listenForMessage(last.toString(), subject, makeObj)
  }
}

type EntitySnapshot<T extends MItem> = {
  event: MEvent<T, MEventType>
  lastId: string
}
