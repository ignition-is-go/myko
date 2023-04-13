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
} from '@myko/core'
import { Observable, Subject } from 'rxjs'
import { Redis } from 'ioredis'
import { LoggerService } from '@rship/logging'

@Injectable()
export class RedisPersisterFactory {
  constructor(private redis: RedisService, private logger: LoggerService) {}
  getPersister<T extends MItem>(ent: MItemConstructor<T>) {
    const client = this.redis.getClient()

    const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    if (!entity) {
      throw new Error('Cannot get Entity from Metadata')
    }
    return new RedisStreamPersister<T>(client, entity, this.logger)
  }
}

export class RedisStreamPersister<T extends MItem> implements Persister<T> {
  output: Subject<MEvent<T>>
  constructor(
    private redis: Redis,
    private entity: string,
    private logger: LoggerService,
  ) {
    this.output = new Subject<MEvent<T>>()
    this.init()
  }

  persist(event: MEvent<T>): void {
    this.redis
      .xadd(this.entity, '*', event.item.id, JSON.stringify(event))
      .then((id) => this.saveLatest(event, id))
  }

  saveLatest(event: MEvent<T, MEventType>, streamId: ID) {
    this.redis.set(
      `${this.entity}_latest_${event.item.id}`,
      JSON.stringify({
        event: event,
        lastId: streamId,
      } satisfies EntitySnapshot<T>),
    )
  }

  async init() {
    const keys = await this.redis.keys(`${this.entity}_latest_*`)

    if (keys.length === 0) {
      return this.listenForMessage()
    }

    const itemJson = await this.redis.mget(keys)
    const snaps: EntitySnapshot<T>[] = itemJson.map((x) => JSON.parse(x))

    const streamIds = snaps
      .map((x) => x.lastId)
      .sort()
      .reverse()
    const events = snaps.map((x) => x.event)

    events.forEach((event) => {
      this.output.next(event)
    })

    await this.listenForMessage(streamIds.shift())
    this.logger.getLogger('Init').dev.info(`${this.entity} Repo Initialized`)
  }

  listenForMessage = async (lastId = '0'): Promise<any> => {
    const results = await this.redis.xread('STREAMS', this.entity, lastId)

    if (!results) {
      this.listenForMessage(lastId)
      return
    }

    const [key, messages] = results[0]

    if (messages.length === 0) {
      this.listenForMessage(lastId)
    }

    messages?.forEach((aa) => {
      const [streamId, entityPair] = aa
      const [id, entityString] = entityPair
      const event = JSON.parse(entityString)
      this.output.next(event)
    })
    const last = messages[messages.length - 1][0]
    this.listenForMessage(last)
  }
}

type EntitySnapshot<T extends MItem> = {
  event: MEvent<T, MEventType>
  lastId: string
}
