import { Injectable } from '@nestjs/common'
import { RedisService } from '@liaoliaots/nestjs-redis'
import {
  MItem,
  Persister,
  MEvent,
  MEventType,
  MYKO_ITEM_TYPE,
} from '@myko/core'
import { Observable, Subject } from 'rxjs'
import { Redis } from 'ioredis'

@Injectable()
export class RedisPersisterFactory {
  constructor(private redis: RedisService) {}
  getPersister<T extends MItem>(ent: new (args: T) => T) {
    const client = this.redis.getClient()

    const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    if (!entity) {
      throw new Error('Cannot get Entity from Metadata')
    }
    return new RedisStreamPersister<T>(client, entity)
  }
}

export class RedisStreamPersister<T extends MItem> implements Persister<T> {
  output: Subject<MEvent<T>>
  constructor(private redis: Redis, private entity: string) {
    this.output = new Subject<MEvent<T>>()
    this.init()
  }

  persist(event: MEvent<T>): void {
    this.redis.xadd(this.entity, '*', event.item.id, JSON.stringify(event))
  }

  init(): Observable<MEvent<T, MEventType>> {
    this.listenForMessage('0')
    return this.output.pipe()
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
    if (lastId === '0') {
      console.log(this.entity, 'Repo Init Complete')
    }
    this.listenForMessage(last)
  }
}
