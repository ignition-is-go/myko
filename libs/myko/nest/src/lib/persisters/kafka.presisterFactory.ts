import { Inject, Injectable } from '@nestjs/common'
import {
  MItem,
  Persister,
  MEvent,
  MYKO_ITEM_TYPE,
  ID,
  MItemConstructor,
  fireInit,
  ControledPersister,
  beforeInit,
  MEventType,
  Server,
} from '@myko/core'
import { Subject } from 'rxjs'
import { LoggerService } from '@rship/logging'

import { decode, encode } from '@msgpack/msgpack'
import { ConfigService } from '@nestjs/config'
import {
  Producer,
  Message,
  AdminClient,
  GlobalConfig,
  ProducerGlobalConfig,
  ConsumerGlobalConfig,
} from 'node-rdkafka'
import { v4 as uuid } from 'uuid'
import { MykoQueryBus } from '../busses'

import { Redis } from 'ioredis'
import { SERVER_TOKEN } from '../../types'
import { KafkaTopicConsumer } from './util/kafka.topicConsumer'
import { KafkaTopicProducer } from './util/kafka.topicProducer'
import { MultiMap } from './util/multimap'

export type KafkaPersisterOptions = {
  enableEventLog: boolean
}

const optionDefaults: Partial<KafkaPersisterOptions> = {
  enableEventLog: true,
}

@Injectable()
export class KafkaPersisterFactory {
  private conf: GlobalConfig
  private prodConf: ProducerGlobalConfig
  private consConf: ConsumerGlobalConfig

  constructor(
    private logger: LoggerService,
    private config: ConfigService,
    private query: MykoQueryBus,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {
    this.conf = {
      'metadata.broker.list': this.getBrokers().join(','),
    }

    this.prodConf = {
      'allow.auto.create.topics': true,
    }

    this.consConf = {
      'group.id': server.id,
      'allow.auto.create.topics': true,
    }
  }

  private getBrokers(): string[] {
    const brokersString = this.config.get('KAFKA_BROKERS')
    if (!brokersString) {
      throw new Error('KAFKA_BROKERS not set')
    }

    return brokersString.split(',')
  }

  getControlledPersister<T extends MItem>(
    ent: MItemConstructor<T>,
  ): ControledPersister<T> {
    const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    if (!entity) {
      throw new Error('Cannot get Entity from Metadata')
    }

    return new KafkaControlledPersister(
      entity,
      this.logger,
      {
        enableEventLog: false,
      },
      this.conf,
      this.prodConf,
      this.consConf,
      this.server,
    )
  }

  getPersister<T extends MItem>(
    ent: MItemConstructor<T>,
    options?: KafkaPersisterOptions,
  ) {
    const opts = {
      ...optionDefaults,
      ...options,
    }

    const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    if (!entity) {
      throw new Error('Cannot get Entity from Metadata')
    }

    return new KafkaEntityPersister<T>(
      entity,
      this.logger,
      opts,
      this.conf,
      this.prodConf,
      this.consConf,
      this.server,
    )
  }
}

abstract class KafkaPersister<T extends MItem> implements Persister<T> {
  redis: Redis

  constructor(
    protected entity: string,
    protected logger: LoggerService,
    protected options: KafkaPersisterOptions,
    protected server: Server,
  ) {
    const redisHost = process.env.REDIS_HOST || 'localhost'
    const redisPort = Number.parseInt(process.env.REDIS_PORT) || 6379

    this.redis = new Redis(redisPort, redisHost)

    this.output = new Subject<MEvent<T>>()

    this.init()
  }

  public output: Subject<MEvent<T>>

  protected onMessage(message: Pick<Message, 'value' | 'offset'>) {
    const event = decode(message.value) as MEvent<T>

    const id = event.item.id

    const latestKey = `${this.entity}:latest:${id}`

    const offsetKey = `${this.entity}:offset`

    if (event.changeType === MEventType.SET) {
      this.redis.set(latestKey, message.value)
    }

    if (event.changeType === MEventType.DEL) {
      this.redis.del(latestKey)
    }

    this.redis.set(offsetKey, message.offset)

    if (event.sourceId === this.server.id) {
      return
    }

    this.onEvent(event)
  }

  protected onEvent(event: MEvent<T>) {
    this.output.next(event)
  }

  async init() {
    const keys = await this.redis.keys(`${this.entity}:latest:*`)

    keys.forEach(async (key) => {
      const data = await this.redis.getBuffer(key)

      const event = decode(data) as MEvent<T>

      this.onEvent(event)
    })
  }

  abstract persist(event: MEvent<T>): void

  protected onInit() {
    fireInit(this.entity)
  }
}

export class KafkaEntityPersister<T extends MItem> extends KafkaPersister<T> {
  cons: KafkaTopicConsumer
  prod: KafkaTopicProducer

  constructor(
    entity: string,
    logger: LoggerService,
    options: KafkaPersisterOptions,
    private config: GlobalConfig,
    private prodConfig: ProducerGlobalConfig,
    private consConfig: ConsumerGlobalConfig,
    server: Server,
  ) {
    super(entity, logger, options, server)
    beforeInit(entity)

    const admin = AdminClient.create(this.config)
    admin.createTopic({
      topic: this.entity,
      replication_factor: 3,
      num_partitions: 1,
      config: {
        'max.message.bytes': '-1',
        'retention.ms': '-1',
        'retention.bytes': '-1',
      },
    })

    this.cons = new KafkaTopicConsumer(
      { ...this.config, ...this.consConfig },
      this.entity,
      (msg) => this.onMessage(msg),
      `${this.entity}:offset`,
      `${this.entity}:partition`,
      () => {
        this.onInit()
      },
    )

    this.prod = new KafkaTopicProducer(
      this.entity,
      { ...this.config, ...this.prodConfig },
      (msg) =>
        this.logger
          .getLogger(`${this.entity}.KafkaTopicProducer`)
          .dev.log('info', msg),
    )

    this.init()
  }

  persist(event: MEvent<T>): void {
    this.prod.publish(Buffer.from(encode(event)), event.item.id)
    this.onEvent(event)
  }
}

export class KafkaControlledPersister<T extends MItem>
  extends KafkaPersister<T>
  implements ControledPersister<T>
{
  prod: Producer

  prodConnected = false

  consumers: Map<ID, KafkaTopicConsumer> = new Map()
  producers: Map<ID, KafkaTopicProducer> = new Map()
  // maps from entityId to a set of releaseIds
  handles: MultiMap<ID, ID> = new MultiMap()

  output: Subject<MEvent<T>>

  constructor(
    entity: string,
    logger: LoggerService,
    options: KafkaPersisterOptions,
    private config: GlobalConfig,
    private prodConf: ProducerGlobalConfig,
    private consConf: ConsumerGlobalConfig,
    server: Server,
  ) {
    super(entity, logger, options, server)
  }

  persist(event: MEvent<T>): void {
    const topic = this.makeProducerTopic(event)

    if (!this.producers.has(topic)) {
      this.producers.set(
        topic,
        new KafkaTopicProducer(
          topic,
          {
            ...this.config,
            ...this.prodConf,
          },
          (msg) =>
            this.logger
              .getLogger(`${this.entity}.KafkaTopicProducer`)
              .dev.log('info', msg),
        ),
      )
    }

    const producer = this.producers.get(topic)

    if (!producer) {
      throw new Error('Producer not found')
    }

    producer.publish(Buffer.from(encode(event)), event.item.id)

    this.onEvent(event)
  }

  listenId(id: string, fromBeginning = false) {
    const releaseId = uuid()

    if (this.consumers.has(id)) {
      this.logger
        .getLogger(`Kafka.${this.entity}`)
        .dev.log('warn', `Already listening to ${id}`)
      return
    }

    const topic = `${this.entity}_${id}`
    const cons = new KafkaTopicConsumer(
      { ...this.config, ...this.consConf },
      topic,
      (msg) => this.onMessage(msg),
      `${topic}:offset`,
      `${topic}:partition`,
    )

    this.handles.add(id, releaseId)
    this.consumers.set(id, cons)
    return releaseId
  }

  release(releaseId: string): void {
    const keyRemoved = this.handles.removeValue(releaseId)

    if (!keyRemoved) {
      return
    }

    const cons = this.consumers.get(keyRemoved)
    cons.disconnect()
    this.consumers.delete(keyRemoved)
  }

  makeProducerTopic(event: MEvent<T>): string {
    return `${this.entity}_${event.item.id}`
  }
}
