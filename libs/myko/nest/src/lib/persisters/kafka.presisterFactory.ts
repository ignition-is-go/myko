import {
  MEvent,
  MEventType,
  MItem,
  MItemConstructor,
  MYKO_ITEM_TYPE,
  Persister,
  Server,
  beforeInit,
  fireInit,
} from '@myko/core'
import { Inject, Injectable } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { Redis } from 'ioredis'
import { unpack as decode } from 'msgpackr'
import {
  AdminClient,
  ConsumerGlobalConfig,
  GlobalConfig,
  Message,
  ProducerGlobalConfig,
} from 'node-rdkafka'
import { Subject } from 'rxjs'
import { SERVER_TOKEN } from '../../types'
import { MykoQueryBus } from '../busses'
import { newTopic } from './util/kafka.newTopic'
import { KafkaTopicConsumer } from './util/kafka.topicConsumer'
import { KafkaTopicProducer } from './util/kafka.topicProducer'

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

  // getControlledPersister<T extends MItem>(
  //   ent: MItemConstructor<T>,
  // ): ControledPersister<T> {
  //   const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

  //   if (!entity) {
  //     throw new Error('Cannot get Entity from Metadata')
  //   }

  //   return new KafkaControlledPersister(
  //     entity,
  //     this.logger,
  //     {
  //       enableEventLog: false,
  //     },
  //     this.conf,
  //     this.prodConf,
  //     this.consConf,
  //     this.server,
  //   )
  // }

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

  private decodeMsg(buffer): MEvent<T> | null {
    try {
      return decode(buffer) as MEvent<T>
    } catch (e) {}

    try {
      return JSON.parse(buffer.toString()) as MEvent<T>
    } catch (e) {}

    console.warn('could not decode event', buffer.toString())

    return null
  }

  protected encodeMsg(event: MEvent<T>): Buffer {
    return Buffer.from(JSON.stringify(event))
  }

  protected onMessage(message: Pick<Message, 'value' | 'offset'>) {
    const event = this.decodeMsg(message.value)

    if (!event) {
      return
    }

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

      const event = this.decodeMsg(data) as MEvent<T>

      if (!event) {
        return
      }

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
    options: KafkaPersisterOptions,
    private config: GlobalConfig,
    private prodConfig: ProducerGlobalConfig,
    private consConfig: ConsumerGlobalConfig,
    server: Server,
  ) {
    super(entity, options, server)
    beforeInit(entity)

    const admin = AdminClient.create(this.config)
    admin.createTopic(newTopic(this.entity))

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
      (msg) => console.log(this.entity, 'KafkaTopicProducer', msg),
    )

    this.init()
  }

  persist(event: MEvent<T>): void {
    this.prod.publish(this.encodeMsg(event), event.item.id)
    this.onEvent(event)
  }
}

// export class KafkaControlledPersister<T extends MItem>
//   extends KafkaPersister<T>
//   implements ControledPersister<T>
// {
//   prod: Producer

//   prodConnected = false

//   consumers: Map<ID, KafkaTopicConsumer> = new Map()
//   producers: Map<ID, KafkaTopicProducer> = new Map()
//   // maps from entityId to a set of releaseIds
//   handles: MultiMap<ID, ID> = new MultiMap()

//   output: Subject<MEvent<T>>

//   constructor(
//     entity: string,
//     logger: LoggerService,
//     options: KafkaPersisterOptions,
//     private config: GlobalConfig,
//     private prodConf: ProducerGlobalConfig,
//     private consConf: ConsumerGlobalConfig,
//     server: Server,
//   ) {
//     super(entity, logger, options, server)
//   }

//   persist(event: MEvent<T>): void {
//     const topic = this.makeProducerTopic(event)

//     if (!this.producers.has(topic)) {
//       this.producers.set(
//         topic,
//         new KafkaTopicProducer(
//           topic,
//           {
//             ...this.config,
//             ...this.prodConf,
//           },
//           (msg) =>
//             this.logger
//               .getLogger(`${this.entity}.KafkaTopicProducer`)
//               .dev.log('info', msg),
//         ),
//       )
//     }

//     const producer = this.producers.get(topic)

//     if (!producer) {
//       throw new Error('Producer not found')
//     }

//     producer.publish(this.encodeMsg(event), event.item.id)

//     this.onEvent(event)
//   }

//   listenId(id: string, fromBeginning = false) {
//     const releaseId = uuid()

//     if (this.consumers.has(id)) {
//       this.logger
//         .getLogger(`Kafka.${this.entity}`)
//         .dev.log('warn', `Already listening to ${id}`)
//       return
//     }

//     const topic = `${this.entity}_${id}`
//     const cons = new KafkaTopicConsumer(
//       { ...this.config, ...this.consConf },
//       topic,
//       (msg) => this.onMessage(msg),
//       `${topic}:offset`,
//       `${topic}:partition`,
//     )

//     this.handles.add(id, releaseId)
//     this.consumers.set(id, cons)
//     return releaseId
//   }

//   release(releaseId: string): void {
//     const keyRemoved = this.handles.removeValue(releaseId)

//     if (!keyRemoved) {
//       return
//     }

//     const cons = this.consumers.get(keyRemoved)
//     cons.disconnect()
//     this.consumers.delete(keyRemoved)
//   }

//   makeProducerTopic(event: MEvent<T>): string {
//     return `${this.entity}_${event.item.id}`
//   }
// }
