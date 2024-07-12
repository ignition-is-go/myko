import {
  MEvent,
  MItem,
  MykoLogger,
  Persister,
  PersisterFactory,
  beforeInit,
  fireInit,
  getHostId,
  type ID,
} from '@myko/core'
import { unpack as decode } from 'msgpackr'

import {
  ConsumerConfig,
  Kafka,
  KafkaConfig,
  Message,
  ProducerConfig,
  logLevel,
} from 'kafkajs'
import { Subject } from 'rxjs'
import { KafkaTopicConsumer } from './util/kafka.topicConsumer'
import { KafkaTopicProducer } from './util/kafka.topicProducer'

export type KafkaPersisterOptions = {
  enableEventLog: boolean
  brokers: string[]
}

const optionDefaults: KafkaPersisterOptions = {
  enableEventLog: true,
  brokers: [],
}

export const getPersister: PersisterFactory = <
  KafkaPersisterOptions,
  T extends MItem,
>(
  entity,
  options: KafkaPersisterOptions,
) => {
  const opts = {
    ...optionDefaults,
    ...options,
  }

  if (!entity) {
    throw new Error('entity name cannot be blank')
  }

  const conf = {
    brokers: opts.brokers,
    logLevel: logLevel.NOTHING,
  }

  const prodConf = {}

  const consConf = {
    groupId: getHostId(),
  }

  return new KafkaEntityPersister<T>(
    entity,
    opts,
    conf,
    prodConf,
    consConf,
    new MykoLogger(entity),
    getHostId(),
  )
}

abstract class KafkaPersister<T extends MItem> implements Persister<T> {
  constructor(
    protected entity: string,
    protected options: KafkaPersisterOptions,
    protected serverId: ID,
  ) {
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

  protected onMessage(message: Message) {
    const event = this.decodeMsg(message.value)

    if (!event) {
      return
    }

    if (event.sourceId === this.serverId) {
      return
    }

    this.onEvent(event)
  }

  protected onEvent(event: MEvent<T>) {
    this.output.next(event)
  }

  async init() {}

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
    private config: KafkaConfig,
    private prodConfig: ProducerConfig,
    private consConfig: ConsumerConfig,
    private logger: MykoLogger,
    serverId: ID,
  ) {
    super(entity, options, serverId)
    beforeInit(entity)

    const kafka = new Kafka(this.config)

    const admin = kafka.admin()

    admin.createTopics({
      topics: [
        {
          topic: entity,
          numPartitions: 1,
          replicationFactor: 3,
          configEntries: [
            { name: 'retention.ms', value: '-1' },
            { name: 'cleanup.policy', value: 'compact' },
          ],
        },
      ],
    })

    this.cons = new KafkaTopicConsumer(
      kafka,
      { ...this.config, ...this.consConfig },
      this.entity,
      (msg) => this.onMessage(msg),
      () => {
        this.onInit()
      },
    )

    this.prod = new KafkaTopicProducer(
      kafka,
      this.entity,
      { ...this.config, ...this.prodConfig },
      (msg) => this.logger.info(this.entity, 'KafkaTopicProducer', msg),
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
