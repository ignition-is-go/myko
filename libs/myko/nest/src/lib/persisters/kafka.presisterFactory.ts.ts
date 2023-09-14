import { Injectable } from '@nestjs/common'
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
} from '@myko/core'
import { Subject } from 'rxjs'
import { LoggerService } from '@rship/logging'

import { decode, encode } from '@msgpack/msgpack'
import { ConfigService } from '@nestjs/config'
import { KafkaConsumer, Producer, Message, AdminClient } from 'node-rdkafka'
import { v4 as uuid } from 'uuid'
import { MykoQueryBus } from '../busses'

export type KafkaPersisterOptions = {
  enableEventLog: boolean
}

const optionDefaults: Partial<KafkaPersisterOptions> = {
  enableEventLog: true,
}

@Injectable()
export class KafkaPersisterFactory {
  constructor(
    private logger: LoggerService,
    private config: ConfigService,
    private query: MykoQueryBus,
  ) {}

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
      this.getBrokers(),
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
      this.getBrokers(),
    )
  }
}

abstract class KafkaPersister<T extends MItem> implements Persister<T> {
  prod: Producer

  prodConnected = false

  protected sendQueue = []

  constructor(
    protected entity: string,
    protected logger: LoggerService,
    protected options: KafkaPersisterOptions,
    protected brokers: string[],
  ) {
    this.output = new Subject<MEvent<T>>()

    this.prod = new Producer({
      'metadata.broker.list': brokers.join(','),
      stats_cb: (stats) => {
        console.log(stats)
      },
      'statistics.interval.ms': 1000,
      'linger.ms': 10,
      'compression.type': 'zstd',
      dr_cb: true,
    })

    this.init()
  }

  public output: Subject<MEvent<T>>

  persist(event: MEvent<T>): void {
    if (event.itemType !== this.entity) {
      return
    }

    if (!this.prodConnected) {
      this.logger
        .getLogger(this.entity)
        .dev.debug('Queueing Persist due to not connected')
      this.sendQueue.push(event)
      return
    }

    this.send(event)
  }

  abstract makeProducerTopic(event: MEvent<T>): string

  protected onMessage(message: Message) {
    const event = decode(message.value) as MEvent<T>
    this.onEvent(event)
  }

  protected onEvent(event: MEvent<T>) {
    this.output.next(event)
  }

  private send(event: MEvent<T>) {
    const t = this.makeProducerTopic(event)
    if (!t) {
      throw new Error('No Topic')
    }
    const streamKey = t.replace(/[^a-zA-Z0-9\.\-_]/g, '_')

    try {
      this.prod.produce(
        streamKey,
        null,
        Buffer.from(encode(event)),
        event.item.id,
        Date.now(),
      )
    } catch (e) {
      if (e instanceof Error && e.message === 'Local: Queue full') {
      }
    }
  }

  prodHighWaterMarkTimeout: Map<string, NodeJS.Timeout> = new Map()

  private startHighwatermarkCheck(streamKey: string) {
    if (this.prodHighWaterMarkTimeout.has(streamKey)) {
      return
    }
    const timeout = setInterval(() => {
      this.checkProdHighWaterMark(streamKey)
    }, 1000)
    this.prodHighWaterMarkTimeout.set(streamKey, timeout)
  }

  private checkProdHighWaterMark(streamKey: string) {
    this.prod.queryWatermarkOffsets(streamKey, 0, 1000 * 10, (err, offsets) => {
      if (err) {
        console.error(err)
        return
      }

      console.log(offsets)
    })

    this.prod.on('delivery-report', (err, report) => {
      if (err) {
        console.error(err)
        return
      }

      console.log(report)
    })
  }

  async init() {
    this.prod.connect()
    this.prod.on('ready', () => {
      this.prod.setPollInterval(100)
      this.prod.poll()
      this.prodConnected = true
      if (this.sendQueue.length > 0) {
        this.logger.getLogger(this.entity).dev.debug('Sending Queue')
        this.sendQueue.forEach((e) => this.send(e))
        this.sendQueue = []
      }
    })
  }

  protected onInit() {
    fireInit(this.entity)
  }
}

export class KafkaEntityPersister<T extends MItem> extends KafkaPersister<T> {
  cons: KafkaTopicConsumer

  constructor(
    entity: string,
    logger: LoggerService,
    options: KafkaPersisterOptions,
    brokers: string[],
  ) {
    super(entity, logger, options, brokers)
    beforeInit(entity)

    const admin = AdminClient.create({
      'metadata.broker.list': brokers.join(','),
    })

    admin.createTopic({
      topic: this.entity,
      replication_factor: 3,
      num_partitions: 1,
    })

    this.cons = new KafkaTopicConsumer(
      brokers,
      uuid(),
      [this.entity],
      (msg) => this.onMessage(msg),
      () => {
        this.onInit()
      },
    )

    this.init()
  }

  makeProducerTopic(event: MEvent<T>): string {
    return event.itemType
  }
}

class MultiMap<K, V> {
  private reverseMap: Map<V, K> = new Map()
  private map: Map<K, Set<V>> = new Map()

  add(key: K, value: V) {
    if (!this.map.has(key)) {
      this.map.set(key, new Set())
    }

    this.reverseMap.set(value, key)
    this.map.get(key).add(value)
  }

  removeValue(value: V): K | null {
    if (!this.reverseMap.has(value)) {
      return
    }

    const key = this.reverseMap.get(value)
    return this.remove(key, value)
  }

  remove(key: K, value: V): K | null {
    if (!this.map.has(key)) {
      return null
    }

    this.map.get(key).delete(value)

    if (this.map.get(key).size === 0) {
      this.map.delete(key)
      return key
    }
    return null
  }

  get(key: K): Set<V> {
    return this.map.get(key)
  }

  has(key: K): boolean {
    return this.map.has(key)
  }
}

export class KafkaControlledPersister<T extends MItem>
  extends KafkaPersister<T>
  implements ControledPersister<T>
{
  prod: Producer

  prodConnected = false

  consumers: Map<ID, KafkaTopicConsumer> = new Map()
  // maps from entityId to a set of releaseIds
  handles: MultiMap<ID, ID> = new MultiMap()

  output: Subject<MEvent<T>>

  constructor(
    entity: string,
    logger: LoggerService,
    options: KafkaPersisterOptions,
    brokers: string[],
  ) {
    super(entity, logger, options, brokers)
  }

  listenId(id: string, fromBeginning = false) {
    const releaseId = uuid()

    if (this.consumers.has(id)) {
      this.logger
        .getLogger(`Kafka.${this.entity}`)
        .dev.log('warn', `Already listening to ${id}`)
      return
    }

    const topic = makeSafeTopic(`${this.entity}_${id}`)
    const cons = new KafkaTopicConsumer(this.brokers, uuid(), [topic], (msg) =>
      this.onMessage(msg),
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
    return makeSafeTopic(`${this.entity}_${event.item.id}`)
  }
}

const makeSafeTopic = (topic: string) => {
  const safe = topic.replace(/[^a-zA-Z0-9\.\-_]/g, '_')
  return safe
}

class KafkaTopicConsumer {
  cons: KafkaConsumer

  private readOffset: number = 0

  constructor(
    brokers: string[],
    groupId: string,
    private topics: string[],
    onMessage: (buf: Message) => void,
    private onCaughtUp?: () => void,
  ) {
    this.cons = new KafkaConsumer(
      {
        'metadata.broker.list': brokers.join(','),
        'group.id': groupId,
      },
      {
        'auto.offset.reset': 'smallest',
      },
    )

    this.cons
      .on('ready', () => {
        this.cons.subscribe(topics.map(makeSafeTopic))
        this.cons.consume()
        this.checkCaughtUp()
      })
      .on('data', (data) => {
        this.readOffset = data.offset
        onMessage(data)
      })
    this.cons.connect()
  }

  checkCaughtUp() {
    if (this.cons.isConnected() === false) {
      return
    }
    this.cons.queryWatermarkOffsets(this.topics[0], 0, 1000, (err, offsets) => {
      if (err) {
        console.warn(err)
      } else {
        if (this.readOffset >= offsets.highOffset - 1) {
          this.onCaughtUp?.()
        }
      }

      setTimeout(() => this.checkCaughtUp(), 1000)
    })
  }

  disconnect() {
    this.cons.unsubscribe()
    this.cons.disconnect()
  }
}
