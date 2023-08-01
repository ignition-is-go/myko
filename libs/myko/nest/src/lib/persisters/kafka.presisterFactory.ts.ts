import { Injectable } from '@nestjs/common'
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
import { LoggerService } from '@rship/logging'

import { decode, encode } from '@msgpack/msgpack'
import { ConfigService } from '@nestjs/config'
import { CompressionTypes, Consumer, Kafka, Producer } from 'kafkajs'
import { v4 as uuid } from 'uuid'

export type KafkaPersisterOptions = {
  enableEventLog: boolean
  autoGetAllNew: boolean
  serverId: string
}

const optionDefaults: Partial<KafkaPersisterOptions> = {
  enableEventLog: true,
  autoGetAllNew: true,
}

@Injectable()
export class KafkaPersisterFactory {
  constructor(private logger: LoggerService, private config: ConfigService) {}
  getPersister<T extends MItem>(
    ent: MItemConstructor<T>,
    options?: KafkaPersisterOptions,
  ) {
    const entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    if (!entity) {
      throw new Error('Cannot get Entity from Metadata')
    }

    const client = new Kafka({
      clientId: 'myko',
      brokers: ['localhost:9092'],
    })

    const admin = client.admin()

    return new KafkaStreamPersister<T>(client, entity, this.logger, {
      ...optionDefaults,
      ...options,
    })
  }
}

export class KafkaStreamPersister<T extends MItem> implements Persister<T> {
  streamHandles = new Map<ID, TopicListener<MEvent<T>>>()

  private newItemsKey: string

  private newItemsListener: TopicListener<ID[]>

  output: Subject<MEvent<T>>
  constructor(
    private kafka: Kafka,
    private entity: string,
    private logger: LoggerService,
    private readonly options: KafkaPersisterOptions,
  ) {
    this.output = new Subject<MEvent<T>>()

    if (this.options.enableEventLog) {
      getEvents.set(this.entity, this.getEvents.bind(this))
    }

    if (this.options.autoGetAllNew) {
      this.newItemsKey = `${this.entity}_new`
      this.newItemsListener = new TopicListener<ID[]>(
        this.newItemsKey,
        this.kafka,
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
    const streamKey = `${this.entity}:${itemId}`.replace(':', '_')

    return new Promise((resolve) => {
      if (!this.streamHandles.has(itemId)) {
        this.streamHandles.set(
          itemId,
          new TopicListener<MEvent<T>>(
            streamKey,
            this.kafka,
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
    // const keys = await this.kafka.keys(`${this.entity}:*`)
    // this.logger.getLogger('Init').dev.info(`${this.entity} Repo Initializing`)

    // const itemIds = keys.map((key) => key.replace(`${this.entity}:`, ''))

    // await Promise.all(itemIds.map((itemId) => this.assertHandler(itemId)))

    fireInit(this.entity)
  }
}

class TopicListener<U> {
  constructor(
    private streamKey: string,
    private kafka: Kafka,
    private onEvent: (event: U) => any,
    private makeId: (item: U) => string,
    private onInit: () => void,
  ) {
    this.prod = this.kafka.producer({ allowAutoTopicCreation: true })
    this.cons = this.kafka.consumer({ groupId: uuid() })

    this.listen()
  }

  prod: Producer
  cons: Consumer

  consConnected = false
  prodConnected = false

  lastId: string | undefined

  async prime() {
    console.log('Priming')
    await this.cons.connect()
    await this.cons.subscribe({ topic: this.streamKey, fromBeginning: true })

    await this.cons.run({
      eachMessage: async ({ message }) => {
        try {
          const event = decode(message.value) as U
          this.onEvent(event)
        } catch (e) {
          console.warn(e)
        }
      },
    })

    this.onInit()
  }

  async listen() {
    await this.prime()
  }

  persist(event: U) {
    if (!this.prodConnected) {
      this.prod.connect().then(() => {
        console.log('Producer Connected')
        this.prodConnected = true
        this.persist(event)
      })

      return
    }
    this.prod.send({
      messages: [
        {
          value: Buffer.from(encode(event)),
        },
      ],
      topic: this.streamKey,
    })
  }
}
