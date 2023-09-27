import { Producer, AdminClient } from 'node-rdkafka'
import { makeSafeTopic } from './helpers'

export class KafkaTopicProducer {
  prod: Producer

  prodConnected = false

  protected sendQueue: Map<string, Buffer> = new Map()

  constructor(
    private topic: string,
    protected brokers: string[],
    private log: (msg: string) => void,
  ) {
    const admin = AdminClient.create({
      'metadata.broker.list': brokers.join(','),
    })

    admin.createTopic({
      num_partitions: 1,
      replication_factor: 3,
      topic: topic,
    })

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

    this.prod.connect()
    this.prod.on('ready', () => {
      this.log('Producer is ready')
      this.prod.setPollInterval(100)
      this.prod.poll()
      this.prodConnected = true
      if (this.sendQueue.size > 0) {
        log(`Sending ${this.sendQueue.size} queued messages`)
        ;[...this.sendQueue.entries()].forEach(([key, buf]) =>
          this.send(buf, key),
        )
        this.sendQueue.clear()
      }
    })
  }

  public publish(msg: Buffer, key: string) {
    if (!this.prodConnected) {
      this.log(`Caching Persist due to not connected`)
      this.sendQueue.set(key, msg)
      return
    }

    this.send(msg, key)
  }

  private send(msg: Buffer, key: string) {
    const streamKey = makeSafeTopic(this.topic)

    try {
      this.prod.produce(streamKey, null, msg, key, Date.now())
    } catch (e) {
      if (e instanceof Error && e.message === 'Local: Queue full') {
      }
    }
  }

  // private startHighwatermarkCheck(streamKey: string) {
  //   if (this.prodHighWaterMarkTimeout.has(streamKey)) {
  //     return
  //   }
  //   const timeout = setInterval(() => {
  //     this.checkProdHighWaterMark(streamKey)
  //   }, 1000)
  //   this.prodHighWaterMarkTimeout.set(streamKey, timeout)
  // }

  // private checkProdHighWaterMark(streamKey: string) {
  //   this.prod.queryWatermarkOffsets(streamKey, 0, 1000 * 10, (err, offsets) => {
  //     if (err) {
  //       console.error(err)
  //       return
  //     }

  //     console.log(offsets)
  //   })

  //   this.prod.on('delivery-report', (err, report) => {
  //     if (err) {
  //       console.error(err)
  //       return
  //     }

  //     console.log(report)
  //   })
  // }
}
