import { MEvent, MYKO_ITEM_TYPE } from '@myko/core'
import { Injectable, OnModuleInit } from '@nestjs/common'
import { QueryResponse, make_watch_id } from 'myko-rs'
import { MItem } from '@myko/core'
import { WebSocket } from 'ws'
import { v4 as uuid } from 'uuid'

@Injectable()
export class MykoBackplaneClient implements OnModuleInit {
  private client: WebSocket

  constructor() {}

  private connectCallbacks = new Set<() => void>()
  private queryCallbacks = new Map<string, (items: MItem[]) => void>()

  onModuleInit() {
    this.connect()
  }

  private async connect() {
    try {
      this.client = new WebSocket('ws://127.0.0.1:5156')
    } catch (e) {
      console.log(e)
      setTimeout(() => {
        this.connect()
      }, 1000)
      return
    }

    this.client.on('error', (e) => {
      console.error(e)
    })

    this.client.on('open', () => {
      this.connectCallbacks.forEach((cb) => cb())
    })

    this.client.on('close', () => {
      setTimeout(() => {
        this.connect()
      }, 1000)
    })

    this.client.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString()) as QueryResponse

        const cb = this.queryCallbacks.get(msg.tx)

        if (!cb) {
          console.warn('NO CALLBACK FOUND')
          return
        }
        const item = msg.result as unknown as MItem[]
        cb(item.slice())
        item.forEach((item) => {
          console.timeEnd(item.hash)
        })
      } catch (e) {
        console.error(e, data.toLocaleString())
      }
    })
  }

  async publishEvent(event: MEvent) {
    console.time(event.item.hash)
    this.send(event)
  }

  private async send(any: Record<string, any>) {
    this.send_string(JSON.stringify(any))
  }

  private send_string(str: string) {
    if (this.client?.readyState !== WebSocket.OPEN) {
      console.warn('NOT OPEN YET')
      return
    }

    try {
      this.client.send(str)
    } catch (e) {
      console.log('>>>', e)
    }
  }

  public watchId<T extends MItem>(
    id: string,
    type: new (args: any) => T,
    onUpdate: (items: T) => void,
  ) {
    const tx = uuid()
    console.time(tx)
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, type)
    this.send_string(make_watch_id(tx, id, itemType))
    this.queryCallbacks.set(tx, (items) => onUpdate(items.shift() as T))
  }

  public onConnect(cb: () => void) {
    this.connectCallbacks.add(cb)
  }
}
