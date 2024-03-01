import { MEvent, MYKO_ITEM_TYPE } from '@myko/core'
import { Injectable, OnModuleInit } from '@nestjs/common'
import { QueryResponse, make_watch_id, make_watch } from 'myko-wasm'
import { MItem } from '@myko/core'
import { WebSocket } from 'ws'
import { v4 as uuid } from 'uuid'

@Injectable()
export class MykoBackplaneClient implements OnModuleInit {
  private client: WebSocket

  constructor() {}

  private connectCallbacks = new Set<() => () => void>()
  private disconnectCallbacks = new Set<() => void>()
  private queryCallbacks = new Map<string, (items: MItem[]) => void>()

  onModuleInit() {
    this.connect()
  }

  private async connect() {
    try {
      console.log('CONNECTING')
      this.client = new WebSocket('ws://127.0.0.1:5156', { timeout: 1000 })
      console.log('CONNECTED')
    } catch (e) {
      console.log(e)
      this.client = undefined
      return
    }

    this.client.on('error', (e) => {
      console.error(e)
    })

    this.client.on('open', () => {
      this.connectCallbacks.forEach((cb) => this.disconnectCallbacks.add(cb()))
    })

    this.client.on('close', () => {
      console.log('DISCONNECTED')
      this.disconnectCallbacks.forEach((cb) => cb())
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
        item.forEach((item) => {})
      } catch (e) {
        console.error(e, data.toLocaleString())
      }
    })
  }

  async publishEvent(event: MEvent) {
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
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, type)
    this.send_string(make_watch_id(tx, id, itemType))
    this.queryCallbacks.set(tx, (items) => onUpdate(items.shift() as T))
  }

  public watch<T extends MItem>(
    partial: Partial<T>,
    type: new (args: any) => T,
    onUpdate: (items: T[]) => void,
  ) {
    const tx = uuid()
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, type)
    this.send_string(make_watch(tx, JSON.stringify(partial), itemType))
    this.queryCallbacks.set(tx, (items) => onUpdate(items as T[]))
  }

  public watchAll<T extends MItem>(
    type: new (args: any) => T,
    onUpdate: (items: T[]) => void,
  ) {
    const tx = uuid()
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, type)
    this.send_string(make_watch(tx, '{}', itemType))
    this.queryCallbacks.set(tx, (items) => onUpdate(items as T[]))
  }

  public onConnect(cb: () => () => void) {
    this.connectCallbacks.add(cb)
  }
}
