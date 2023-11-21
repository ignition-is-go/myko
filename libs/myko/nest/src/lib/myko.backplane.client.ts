import { MEvent, MQuery, MWrappedQuery, wrapQuery } from '@myko/core'
import { Injectable, OnModuleInit } from '@nestjs/common'
import { MItem, WatchId } from 'myko-rs'
import { WebSocket } from 'ws'
import { v4 as uuid } from 'uuid'

@Injectable()
export class MykoBackplaneClient implements OnModuleInit {
  private client: WebSocket

  constructor() {}

  private connectCallbacks = new Set<() => void>()

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
    }

    this.client.on('open', () => {
      console.log('connected')
      this.connectCallbacks.forEach((cb) => cb())
    })

    this.client.on('close', () => {
      setTimeout(() => {
        this.connect()
      }, 1000)
    })
  }

  async publishEvent(event: MEvent) {
    if (this.client?.readyState !== WebSocket.OPEN) {
      return
    }
    try {
      this.client.send(JSON.stringify(event))
    } catch (e) {
      console.log(e)
    }
  }

  public watchId<T extends MItem>(id: string, itemType: string) {
    if (this.client?.readyState !== WebSocket.OPEN) {
      return
    }
  }

  public onConnect(cb: () => void) {
    this.connectCallbacks.add(cb)
  }
}
