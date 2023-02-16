import { Observable, Subject } from 'rxjs'
import {
  MCommand,
  MQuery,
  MEvent,
  unwrapCommand,
  unwrapQuery,
} from '@myko/core'
import { wrapCommandWS, wrapEventWS, wrapQueryWS } from './wrappers'
import { WSMMessage, MCOMMAND_EVENT, MQUERY_EVENT, MEVENT_EVENT } from './types'

import * as WebSocket from 'isomorphic-ws'

export class WSMClient {
  private ws: WebSocket

  get commands(): Observable<MCommand> {
    return this.commandSubject.pipe()
  }

  get queries(): Observable<MQuery> {
    return this.querySubject.pipe()
  }

  get events(): Observable<MEvent> {
    return this.eventSubject.pipe()
  }

  private commandSubject: Subject<MCommand>
  private querySubject: Subject<MQuery>
  private eventSubject: Subject<MEvent>

  constructor(
    private host: string,
    private port: number,
    private onReconnect: () => void,
  ) {
    this.commandSubject = new Subject()
    this.querySubject = new Subject()
    this.eventSubject = new Subject()
    this.connect()
  }

  sendCommand(command: MCommand) {
    this.ws.send(JSON.stringify(wrapCommandWS(command)))
  }

  sendQuery(query: MQuery) {
    this.ws.send(JSON.stringify(wrapQueryWS(query)))
  }

  sendEvent(event: MEvent) {
    this.ws.send(JSON.stringify(wrapEventWS(event)))
  }

  connect() {
    this.ws = new WebSocket(`ws://${this.host}:${this.port}`)
    this.ws.onopen = () => {
      console.log('Connected')
      this
      this.onReconnect()
    }

    this.ws.onmessage = (e: WebSocket.MessageEvent) => {
      const body = e.data
      const message: WSMMessage = JSON.parse(body.toString())

      switch (message.event) {
        case MCOMMAND_EVENT:
          const cmd = unwrapCommand(message.data)
          this.commandSubject.next(cmd)
          break

        case MQUERY_EVENT:
          const query = unwrapQuery(message.data)
          this.querySubject.next(query)
          break

        case MEVENT_EVENT:
          const evt = message.data
          this.eventSubject.next(evt)
          break
        default:
          console.warn('no idea what to do with this', message)
      }
    }

    this.ws.onclose = (e) => {
      console.log(
        'Socket Closed: Reconnecting',
        `ws://${this.host}:${this.port}`,
      )
      setTimeout(() => {
        this.connect()
      }, 1000)
    }

    this.ws.onerror = (err) => {
      console.error('An Error Occured')
    }
  }
}
