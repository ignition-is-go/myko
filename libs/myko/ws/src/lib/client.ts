import {
  filter,
  finalize,
  firstValueFrom,
  map,
  Observable,
  shareReplay,
  Subject,
  tap,
} from 'rxjs'
import {
  MCommand,
  MQuery,
  unwrapCommand,
  unwrapQuery,
  unwrapItem,
  type MEvent,
  type MLiveQueryResult,
  type ID,
  type CommandResponse,
  ProtocolMessages,
  MykoProtocol,
} from '@myko/core'
import {
  wrapCommandWS,
  wrapEventWS,
  wrapQueryCancel,
  wrapQueryWS,
} from './wrappers'
import {
  type WSMMessage,
  MCOMMAND_EVENT,
  MQUERY_EVENT,
  MEVENT_EVENT,
  MQUERY_RESPONSE_EVENT,
  type WSMQueryResponse,
  type WSMCommandResponse,
  MCOMMAND_RESPONSE_EVENT,
  MCOMMAND_ERROR_EVENT,
  type WSMCommandError,
  WSMQuery,
} from './types'

import { Encoder, Decoder } from '@msgpack/msgpack'

export class WSMClient {
  private q = new Set<WSMMessage>()
  private ws: WebSocket | null = null

  get commands(): Observable<MCommand> {
    return this.commandSubject.pipe()
  }

  get queries(): Observable<MQuery> {
    return this.querySubject.pipe()
  }

  get events(): Observable<MEvent> {
    return this.eventSubject.pipe()
  }

  get errors() {
    return this.errorsSubject.pipe()
  }

  get successes() {
    return this.successSubject.pipe()
  }

  private commandSubject: Subject<MCommand>
  private querySubject: Subject<MQuery>
  private eventSubject: Subject<MEvent>
  private queryResponses: Subject<WSMQueryResponse>
  private commandResponses: Subject<WSMCommandResponse | WSMCommandError>
  private errorsSubject: Subject<WSMCommandError>
  private successSubject: Subject<string>
  private userToken: ID | null = null
  private resendQueries: Map<ID, WSMQuery>

  private protocolReady = true
  private protocol: MykoProtocol = MykoProtocol.JSON

  private decoders = new Map<MykoProtocol, (data: any) => any>()
  private encoders = new Map<MykoProtocol, (data: any) => any>()
  private datapreppers = new Map<MykoProtocol, (data: any) => any>()

  constructor(
    private host: string,
    private port: number,
    private clientId: string,
    private makeSocket: (url: string) => any,
    private hooks?: {
      onStartConnect?: (url: string) => void
      onConnect?: (url: string) => void
      onDisconnect?: (c: CloseEvent) => void
      onError?: (e?: string) => void
    },
    private secure: boolean = false,
  ) {
    this.commandSubject = new Subject()
    this.querySubject = new Subject()
    this.eventSubject = new Subject()
    this.queryResponses = new Subject()
    this.commandResponses = new Subject()
    this.errorsSubject = new Subject()
    this.successSubject = new Subject()
    this.resendQueries = new Map()

    const decoder = new Decoder()
    const encoder = new Encoder()
    this.decoders.set(MykoProtocol.JSON, (data) => JSON.parse(data))
    this.decoders.set(MykoProtocol.MSGPACK, (data) => decoder.decode(data))

    this.encoders.set(MykoProtocol.JSON, (data) => JSON.stringify(data))
    this.encoders.set(MykoProtocol.MSGPACK, (data) => encoder.encode(data))

    this.datapreppers.set(MykoProtocol.JSON, (data) => data.toString())
    this.datapreppers.set(MykoProtocol.MSGPACK, (data) => data)

    this.connect()
  }

  sendCommand<T extends MCommand<unknown>>(
    command: T,
  ): Promise<CommandResponse<T>> {
    if (this.userToken) {
      command.userToken = this.userToken
    }
    const wrapped = wrapCommandWS(command, this.clientId)
    this.send(wrapped)
    return firstValueFrom(
      this.commandResponses.pipe(
        filter((c) => c.tx === command.tx),
        map((e) => {
          if (e.event === MCOMMAND_ERROR_EVENT) {
            throw e
          }
          return e.data as CommandResponse<T>
        }),
        tap((x) =>
          this.successSubject.next(
            wrapped.data.commandId.split(':').reverse().join(' '),
          ),
        ),
      ),
    )
  }

  watchQuery<T extends MQuery>(query: T): MLiveQueryResult<T> {
    const wrappedQuery = wrapQueryWS(query, this.clientId)
    this.resendQueries.set(query.tx, wrappedQuery)
    this.send(wrappedQuery)
    return this.queryResponses.pipe(
      filter((r) => r.tx === query.tx),
      map((r) => r.data.map((rr) => unwrapItem(rr))),

      shareReplay(1),
      finalize(() => {
        this.resendQueries.delete(query.tx)
        this.send(wrapQueryCancel(query.tx))
      }),
    ) as MLiveQueryResult<T>
  }

  sendEvent(event: MEvent) {
    this.send(wrapEventWS(event, this.clientId))
  }

  setUser(token: ID) {
    this.userToken = token
  }

  private switchToMessagePack() {
    this.protocolReady = false
    this.forceSend({ event: ProtocolMessages.SwitchToMSGPACK })
  }

  private connect() {
    const prefix = this.secure ? 'wss' : 'ws'
    const port = this.secure ? '' : `:${this.port}`
    const path = `${prefix}://${this.host}${port}/myko`
    this.hooks?.onStartConnect && this.hooks?.onStartConnect(path)
    this.ws = this.makeSocket(path)
    this.ws.binaryType = 'arraybuffer'

    if (!this.ws) {
      this.hooks?.onError && this.hooks?.onError('No Socket')
      return
    }

    this.ws.onopen = () => {
      if (this.protocol !== MykoProtocol.MSGPACK) {
        this.switchToMessagePack()
      }
      this.processQueue()
      this.hooks?.onConnect && this.hooks?.onConnect(path)
      ;[...this.resendQueries.values()].forEach((q) => {
        this.send(q)
      })
    }

    this.ws.onmessage = (e) => {
      if (e.data === ProtocolMessages.SwitchToMSGPACK) {
        this.protocol = MykoProtocol.MSGPACK
        this.protocolReady = true
        this.processQueue()
        return
      }

      const body = e.data

      const message: WSMMessage = this.decoders.get(this.protocol)(
        this.datapreppers.get(this.protocol)(body),
      )

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
        case MQUERY_RESPONSE_EVENT:
          this.queryResponses.next(message)
          break
        case MCOMMAND_RESPONSE_EVENT:
          this.commandResponses.next(message)
          break

        case MCOMMAND_ERROR_EVENT:
          this.commandResponses.next(message)
          this.errorsSubject.next(message)
          break
        default:
          console.warn('no idea what to do with this', message)
      }
    }

    this.ws.onclose = (e) => {
      this.protocol = MykoProtocol.JSON
      this.hooks?.onDisconnect && this.hooks?.onDisconnect(e)
      setTimeout(() => {
        this.connect()
      }, 1000)
    }

    this.ws.onerror = (err) => {
      this.hooks?.onError && this.hooks?.onError()
    }
  }

  private forceSend(e: { event: string; data?: any }) {
    if (!this.ws || this.ws.readyState !== this.ws.OPEN) {
      return
    }

    const encoded = this.encoders.get(this.protocol ?? MykoProtocol.JSON)(e)

    this.ws.send(encoded)
  }

  private send(item: WSMMessage) {
    if (
      !this.ws ||
      this.ws.readyState !== this.ws.OPEN ||
      !this.protocolReady
    ) {
      this.q.add(item)
      return
    }

    const encoded = this.encoders.get(this.protocol ?? MykoProtocol.JSON)(item)

    this.ws.send(encoded)
  }

  private processQueue() {
    ;[...this.q.values()].forEach((v) => {
      this.q.delete(v)
      this.send(v)
    })
  }

  disconnect() {
    if (this.ws) {
      this.hooks?.onDisconnect?.({} as CloseEvent)
      this.ws.onclose = () => {}
      this.ws.close()
    }
  }
}
