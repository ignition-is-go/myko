import {
  MCommand,
  MLiveReportResult,
  MQuery,
  MReport,
  MReportResult,
  MykoProtocol,
  ProtocolMessages,
  SetClientId,
  unwrapCommand,
  unwrapItem,
  unwrapQuery,
  unwrapReport,
  wrapCommand,
  type ID,
  type MCommandResponse,
  type MEvent,
  type MLiveQueryResult,
} from '@myko/core'
import {
  Observable,
  Subject,
  bufferCount,
  bufferTime,
  catchError,
  combineLatest,
  filter,
  finalize,
  firstValueFrom,
  interval,
  map,
  shareReplay,
  switchMap,
  tap,
} from 'rxjs'
import {
  MCOMMAND_ERROR_EVENT,
  MCOMMAND_EVENT,
  MCOMMAND_RESPONSE_EVENT,
  MEVENT_EVENT,
  MPING_EVENT,
  MQUERY_EVENT,
  MQUERY_RESPONSE_EVENT,
  MREPORT_EVENT,
  MREPORT_RESPONSE_EVENT,
  WSMQuery,
  WSMReport,
  WSMReportResponse,
  WSPingEvent,
  type WSMCommandError,
  type WSMCommandResponse,
  type WSMMessage,
  type WSMQueryResponse,
} from './types'
import {
  wrapCommandWS,
  wrapEventWS,
  wrapQueryCancel,
  wrapQueryWS,
  wrapReportCancel,
  wrapReportWS,
} from './wrappers'

import { Decoder, Encoder } from '@msgpack/msgpack'
import { v4 } from 'uuid'

type ClientStats = {
  ping: number
  mpsDown: number
  mpsUp: number
}

type WSMClientOpts = {
  secure: boolean
  reconnect: boolean
  disableMsgPack: boolean
}
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

  get reports(): Observable<MReport<unknown>> {
    return this.reportSubject.pipe()
  }

  get errors() {
    return this.errorsSubject.pipe()
  }

  get successes() {
    return this.successSubject.pipe()
  }

  clientId: ID | null = null

  private commandSubject: Subject<MCommand>
  private querySubject: Subject<MQuery>
  private eventSubject: Subject<MEvent>
  private reportSubject: Subject<MReport<unknown>>
  private queryResponses: Subject<WSMQueryResponse>
  private commandResponses: Subject<WSMCommandResponse | WSMCommandError>
  private reportResponses: Subject<WSMReportResponse>
  private errorsSubject: Subject<WSMCommandError>
  private successSubject: Subject<string>

  private pingSubject: Subject<WSPingEvent>

  private userToken: ID | null = null
  private resendQueries: Map<ID, WSMQuery>
  private resendReports: Map<ID, WSMReport>

  private protocolReady = true
  private protocol: MykoProtocol = MykoProtocol.JSON

  private decoders = new Map<MykoProtocol, (data: any) => any>()
  private encoders = new Map<MykoProtocol, (data: any) => any>()
  private datapreppers = new Map<MykoProtocol, (data: any) => any>()

  private downMsgCounter = new Subject<void>()
  private upMsgCounter = new Subject<void>()

  private opts: WSMClientOpts

  private shouldReconnect = true

  constructor(
    private host: string,
    private port: number,
    private makeSocket: (url: string) => any,
    private hooks?: {
      onClientId?: (clientId: ID) => void
      onStartConnect?: (url: string) => void
      onConnect?: (url: string) => void
      onDisconnect?: (c: CloseEvent) => void
      onError?: (e?: string) => void
    },
    opts?: Partial<WSMClientOpts>,
  ) {
    this.commandSubject = new Subject()
    this.querySubject = new Subject()
    this.eventSubject = new Subject()
    this.reportSubject = new Subject()
    this.queryResponses = new Subject()
    this.commandResponses = new Subject()
    this.reportResponses = new Subject()
    this.errorsSubject = new Subject()
    this.successSubject = new Subject()
    this.pingSubject = new Subject()

    this.resendQueries = new Map()
    this.resendReports = new Map()

    const decoder = new Decoder()
    const encoder = new Encoder()
    this.decoders.set(MykoProtocol.JSON, (data) => JSON.parse(data))
    this.decoders.set(MykoProtocol.MSGPACK, (data) => decoder.decode(data))

    this.encoders.set(MykoProtocol.JSON, (data) => JSON.stringify(data))
    this.encoders.set(MykoProtocol.MSGPACK, (data) => encoder.encode(data))

    this.datapreppers.set(MykoProtocol.JSON, (data) => data.toString())
    this.datapreppers.set(MykoProtocol.MSGPACK, (data) => data)

    this.opts = {
      secure: false,
      reconnect: true,
      disableMsgPack: false,
      ...opts,
    }

    this.connect()
  }

  async ping() {
    const id = v4()
    const timestamp = Date.now()

    if (this.ws.readyState !== this.ws.OPEN) {
      throw new Error('Not Connected')
    }

    this.send({
      event: MPING_EVENT,
      data: {
        timestamp,
        id,
      },
    })

    return firstValueFrom(
      this.pingSubject.pipe(
        filter((p) => p.data.id === id),
        map((p) => Date.now() - p.data.timestamp),
      ),
    )
  }

  sendCommand<T extends MCommand<unknown>>(
    command: T,
  ): Promise<MCommandResponse<T>> {
    if (this.userToken) {
      command.userToken = this.userToken
    }
    const wrapped = wrapCommandWS(command)
    this.send(wrapped)
    return firstValueFrom(
      this.commandResponses.pipe(
        filter((c) => c.tx === command.tx),
        map((e) => {
          if (e.event === MCOMMAND_ERROR_EVENT) {
            throw e
          }
          return e.data as MCommandResponse<T>
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
    const wrappedQuery = wrapQueryWS(query)
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

  watchReport<T extends MReport<any>>(report: T): MLiveReportResult<T> {
    const wrappedReport = wrapReportWS(report)
    this.resendReports.set(report.tx, wrappedReport)
    this.send(wrappedReport)

    return this.reportResponses.pipe(
      filter((r) => r.tx === report.tx),
      map((e) => {
        // check for report errors here?
        return e.data as MReportResult<T>
      }),
      shareReplay(1),
      finalize(() => {
        this.resendReports.delete(report.tx)
        this.send(wrapReportCancel(report.tx))
      }),
    ) as MLiveReportResult<T>
  }

  sendEvent(event: MEvent) {
    this.send(wrapEventWS(event))
  }

  setUser(token: ID) {
    this.userToken = token
  }

  private switchToMessagePack() {
    this.protocolReady = false
    this.forceSend({ event: ProtocolMessages.SwitchToMSGPACK })
  }

  stats(): Observable<ClientStats> {
    const p = interval(1000).pipe(
      switchMap((_) => this.ping()),
      catchError(() => [0]),
    )
    const mpsDown = this.downMsgCounter.pipe(
      bufferTime(100),
      bufferCount(10),
      map((b) => b.flat().length),
    )

    const mpsUp = this.upMsgCounter.pipe(
      bufferTime(100),
      bufferCount(10),
      map((b) => b.flat().length),
    )

    const stats = combineLatest([p, mpsDown, mpsUp]).pipe(
      map(
        ([ping, mpsDown, mpsUp]) =>
          ({ ping, mpsDown, mpsUp }) satisfies ClientStats,
      ),
    )

    return stats
  }

  private connect() {
    const prefix = this.opts.secure ? 'wss' : 'ws'
    const port = this.opts.secure ? '' : `:${this.port}`
    const path = `${prefix}://${this.host}${port}/myko`
    this.hooks?.onStartConnect && this.hooks?.onStartConnect(path)
    this.ws = this.makeSocket(path)
    this.ws.binaryType = 'arraybuffer'

    if (!this.ws) {
      this.hooks?.onError && this.hooks?.onError('No Socket')
      return
    }

    this.ws.onopen = () => {
      if (this.protocol !== MykoProtocol.MSGPACK && !this.opts.disableMsgPack) {
        this.switchToMessagePack()
      }
      this.processQueue()
      this.hooks?.onConnect && this.hooks?.onConnect(path)
    }

    this.ws.onmessage = (e) => {
      this.downMsgCounter.next()

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
          const cmd = unwrapCommand(message.data) as unknown as SetClientId

          if (
            message.data.commandId ===
            wrapCommand(new SetClientId('fake')).commandId
          ) {
            // console.log('got client id', cmd.clientId)
            this.clientId = cmd.clientId
            console.log('Got Client ID', this.clientId)
            this.hooks?.onClientId && this.hooks?.onClientId(this.clientId)
            ;[...this.resendQueries.values()].forEach((q) => {
              this.send(q)
            })
            ;[...this.resendReports.values()].forEach((r) => {
              this.send(r)
            })
            break
          }

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

        case MREPORT_EVENT:
          const report = unwrapReport(message.data)
          this.reportSubject.next(report)
          break

        case MREPORT_RESPONSE_EVENT:
          this.reportResponses.next(message)
          break

        case MPING_EVENT:
          this.pingSubject.next(message)
          break
        default:
          console.warn('no idea what to do with this', message)
      }
    }

    this.ws.onclose = (e) => {
      this.protocol = MykoProtocol.JSON
      this.hooks?.onDisconnect && this.hooks?.onDisconnect(e)

      setTimeout(() => {
        if (this.shouldReconnect && this.opts?.reconnect) {
          this.connect()
        }
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
    this.upMsgCounter.next()
    this.ws.send(encoded)
  }

  private processQueue() {
    ;[...this.q.values()].forEach((v) => {
      this.q.delete(v)
      this.send(v)
    })
  }

  disconnect() {
    this.shouldReconnect = false
    this.hooks?.onDisconnect?.({} as CloseEvent)
    if (this.ws) {
      this.ws.onclose = () => {}
    }
    this.ws?.close()
  }
}
