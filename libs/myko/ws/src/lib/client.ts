import {
  bufferCount,
  bufferTime,
  catchError,
  combineLatest,
  filter,
  finalize,
  firstValueFrom,
  interval,
  map,
  Observable,
  scan,
  shareReplay,
  Subject,
  switchMap,
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
  type WSMCommandError,
  type WSMCommandResponse,
  type WSMMessage,
  type WSMQuery,
  type WSMQueryResponse,
  type WSMReport,
  type WSMReportResponse,
  type WSPingEvent,
} from './types'
import {
  wrapCommandWS,
  wrapEventWS,
  wrapQueryCancel,
  wrapQueryWS,
  wrapReportCancel,
  wrapReportWS,
} from './wrappers'

import {
  MCommand,
  MQuery,
  MReport,
  MykoProtocol,
  ProtocolMessages,
  unwrapCommand,
  unwrapItem,
  unwrapQuery,
  unwrapReport,
  type ID,
  type MCommandResponse,
  type MEvent,
  type MLiveQueryResult,
  type MLiveReportResult,
  type MReportResult,
  type MWrappedItem,
} from '@myko/core'
import { pack, unpack } from 'msgpackr'
import { v4 } from 'uuid'

type ClientStats = {
  ping: number
  mpsDown: number
  mpsUp: number
}

type WSMClientOpts = {
  secure: boolean
  reconnect: boolean
  maxReconnectAttempts: number
  disableMsgPack: boolean
  preventThrowing: boolean
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

  get errors(): Observable<WSMCommandError> {
    return this.errorsSubject.pipe()
  }

  get successes(): Observable<string> {
    return this.successSubject.pipe()
  }

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
  private connectAttempts = 0
  private reconnectDelay = 1000

  private host: string
  private port: number

  constructor(
    private makeSocket: (url: string) => any,
    private hooks?: {
      onStartConnect?: (url: string) => void
      onConnect?: (url: string) => void
      onDisconnect?: (c: CloseEvent, willAttemptReconnect: boolean) => void
      onError?: (e?: string) => void
      onLog?: (...msg: any[]) => void
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

    this.decoders.set(MykoProtocol.JSON, (data) => JSON.parse(data))
    this.decoders.set(MykoProtocol.MSGPACK, (data) => unpack(data))

    this.encoders.set(MykoProtocol.JSON, (data) => JSON.stringify(data))
    this.encoders.set(MykoProtocol.MSGPACK, (data) => pack(data))

    this.datapreppers.set(MykoProtocol.JSON, (data) => data.toString())
    this.datapreppers.set(MykoProtocol.MSGPACK, (data) => data)

    this.opts = {
      secure: false,
      reconnect: true,
      disableMsgPack: false,
      maxReconnectAttempts: Infinity,
      preventThrowing: false,
      ...opts,
    }
  }

  async ping(): Promise<number> {
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
            this.errorsSubject.next(e)
            if (!this.opts.preventThrowing) throw e
            return
          }
          this.successSubject.next(
            wrapped.data.commandId.split(':').reverse().join(' '),
          )
          return e.data as MCommandResponse<T>
        }),
      ),
    )
  }

  watchQuery<T extends MQuery>(query: T): MLiveQueryResult<T> {
    const wrappedQuery = wrapQueryWS(query)
    this.resendQueries.set(query.tx, wrappedQuery)
    this.send(wrappedQuery)
    return this.queryResponses.pipe(
      filter((r) => r.tx === query.tx),

      scan((acc, update) => {
        if (update.data.sequence === 0) {
          acc.clear()
        }

        if (update.data.deletes.length > 0) {
          update.data.deletes.forEach((d) => {
            acc.delete(d)
          })
        }

        if (update.data.upserts.length > 0) {
          update.data.upserts.forEach((u) => {
            acc.set(u.item.id, u)
          })
        }

        return acc
      }, new Map<ID, MWrappedItem>()),

      map((r) => [...r.values()].map((rr) => unwrapItem(rr))),

      shareReplay(1),
      // clone so downstream slices dont affect the original
      map((x) => x.slice()),

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
      map((x) => {
        // clone the object
        if (x instanceof Array) return x.slice()
        if (x instanceof Object) return { ...x }
        return x
      }),
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

  public connect(host: string, port: number) {
    this.teardownSocket()
    this.host = host
    this.port = port
    this.createSocket()
  }

  private createSocket() {
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

    this.hooks.onLog?.('Connecting to', path)

    this.ws.onopen = () => {
      if (this.protocol !== MykoProtocol.MSGPACK && !this.opts.disableMsgPack) {
        // this.switchToMessagePack()
      }
      this.hooks.onLog?.('Connected to', path)
      this.processQueue()
      this.hooks?.onConnect && this.hooks?.onConnect(path)
      ;[...this.resendQueries.values()].forEach((q) => {
        this.send(q)
      })
      ;[...this.resendReports.values()].forEach((r) => {
        this.send(r)
      })
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
        case MCOMMAND_ERROR_EVENT:
          this.commandResponses.next(message)
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
      const willReconnect =
        this.shouldReconnect &&
        this.opts?.reconnect &&
        this.connectAttempts < this.opts.maxReconnectAttempts

      this.connectAttempts += 1

      this.protocol = MykoProtocol.JSON

      if (e.code === 1002) {
        // this is just cuz the server is not up yet,
        // so will try to reconnect, but not call the
        //disconnect hooks cuz we expect to connect soon
      } else {
        this.hooks?.onDisconnect && this.hooks?.onDisconnect(e, willReconnect)
      }

      this.hooks.onLog?.('Disconnected from', path)

      if (willReconnect) {
        this.hooks.onLog?.(
          `Reconnecting in ${Math.round(this.reconnectDelay / 1000)} sec`,
        )
      }

      setTimeout(() => {
        if (willReconnect) {
          this.createSocket()
        }
      }, this.reconnectDelay)

      this.reconnectDelay *= 1.1
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

  private teardownSocket() {
    if (this.ws) {
      this.hooks?.onDisconnect?.({} as CloseEvent, false)
      this.ws.onclose = () => {}
      this.ws.close()
      this.ws = null
    }
  }

  disconnect() {
    this.shouldReconnect = false
    this.teardownSocket()
  }
}
