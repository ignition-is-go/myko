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
  merge,
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
  MQUERY_ERROR_EVENT,
  MQUERY_EVENT,
  MQUERY_RESPONSE_EVENT,
  MREPORT_ERROR_EVENT,
  MREPORT_EVENT,
  MREPORT_RESPONSE_EVENT,
  type WSMCommand,
  type WSMCommandError,
  type WSMCommandResponse,
  type WSMMessage,
  type WSMQuery,
  type WSMQueryError,
  type WSMQueryResponse,
  type WSMReport,
  type WSMReportError,
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
  GetPeerServers,
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
import { DateTime } from 'luxon'
import { pack, unpack } from 'msgpackr'
import { v4 } from 'uuid'
import { SocketGroup, SocketSendMode } from './socket.group'

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
  singleSocket: boolean
}

type CommandCompletion = {
  command: WSMCommand
  startTime: string
  completeTime?: string
  errorTime?: string
  error?: WSMCommandError
}
export class WSMClient {
  private q = new Set<WSMMessage>()

  private socketGroup: SocketGroup

  // private ws: ReconnectSocket

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

  get errors(): Observable<WSMQueryError | WSMCommandError | WSMReportError> {
    return this.errorSubject.pipe()
  }

  get successes(): Observable<string> {
    return this.successSubject.pipe()
  }

  private commandSubject: Subject<MCommand>
  private querySubject: Subject<MQuery>
  private eventSubject: Subject<MEvent>
  private reportSubject: Subject<MReport<unknown>>
  private queryResponses: Subject<WSMQueryResponse>
  private commandResponses: Subject<WSMCommandResponse>
  private reportResponses: Subject<WSMReportResponse>

  private errorSubject: Subject<
    WSMQueryError | WSMCommandError | WSMReportError
  >
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

  private clientOpts: WSMClientOpts

  private pendingCommands = new Map<ID, CommandCompletion>()

  private pendingCommmandSubject = new Subject<CommandCompletion[]>()

  constructor(
    private makeSocket: (url: string) => any,
    private hooks?: {
      onServerConnect?: (url: string) => void
      onStartConnect?: (url: string) => void
      onTerminated?: () => void
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
    this.errorSubject = new Subject()
    this.successSubject = new Subject()
    this.pingSubject = new Subject()

    this.pendingCommands = new Map()
    this.pendingCommmandSubject = new Subject()

    this.resendQueries = new Map()
    this.resendReports = new Map()

    this.decoders.set(MykoProtocol.JSON, (data) => JSON.parse(data))
    this.decoders.set(MykoProtocol.MSGPACK, (data) => unpack(data))

    this.encoders.set(MykoProtocol.JSON, (data) => JSON.stringify(data))
    this.encoders.set(MykoProtocol.MSGPACK, (data) => pack(data))

    this.datapreppers.set(MykoProtocol.JSON, (data) => data.toString())
    this.datapreppers.set(MykoProtocol.MSGPACK, (data) => data)

    this.clientOpts = {
      secure: false,
      reconnect: true,
      disableMsgPack: false,
      maxReconnectAttempts: Infinity,
      preventThrowing: false,
      singleSocket: false,
      ...opts,
    }

    this.socketGroup = new SocketGroup(
      this.makeSocket,

      {
        onClosed: () => {
          this.hooks.onTerminated()
        },
        onConnected: (url) => {
          this.onServerConnect(url)
        },
        onError: (error) => {
          console.warn(error)
        },
        onLog: (...log) => {
          this.hooks?.onLog?.(...log)
        },
        onMessage: (data) => {
          this.onMessage(data)
        },
        onMainServerChange: (url) => {
          this.onServerConnect(url)
        },
        onMainSocketReconnecting: (url) => {
          this.hooks?.onLog?.('Reconnecting to', url)
        },
        socketSendMode: SocketSendMode.Single,
        reconnect: this.clientOpts.reconnect,
        secure: this.clientOpts.secure,
      },
    )

    const servers = this.watchQuery(new GetPeerServers())

    if (this.clientOpts.singleSocket) {
      return
    }

    this.hooks.onLog?.('Watching for Additional servers')
    servers.subscribe((s) => {
      this.socketGroup.addServers(
        s.map((s) => ({ host: s.address, port: s.port })),
      )
    })
  }

  async ping(): Promise<number> {
    const id = v4()
    const timestamp = Date.now()

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

  private setCommandPending(command: WSMCommand) {
    this.pendingCommands.set(command.data.command.tx, {
      command,
      startTime: DateTime.utc().toISO(),
    })
    this.pendingCommmandSubject.next([...this.pendingCommands.values()])
  }

  private setCommandComplete(command: WSMCommand) {
    const cmd = this.pendingCommands.get(command.data.command.tx)
    if (cmd) {
      cmd.completeTime = DateTime.utc().toISO()
    }
    this.pendingCommmandSubject.next([...this.pendingCommands.values()])
    setTimeout(() => {
      this.clearCommandCompletion(command.data.command.tx)
    }, 1000)
  }

  private setCommandError(command: WSMCommand, error: WSMCommandError) {
    const cmd = this.pendingCommands.get(command.data.command.tx)
    if (cmd) {
      cmd.error = error
      cmd.errorTime = DateTime.utc().toISO()
    }
    this.pendingCommmandSubject.next([...this.pendingCommands.values()])
  }

  clearCommandCompletion(tx: ID) {
    this.pendingCommands.delete(tx)
    this.pendingCommmandSubject.next([...this.pendingCommands.values()])
  }

  sendCommand<T extends MCommand<unknown>>(
    command: T,
  ): Promise<MCommandResponse<T>> {
    if (this.userToken) {
      command.userToken = this.userToken
    }

    const wrapped = wrapCommandWS(command)
    this.setCommandPending(wrapped)
    this.send(wrapped)
    return firstValueFrom(
      merge(
        this.commandResponses,
        this.errorSubject.pipe(filter((x) => x.event === MCOMMAND_ERROR_EVENT)),
      ).pipe(
        filter(
          (c) =>
            (c.event === MCOMMAND_ERROR_EVENT ||
              c.event === MCOMMAND_RESPONSE_EVENT) &&
            c.data.tx === command.tx,
        ),
        map((e) => {
          if (e.event === MCOMMAND_ERROR_EVENT) {
            this.setCommandError(wrapped, e)
            if (!this.clientOpts.preventThrowing) throw e
            return
          }
          this.successSubject.next(
            wrapped.data.commandId.split(':').reverse().join(' '),
          )
          this.setCommandComplete(wrapped)
          return e.data.response as MCommandResponse<T>
        }),
      ),
    )
  }

  watchCommandStatus(): Observable<CommandCompletion[]> {
    return this.pendingCommmandSubject.asObservable()
  }

  watchQuery<T extends MQuery>(query: T): MLiveQueryResult<T> {
    const wrappedQuery = wrapQueryWS(query)
    this.resendQueries.set(query.tx, wrappedQuery)
    this.send(wrappedQuery)
    return this.queryResponses.pipe(
      filter((r) => r.data.tx === query.tx),

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
      filter((r) => r.data.tx === report.tx),
      map((e) => {
        // check for report errors here?
        return e.data.response as MReportResult<T>
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

  // private switchToMessagePack() {
  //   this.protocolReady = false
  //   this.forceSend({ event: ProtocolMessages.SwitchToMSGPACK })
  // }

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
    this.socketGroup.bootstrap(host, port)
  }

  private onServerConnect(path: string) {
    if (
      this.protocol !== MykoProtocol.MSGPACK &&
      !this.clientOpts.disableMsgPack
    ) {
      // this.switchToMessagePack()
    }
    this.hooks.onLog?.('Connected to', path)
    this.hooks?.onServerConnect && this.hooks?.onServerConnect(path)
    this.processQueue()
    ;[...this.resendQueries.values()].forEach((q) => {
      this.send(q)
    })
    ;[...this.resendReports.values()].forEach((r) => {
      this.send(r)
    })
  }

  private onMessage(e: MessageEvent) {
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

      case MQUERY_ERROR_EVENT:
        this.errorSubject.next(message)
        break
      case MQUERY_RESPONSE_EVENT:
        this.queryResponses.next(message)
        break

      case MCOMMAND_ERROR_EVENT:
        this.errorSubject.next(message)
        break

      case MCOMMAND_RESPONSE_EVENT:
        this.commandResponses.next(message)
        break

      case MREPORT_EVENT:
        const report = unwrapReport(message.data)
        this.reportSubject.next(report)
        break

      case MREPORT_ERROR_EVENT:
        this.errorSubject.next(message)
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

  // private forceSend(e: { event: string; data?: any }) {
  //   if (!this.socketGroup.ready) {
  //     return
  //   }
  //   const encoded = this.encoders.get(this.protocol ?? MykoProtocol.JSON)(e)

  //   this.socketGroup.send(encoded)
  // }

  private send(item: WSMMessage) {
    if (!this.socketGroup.ready || !this.protocolReady) {
      this.q.add(item)
      return
    }

    const encoded = this.encoders.get(this.protocol ?? MykoProtocol.JSON)(item)
    this.upMsgCounter.next()
    this.socketGroup.send(encoded)
  }

  private processQueue() {
    ;[...this.q.values()].forEach((v) => {
      this.q.delete(v)
      this.send(v)
    })
  }

  private teardown() {
    this.socketGroup.teardown()
  }

  disconnect() {
    this.teardown()
  }
}
