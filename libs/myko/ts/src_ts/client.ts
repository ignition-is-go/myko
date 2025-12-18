/**
 * Pure TypeScript WebSocket client for Myko servers
 *
 * Maintains connections to all known servers for instant failover.
 * When the current server disconnects, instantly switches to another open connection.
 */

import {
  GetPeerServers,
  MykoEvent,
  type JsonValue,
  type MEvent,
  type MykoMessage,
  type PingData,
  type Server,
  type WrappedItem,
  type WrappedQuery,
  type WrappedReport,
} from '@myko/rs'
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
  of,
  ReplaySubject,
  scan,
  shareReplay,
  Subject,
  Subscription,
  switchMap,
} from 'rxjs'
import { v4 as uuid } from 'uuid'

/** Union type for error event names */
export type MykoErrorEvent =
  | typeof MykoEvent.QueryError
  | typeof MykoEvent.CommandError
  | typeof MykoEvent.ReportError

/** Error types from server */
export type MykoError = {
  event: MykoErrorEvent
  tx: string
  message: string
}

/** Client statistics */
export type ClientStats = {
  ping: number
  mpsDown: number
  mpsUp: number
}

/** Connection status */
export enum ConnectionStatus {
  Connected = 'Connected',
  Disconnected = 'Disconnected',
  Connecting = 'Connecting',
}

/** Query class interface */
export interface Query<T> {
  readonly queryId: string
  readonly queryItemType: string
  readonly query: Record<string, unknown>
  readonly $res?: () => T[]
}

/** Report class interface */
export interface Report<T> {
  readonly reportId: string
  readonly report: Record<string, unknown>
  readonly $res?: () => T
}

/** Command class interface */
export interface Command<T> {
  readonly commandId: string
  readonly command: Record<string, unknown>
  readonly $res?: () => T
}

/** Extract result type from a query */
export type QueryResult<Q> = Q extends Query<infer R> ? R[] : unknown[]

/** Extract item type from a query */
export type QueryItem<Q> = Q extends Query<infer R> ? R : unknown

/** Extract result type from a report */
export type ReportResult<R> = R extends Report<infer T> ? T : unknown

/** Extract result type from a command */
export type CommandResult<C> = C extends Command<infer T> ? T : unknown

/** Diff event for incremental query updates */
export type QueryDiff<T> = {
  sequence: bigint
  deletes: string[]
  upserts: T[]
}

// Message type aliases
type QueryResponseMessage = Extract<MykoMessage, { event: typeof MykoEvent.QueryResponse }>
type ReportResponseMessage = Extract<MykoMessage, { event: typeof MykoEvent.ReportResponse }>
type CommandResponseMessage = Extract<MykoMessage, { event: typeof MykoEvent.CommandResponse }>
type CommandErrorMessage = Extract<MykoMessage, { event: typeof MykoEvent.CommandError }>
type QueryErrorMessage = Extract<MykoMessage, { event: typeof MykoEvent.QueryError }>
type ReportErrorMessage = Extract<MykoMessage, { event: typeof MykoEvent.ReportError }>
type PingMessage = Extract<MykoMessage, { event: typeof MykoEvent.Ping }>

interface ManagedSocket {
  ws: WebSocket
  address: string
}

/**
 * Reactive WebSocket client for Myko servers with automatic failover.
 */
export class MykoClient {
  // Socket management
  private sockets = new Map<string, ManagedSocket>()
  private reconnectTimers = new Map<string, ReturnType<typeof setTimeout>>()
  private currentServer: string | null = null
  private shouldReconnect = true

  // Message routing
  private queryResponses = new Subject<QueryResponseMessage>()
  private reportResponses = new Subject<ReportResponseMessage>()
  private commandResponses = new Subject<CommandResponseMessage>()
  private commandErrors = new Subject<CommandErrorMessage>()
  private queryErrors = new Subject<QueryErrorMessage>()
  private reportErrors = new Subject<ReportErrorMessage>()
  private pingResponses = new Subject<PingMessage>()

  // State observables
  private connectionStatusSubject = new ReplaySubject<ConnectionStatus>(1)
  private currentServerSubject = new ReplaySubject<string | null>(1)

  // Subscription tracking
  private activeQueries = new Map<string, WrappedQuery>()
  private activeReports = new Map<string, WrappedReport>()
  private sharedReports = new Map<string, Observable<unknown>>()
  private messageQueue: MykoMessage[] = []

  // Stats
  private downMsgCounter = new Subject<void>()
  private upMsgCounter = new Subject<void>()

  // Auth & peer discovery
  private userToken: string | null = null
  private peerDiscoveryEnabled = false
  private peerDiscoverySubscription: Subscription | null = null
  private useSecureWebSocket = false

  constructor() {
    this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
    this.currentServerSubject.next(null)
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Connection Management
  // ─────────────────────────────────────────────────────────────────────────────

  /** Set a single server address, clearing any existing connections */
  setAddress(address: string | null): void {
    this.closeAllSockets()
    if (address) {
      this.createSocket(address)
    }
  }

  /** Set multiple server addresses, clearing any existing connections */
  setAddresses(addresses: string[]): void {
    this.closeAllSockets()
    for (const addr of addresses) {
      this.createSocket(addr)
    }
  }

  /** Add additional servers (connects immediately) */
  addServers(addresses: string[]): void {
    for (const addr of addresses) {
      if (!this.hasConnectionTo(addr)) {
        this.createSocket(addr)
      }
    }
  }

  /** Disconnect from all servers */
  disconnect(): void {
    this.shouldReconnect = false
    this.stopPeerDiscovery()
    this.closeAllSockets()
  }

  /** Get the currently active server address */
  getCurrentServer(): string | null {
    return this.currentServer
  }

  /** Get all server addresses */
  getServers(): string[] {
    return Array.from(this.sockets.keys())
  }

  /** Get addresses of all open connections */
  getOpenServers(): string[] {
    return Array.from(this.sockets.entries())
      .filter(([, m]) => m.ws.readyState === WebSocket.OPEN)
      .map(([addr]) => addr)
  }

  /** Observable of current server changes */
  get currentServer$(): Observable<string | null> {
    return this.currentServerSubject.asObservable()
  }

  /** Get current connection status */
  getConnectionStatus(): ConnectionStatus {
    if (this.currentServer) return ConnectionStatus.Connected
    for (const m of this.sockets.values()) {
      if (m.ws.readyState === WebSocket.CONNECTING) return ConnectionStatus.Connecting
    }
    return ConnectionStatus.Disconnected
  }

  /** Observable of connection status changes */
  get connectionStatus$(): Observable<ConnectionStatus> {
    return this.connectionStatusSubject.asObservable()
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Peer Discovery
  // ─────────────────────────────────────────────────────────────────────────────

  /** Enable automatic peer discovery via GetPeerServers query */
  enablePeerDiscovery(enabled: boolean, secure = false): void {
    this.peerDiscoveryEnabled = enabled
    this.useSecureWebSocket = secure

    if (!enabled && this.peerDiscoverySubscription) {
      this.peerDiscoverySubscription.unsubscribe()
      this.peerDiscoverySubscription = null
    } else if (enabled && this.hasOpenConnection()) {
      this.startPeerDiscovery()
    }
  }

  private startPeerDiscovery(): void {
    this.peerDiscoverySubscription?.unsubscribe()
    this.peerDiscoverySubscription = this.watchQuery(new GetPeerServers({})).subscribe(
      (servers: Server[]) => {
        const addresses = servers.map((s) =>
          this.useSecureWebSocket
            ? `wss://${s.address}/myko`
            : `ws://${s.address}:${s.port}/myko`,
        )
        if (addresses.length > 0) {
          this.addServers(addresses)
        }
      },
    )
  }

  private stopPeerDiscovery(): void {
    this.peerDiscoverySubscription?.unsubscribe()
    this.peerDiscoverySubscription = null
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Auth & Stats
  // ─────────────────────────────────────────────────────────────────────────────

  /** Set authentication token for commands */
  setToken(token: string | null): void {
    this.userToken = token
  }

  /** Observable of all errors */
  get errors$(): Observable<MykoError> {
    const toError = <T extends { event: MykoErrorEvent; data: { tx: string; message: string } }>(
      e: T,
    ): MykoError => ({ event: e.event, tx: e.data.tx, message: e.data.message })

    return merge(
      this.queryErrors.pipe(map(toError)),
      this.commandErrors.pipe(map(toError)),
      this.reportErrors.pipe(map(toError)),
    )
  }

  /** Observable of successful command completions (tx id) */
  get successes$(): Observable<string> {
    return this.commandResponses.pipe(map((r) => r.data.tx))
  }

  /** Measure round-trip latency */
  async ping(): Promise<number> {
    const id = uuid()
    const timestamp = BigInt(Date.now())

    this.send({
      event: MykoEvent.Ping,
      data: { id, timestamp } satisfies PingData,
    } as MykoMessage)

    return firstValueFrom(
      this.pingResponses.pipe(
        filter((p) => p.data.id === id),
        map((p) => Date.now() - Number(p.data.timestamp)),
      ),
    )
  }

  /** Get real-time client statistics (emits every second) */
  stats(): Observable<ClientStats> {
    const pingLatency = interval(1000).pipe(
      switchMap(() => this.ping()),
      catchError(() => of(0)),
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
    return combineLatest([pingLatency, mpsDown, mpsUp]).pipe(
      map(([ping, down, up]) => ({ ping, mpsDown: down, mpsUp: up })),
    )
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Queries & Reports
  // ─────────────────────────────────────────────────────────────────────────────

  /** Start a query subscription, returns [tx, responses$] */
  private startQuery<Q extends Query<unknown>>(
    query: Q,
  ): [string, Observable<QueryResponseMessage>] {
    const tx = uuid()
    const wrappedQuery: WrappedQuery = {
      query: { ...query.query, tx, createdAt: new Date().toISOString() },
      queryId: query.queryId,
      queryItemType: query.queryItemType,
    }

    this.activeQueries.set(tx, wrappedQuery)
    this.send({ event: MykoEvent.Query, data: wrappedQuery })

    const responses$ = this.queryResponses.pipe(
      filter((r) => r.data.tx === tx),
      finalize(() => {
        this.activeQueries.delete(tx)
        this.send({ event: MykoEvent.QueryCancel, data: { tx } })
      }),
    )

    return [tx, responses$]
  }

  /** Watch a query and receive live updates */
  watchQuery<Q extends Query<unknown>>(query: Q): Observable<QueryResult<Q>> {
    const [, responses$] = this.startQuery(query)

    return responses$.pipe(
      scan((acc, update) => {
        if (BigInt(update.data.sequence) === 0n) acc.clear()
        for (const id of update.data.deletes) acc.delete(id)
        for (const wrapped of update.data.upserts) {
          const item = wrapped.item as { id: string }
          if (item?.id) acc.set(item.id, wrapped)
        }
        return acc
      }, new Map<string, WrappedItem<unknown>>()),
      map((items) => [...items.values()].map((w) => w.item) as QueryResult<Q>),
      shareReplay({ bufferSize: 1, refCount: true }),
    )
  }

  /** Watch a query and receive raw diff events */
  watchQueryDiff<Q extends Query<unknown>>(query: Q): Observable<QueryDiff<QueryItem<Q>>> {
    const [, responses$] = this.startQuery(query)

    return responses$.pipe(
      map((r) => ({
        sequence: BigInt(r.data.sequence),
        deletes: r.data.deletes,
        upserts: r.data.upserts.map((w: WrappedItem<JsonValue>) => w.item) as QueryItem<Q>[],
      })),
    )
  }

  /** Watch a report with automatic deduplication */
  watchReport<R extends Report<unknown>>(report: R): Observable<ReportResult<R>> {
    const cacheKey = `report:${report.reportId}:${JSON.stringify(report.report)}`

    const existing = this.sharedReports.get(cacheKey)
    if (existing) return existing as Observable<ReportResult<R>>

    const tx = uuid()
    const wrappedReport: WrappedReport = {
      report: { ...report.report, tx },
      reportId: report.reportId,
    }

    this.activeReports.set(tx, wrappedReport)
    this.send({ event: MykoEvent.Report, data: wrappedReport })

    const shared$ = this.reportResponses.pipe(
      filter((r) => r.data.tx === tx),
      map((r) => r.data.response as ReportResult<R>),
      finalize(() => {
        this.sharedReports.delete(cacheKey)
        this.activeReports.delete(tx)
        this.send({ event: MykoEvent.ReportCancel, data: { tx } })
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
    )

    this.sharedReports.set(cacheKey, shared$)
    return shared$
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Commands & Events
  // ─────────────────────────────────────────────────────────────────────────────

  /** Send an event to the server */
  sendEvent(event: MEvent): void {
    this.send({ event: MykoEvent.Event, data: event })
  }

  /** Send a command and wait for response */
  sendCommand<C extends Command<unknown>>(command: C): Promise<CommandResult<C>> {
    const tx = uuid()

    const wrappedCommand = {
      command: {
        ...command.command,
        tx,
        createdAt: new Date().toISOString(),
        ...(this.userToken && { userToken: this.userToken }),
      },
      commandId: command.commandId,
    }

    return new Promise<CommandResult<C>>((resolve, reject) => {
      const responseSub = this.commandResponses
        .pipe(filter((r) => r.data.tx === tx))
        .subscribe((r) => {
          cleanup()
          resolve(r.data.response as CommandResult<C>)
        })

      const errorSub = this.commandErrors
        .pipe(filter((r) => r.data.tx === tx))
        .subscribe((r) => {
          cleanup()
          reject(new Error(r.data.message))
        })

      const cleanup = () => {
        responseSub.unsubscribe()
        errorSub.unsubscribe()
      }

      this.send({ event: MykoEvent.Command, data: wrappedCommand })
    })
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Private: Socket Management
  // ─────────────────────────────────────────────────────────────────────────────

  private getFirstOpenSocket(): ManagedSocket | null {
    for (const m of this.sockets.values()) {
      if (m.ws.readyState === WebSocket.OPEN) return m
    }
    return null
  }

  private hasOpenConnection(): boolean {
    return this.getFirstOpenSocket() !== null
  }

  private hasConnectionTo(address: string): boolean {
    if (this.sockets.has(address)) return true

    // Check by port (handles localhost vs 127.0.0.1)
    const parsed = this.parseAddress(address)
    if (!parsed) return false

    for (const existing of this.sockets.keys()) {
      const existingParsed = this.parseAddress(existing)
      if (existingParsed?.port === parsed.port) return true
    }
    return false
  }

  private parseAddress(address: string): { host: string; port: number } | null {
    try {
      const url = new URL(address)
      const port = url.port ? parseInt(url.port, 10) : url.protocol === 'wss:' ? 443 : 80
      return { host: url.hostname, port }
    } catch {
      return null
    }
  }

  private closeAllSockets(): void {
    for (const timer of this.reconnectTimers.values()) clearTimeout(timer)
    this.reconnectTimers.clear()

    for (const m of this.sockets.values()) {
      m.ws.onclose = null
      m.ws.onerror = null
      m.ws.onopen = null
      m.ws.onmessage = null
      m.ws.close()
    }
    this.sockets.clear()
    this.currentServer = null
    this.currentServerSubject.next(null)
    this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
  }

  private createSocket(address: string): void {
    if (this.sockets.has(address)) return

    const ws = new WebSocket(address)
    const managed: ManagedSocket = { ws, address }
    this.sockets.set(address, managed)

    if (this.sockets.size === 1) {
      this.connectionStatusSubject.next(ConnectionStatus.Connecting)
    }

    ws.onopen = () => {
      if (this.sockets.get(address) !== managed) {
        ws.close()
        return
      }

      if (!this.currentServer) {
        this.currentServer = address
        this.currentServerSubject.next(address)
        this.connectionStatusSubject.next(ConnectionStatus.Connected)
        this.flushQueue()
        this.resendSubscriptions()
        if (this.peerDiscoveryEnabled) this.startPeerDiscovery()
      }
    }

    ws.onclose = () => {
      if (this.sockets.get(address) !== managed) return

      this.sockets.delete(address)

      if (this.currentServer === address) {
        const next = this.getFirstOpenSocket()
        if (next) {
          this.currentServer = next.address
          this.currentServerSubject.next(next.address)
          this.resendSubscriptions()
          return // Peer discovery will re-add this server when it's back
        }
        this.currentServer = null
        this.currentServerSubject.next(null)
        this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
      }

      // Only retry if completely disconnected
      if (this.shouldReconnect && !this.hasOpenConnection()) {
        this.scheduleReconnect(address)
      }
    }

    ws.onerror = () => {}

    ws.onmessage = (event) => {
      if (this.sockets.get(address) !== managed) return
      this.onMessage(event.data)
    }
  }

  private scheduleReconnect(address: string): void {
    if (this.reconnectTimers.has(address)) return

    const timer = setTimeout(() => {
      this.reconnectTimers.delete(address)
      if (this.shouldReconnect && !this.hasOpenConnection()) {
        this.createSocket(address)
      }
    }, 1000)

    this.reconnectTimers.set(address, timer)
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Private: Message Handling
  // ─────────────────────────────────────────────────────────────────────────────

  private onMessage(data: string | ArrayBuffer | Blob): void {
    if (typeof data !== 'string') return
    this.downMsgCounter.next()

    try {
      const message = JSON.parse(data) as MykoMessage
      switch (message.event) {
        case MykoEvent.QueryResponse:
          this.queryResponses.next(message)
          break
        case MykoEvent.ReportResponse:
          this.reportResponses.next(message)
          break
        case MykoEvent.CommandResponse:
          this.commandResponses.next(message as CommandResponseMessage)
          break
        case MykoEvent.CommandError:
          this.commandErrors.next(message as CommandErrorMessage)
          break
        case MykoEvent.QueryError:
          this.queryErrors.next(message as QueryErrorMessage)
          break
        case MykoEvent.ReportError:
          this.reportErrors.next(message as ReportErrorMessage)
          break
        case MykoEvent.Ping:
          this.pingResponses.next(message as PingMessage)
          break
      }
    } catch {
      // Ignore parse errors
    }
  }

  private send(message: MykoMessage): void {
    if (this.currentServer) {
      const managed = this.sockets.get(this.currentServer)
      if (managed?.ws.readyState === WebSocket.OPEN) {
        managed.ws.send(JSON.stringify(message))
        this.upMsgCounter.next()
        return
      }
    }
    this.messageQueue.push(message)
  }

  private flushQueue(): void {
    const queue = this.messageQueue
    this.messageQueue = []
    for (const msg of queue) this.send(msg)
  }

  private resendSubscriptions(): void {
    for (const q of this.activeQueries.values()) {
      this.send({ event: MykoEvent.Query, data: q })
    }
    for (const r of this.activeReports.values()) {
      this.send({ event: MykoEvent.Report, data: r })
    }
  }
}
