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
  type WrappedView,
} from './generated'
import { Decoder, Encoder } from 'cbor-x'
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

// CBOR encodes `undefined` as a distinct major-type-7 simple value by default, but our Rust
// server deserializes binary frames into `serde_json::Value`, which has no `undefined`. CBOR's
// natural mapping for absent values is `null`, and cbor-x's defaults treat `undefined` as null
// when encoding plain JS values, matching what the Rust side expects.
const cborEncoder = new Encoder({})
const cborDecoder = new Decoder({})
const EVENT_BATCH = 'ws:m:event-batch'

// ─────────────────────────────────────────────────────────────────────────────
// WS message-throughput instrumentation
// ─────────────────────────────────────────────────────────────────────────────
// Counterpart to the server's `ws_timing` module. Counts inbound/outbound
// messages by `event` kind into per-window buckets, logs a single summary
// line every WINDOW_MS so we can diagnose "server CPU idle but loads slow"
// — comparing client send/recv rates to the server's ws_timing log lines
// tells us whether time is in server-reply latency, client-send pacing, or
// network round-trip.
const WS_TIMING_WINDOW_MS = 250
const wsTimingInbound = new Map<string, number>()
const wsTimingOutbound = new Map<string, number>()
let wsTimingTimer: ReturnType<typeof setInterval> | null = null

function recordWsInbound(kind: string): void {
  wsTimingInbound.set(kind, (wsTimingInbound.get(kind) ?? 0) + 1)
}
function recordWsOutbound(kind: string): void {
  wsTimingOutbound.set(kind, (wsTimingOutbound.get(kind) ?? 0) + 1)
}
/** Strip the `ws:m:` prefix and CamelCase the kind so it matches the
 *  server-side `message_kind` tag verbatim (e.g. "ws:m:report" → "Report",
 *  "ws:m:query-response" → "QueryResponse"). */
function eventKindShort(event: string): string {
  const stripped = event.startsWith('ws:m:') ? event.slice(5) : event
  return stripped
    .split('-')
    .map((part) =>
      part.length > 0 ? part[0].toUpperCase() + part.slice(1) : part,
    )
    .join('')
}

function ensureWsTimingLogger(): void {
  if (wsTimingTimer) return
  wsTimingTimer = setInterval(() => {
    if (wsTimingInbound.size === 0 && wsTimingOutbound.size === 0) return
    const inEntries = [...wsTimingInbound.entries()].sort((a, b) => b[1] - a[1])
    const outEntries = [...wsTimingOutbound.entries()].sort(
      (a, b) => b[1] - a[1],
    )
    const inTotal = inEntries.reduce((s, [, n]) => s + n, 0)
    const outTotal = outEntries.reduce((s, [, n]) => s + n, 0)
    const inFmt = inEntries.map(([k, n]) => `${k}=${n}`).join(', ')
    const outFmt = outEntries.map(([k, n]) => `${k}=${n}`).join(', ')
    // eslint-disable-next-line no-console
    console.info(
      `[ws_timing window=${WS_TIMING_WINDOW_MS}ms] in=${inTotal} [${inFmt}] out=${outTotal} [${outFmt}]`,
    )
    wsTimingInbound.clear()
    wsTimingOutbound.clear()
  }, WS_TIMING_WINDOW_MS)
}

function stableStringify(value: unknown): string | null {
  const seen = new WeakSet<object>()
  try {
    return JSON.stringify(value, (_key, raw) => {
      if (typeof raw === 'bigint') return `__bigint:${raw.toString()}`
      if (!raw || typeof raw !== 'object') return raw
      if (seen.has(raw)) return '__circular__'
      seen.add(raw)
      if (Array.isArray(raw)) return raw
      const sorted: Record<string, unknown> = {}
      for (const key of Object.keys(raw as Record<string, unknown>).sort()) {
        sorted[key] = (raw as Record<string, unknown>)[key]
      }
      return sorted
    })
  } catch {
    return null
  }
}

/** Union type for error event names */
export type MykoErrorEvent =
  | typeof MykoEvent.QueryError
  | typeof MykoEvent.ViewError
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

/** Wire protocol for encoding messages */
export enum MykoProtocol {
  JSON = 'JSON',
  CBOR = 'CBOR',
}

type ConnectionLogLevel = 'silent' | 'error' | 'warn' | 'info' | 'debug' | 'verbose'

/** Query class interface */
export interface Query<T> {
  readonly queryId: string
  readonly queryItemType: string
  readonly query: Record<string, unknown>
  readonly $res?: () => T[]
}

/** View class interface */
export interface View<T> {
  readonly viewId: string
  readonly viewItemType: string
  readonly view: Record<string, unknown>
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

/** Extract item type from a view */
export type ViewItem<V> = V extends View<infer R> ? R : unknown

/** Extract result type from a view */
export type ViewResult<V> = V extends View<infer R> ? R[] : unknown[]

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

export type QueryWindow = {
  offset: number
  limit: number
}

export type QueryWatchOptions = {
  window?: QueryWindow | null
}

export type QueryWindowInfo = {
  totalCount: number | null
  window: QueryWindow | null
}

function queryCacheKey(
  query: Pick<Query<unknown>, 'queryId' | 'query'>,
  options?: QueryWatchOptions,
): string {
  const queryPayload = stableStringify(query.query) ?? '__unstable_query__'
  const windowPayload = stableStringify(options?.window ?? null) ?? '__unstable_window__'
  return `query:${query.queryId}:${queryPayload}:${windowPayload}`
}

function viewCacheKey(
  view: Pick<View<unknown>, 'viewId' | 'view'>,
  options?: QueryWatchOptions,
): string {
  const viewPayload = stableStringify(view.view) ?? '__unstable_view__'
  const windowPayload = stableStringify(options?.window ?? null) ?? '__unstable_window__'
  return `view:${view.viewId}:${viewPayload}:${windowPayload}`
}

function reportCacheKey(report: Pick<Report<unknown>, 'reportId' | 'report'>): string {
  const payload = stableStringify(report.report) ?? '__unstable_report__'
  return `report:${report.reportId}:${payload}`
}

// Message type aliases
type QueryResponseMessage = Extract<
  MykoMessage,
  { event: typeof MykoEvent.QueryResponse }
>
type ReportResponseMessage = Extract<
  MykoMessage,
  { event: typeof MykoEvent.ReportResponse }
>
type CommandResponseMessage = Extract<
  MykoMessage,
  { event: typeof MykoEvent.CommandResponse }
>
type CommandErrorMessage = Extract<
  MykoMessage,
  { event: typeof MykoEvent.CommandError }
>
type QueryErrorMessage = Extract<
  MykoMessage,
  { event: typeof MykoEvent.QueryError }
>
type ReportErrorMessage = Extract<
  MykoMessage,
  { event: typeof MykoEvent.ReportError }
>
type PingMessage = Extract<MykoMessage, { event: typeof MykoEvent.Ping }>
type CommandIncomingMessage = Extract<
  MykoMessage,
  { event: typeof MykoEvent.Command }
>

interface ManagedSocket {
  ws: WebSocket
  address: string
  endpointKey: string
  reconnectOnClose: boolean
}

/**
 * Reactive WebSocket client for Myko servers with automatic failover.
 */
export class MykoClient {
  // Socket management
  private sockets = new Map<string, ManagedSocket>()
  private reconnectTimers = new Map<string, ReturnType<typeof setTimeout>>()
  private endpointSockets = new Map<string, string>()
  // Main server: the socket used for outbound sends.
  // Other open sockets are warm standbys for failover.
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
  private commandIncoming = new Subject<CommandIncomingMessage>()

  // Per-tx response routing. Replaces the previous pattern of every
  // `watch{Report,Query,View}` subscription doing `filter(r => r.data.tx === tx)`
  // on a shared Subject — that scaled as O(active_subscriptions × messages),
  // costing measurable seconds during scene-load bursts. Direct dispatch is O(1).
  private reportResponseRoutes = new Map<string, Subject<ReportResponseMessage>>()
  private queryResponseRoutes = new Map<string, Subject<QueryResponseMessage>>()
  private viewResponseRoutes = new Map<string, Subject<QueryResponseMessage>>()
  private queryErrorRoutes = new Map<string, Subject<QueryErrorMessage>>()
  private viewErrorRoutes = new Map<string, Subject<QueryErrorMessage>>()
  private reportErrorRoutes = new Map<string, Subject<ReportErrorMessage>>()

  // Lighter-weight direct-callback routing for `subscribeReport` — avoids
  // allocating an RxJS Subject per anchor when the consumer just needs a
  // single callback (which is the case for liveReport in the Svelte runtime).
  private reportValueCallbacks = new Map<
    string,
    (msg: ReportResponseMessage) => void
  >()
  private reportErrorCallbacks = new Map<
    string,
    (msg: ReportErrorMessage) => void
  >()

  // State observables
  private connectionStatusSubject = new ReplaySubject<ConnectionStatus>(1)
  private currentServerSubject = new ReplaySubject<string | null>(1)

  // Subscription tracking
  private activeQueries = new Map<string, WrappedQuery>()
  private activeViews = new Map<string, WrappedView>()
  private activeReports = new Map<string, WrappedReport>()
  private activeQueryNames = new Map<string, string>()
  private activeViewNames = new Map<string, string>()
  private activeReportNames = new Map<string, string>()
  private sharedQueries = new Map<string, Observable<unknown>>()
  private sharedViews = new Map<string, Observable<unknown>>()
  private sharedQueryDiffs = new Map<string, Observable<unknown>>()
  private sharedViewDiffs = new Map<string, Observable<unknown>>()
  private sharedReports = new Map<string, Observable<unknown>>()
  private subscriptionStartMs = new Map<string, number>()
  private firstResponseLogged = new Set<string>()
  private messageQueue: MykoMessage[] = []
  private pendingEventBatch: MEvent[] = []
  private eventBatchFlushScheduled = false
  private readonly eventBatchMaxSize = 256
  private connectionLogLevelThreshold: ConnectionLogLevel =
    MykoClient.resolveDefaultConnectionLogLevel()

  // Stats
  private downMsgCounter = new Subject<void>()
  private upMsgCounter = new Subject<void>()

  // Auth & peer discovery
  private userToken: string | null = null
  private peerDiscoveryEnabled = true
  private peerDiscoverySubscription: Subscription | null = null
  private useSecureWebSocket = false

  // Protocol defaults to JSON for maximum compatibility (no CBOR tag handling, bigint issues, etc).
  private protocol: MykoProtocol = MykoProtocol.JSON

  constructor() {
    this.setConnectionStatus(ConnectionStatus.Disconnected, 'init')
    this.setCurrentServer(null, 'init')
  }

  /** Set connection log verbosity at runtime. */
  setConnectionLogLevel(level: ConnectionLogLevel): void {
    this.connectionLogLevelThreshold = level
  }

  /** Set the wire protocol (JSON or CBOR). Default is JSON. */
  setProtocol(protocol: MykoProtocol): void {
    this.protocol = protocol
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Connection Management
  // ─────────────────────────────────────────────────────────────────────────────

  /** Set a single server address, clearing any existing connections */
  setAddress(address: string | null): void {
    this.shouldReconnect = true // Re-enable autoreconnect when setting new address
    this.closeAllSockets()
    if (address) {
      this.useSecureWebSocket = address.startsWith('wss://')
      this.createSocket(address, true)
    }
  }

  /** Set multiple server addresses, clearing any existing connections */
  setAddresses(addresses: string[]): void {
    this.shouldReconnect = true // Re-enable autoreconnect when setting new addresses
    this.closeAllSockets()
    for (const addr of addresses) {
      this.createSocket(addr, true)
    }
  }

  /** Add additional servers (connects immediately) */
  addServers(addresses: string[], reconnectOnClose = true): void {
    this.shouldReconnect = true // Re-enable autoreconnect when adding servers
    for (const addr of addresses) {
      if (!this.hasConnectionTo(addr)) {
        this.createSocket(addr, reconnectOnClose)
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

  /** Get the current main server address used for outbound sends */
  getMainServer(): string | null {
    return this.currentServer
  }

  /** Get all server addresses */
  getServers(): string[] {
    return Array.from(this.sockets.values()).map((m) => m.address)
  }

  /** Get addresses of all open connections */
  getOpenServers(): string[] {
    return Array.from(this.sockets.values())
      .filter((m) => m.ws.readyState === WebSocket.OPEN)
      .map((m) => m.address)
  }

  /** Observable of current server changes */
  get currentServer$(): Observable<string | null> {
    return this.currentServerSubject.asObservable()
  }

  /** Observable of main server changes */
  get mainServer$(): Observable<string | null> {
    return this.currentServer$
  }

  /** Get current connection status */
  getConnectionStatus(): ConnectionStatus {
    if (this.currentServer) return ConnectionStatus.Connected
    for (const m of this.sockets.values()) {
      if (m.ws.readyState === WebSocket.CONNECTING)
        return ConnectionStatus.Connecting
    }
    return ConnectionStatus.Disconnected
  }

  /** Observable of connection status changes */
  get connectionStatus$(): Observable<ConnectionStatus> {
    return this.connectionStatusSubject.asObservable()
  }

  /** Observable of incoming command messages (ws:m:command) from the server */
  get commandIncoming$(): Observable<CommandIncomingMessage> {
    return this.commandIncoming.asObservable()
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
    this.logConnection('peer_discovery_started', {
      secure: this.useSecureWebSocket,
      via: 'query:GetPeerServers',
    })

    this.peerDiscoverySubscription = this.watchQuery(
      new GetPeerServers({}),
    ).subscribe((servers: Server[]) => {
      const addresses = servers.map((s) =>
        this.useSecureWebSocket
          ? `wss://${s.address}/myko`
          : `ws://${s.address}:${s.port}/myko`,
      )
      this.logConnection('peer_discovery_update', {
        peers: servers.length,
        addresses,
      })
      // Discovered peers are ephemeral: if they disconnect, wait for discovery
      // to advertise them again rather than actively redialing.
      if (addresses.length > 0) this.addServers(addresses, false)
    })
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
    const toError = <
      T extends {
        event: MykoErrorEvent
        data: { tx: string; message: string }
      },
    >(
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
    const nowMs = Date.now()
    // IMPORTANT: the Rust server expects `timestamp: i64`.
    // - JSON cannot encode bigint, so use number in JSON mode.
    // - CBOR can encode bigint as a tagged integer, so use bigint in CBOR mode.
    const timestamp =
      this.protocol === MykoProtocol.CBOR ? BigInt(nowMs) : nowMs

    this.send({
      event: MykoEvent.Ping,
      data: { id, timestamp } as unknown as PingData,
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
    options?: QueryWatchOptions,
  ): [string, Observable<QueryResponseMessage>] {
    if (!query.queryId || !query.queryItemType || !(query as { query?: unknown }).query) {
      const details = {
        ctor: (query as { constructor?: { name?: string } }).constructor?.name ?? 'unknown',
        keys: Object.keys((query as Record<string, unknown>) ?? {}),
        queryId: (query as { queryId?: unknown }).queryId,
        queryItemType: (query as { queryItemType?: unknown }).queryItemType,
      }
      this.logConnection('query_shape_invalid', details)
      throw new Error(
        `Invalid query shape for ${details.ctor}: expected { queryId, queryItemType, query }`,
      )
    }

    const tx = uuid()
    const window = options?.window ?? undefined
    const queryName =
      (query as { constructor?: { name?: string } }).constructor?.name ?? query.queryId
    const wrappedQuery = {
      query: { ...query.query, tx, createdAt: new Date().toISOString() },
      queryId: query.queryId,
      queryItemType: query.queryItemType,
      ...(window ? { window } : {}),
    } as WrappedQuery

    this.activeQueries.set(tx, wrappedQuery)
    this.activeQueryNames.set(tx, queryName)
    this.subscriptionStartMs.set(tx, this.nowMs())
    this.firstResponseLogged.delete(tx)
    this.logConnection('query_subscribe', {
      tx,
      queryId: wrappedQuery.queryId,
      queryItemType: wrappedQuery.queryItemType,
      queryName,
      window: window ?? null,
      activeQueries: this.activeQueries.size,
    })
    this.send({ event: MykoEvent.Query, data: wrappedQuery })

    // Direct per-tx routing: see reportResponseRoutes/queryResponseRoutes for
    // background. Each subscription registers its Subject in routeMessage's map
    // so dispatch is O(1) instead of an O(active) filter walk per WS frame.
    const responseRoute = new Subject<QueryResponseMessage>()
    const errorRoute = new Subject<QueryErrorMessage>()
    this.queryResponseRoutes.set(tx, responseRoute)
    this.queryErrorRoutes.set(tx, errorRoute)

    const responses$ = new Observable<QueryResponseMessage>((subscriber) => {
      const responseSub = responseRoute.subscribe({
        next: (response) => subscriber.next(response),
        error: (error) => subscriber.error(error),
      })

      const errorSub = errorRoute.subscribe((error) => {
        subscriber.error(new Error(error.data.message))
      })

      return () => {
        responseSub.unsubscribe()
        errorSub.unsubscribe()
      }
    }).pipe(
      finalize(() => {
        this.logConnection('query_cancel', {
          tx,
          queryId: wrappedQuery.queryId,
          queryItemType: wrappedQuery.queryItemType,
          queryName,
          activeQueriesBefore: this.activeQueries.size,
        })
        this.activeQueries.delete(tx)
        this.activeQueryNames.delete(tx)
        this.subscriptionStartMs.delete(tx)
        this.firstResponseLogged.delete(tx)
        this.queryResponseRoutes.delete(tx)
        this.queryErrorRoutes.delete(tx)
        responseRoute.complete()
        errorRoute.complete()
        this.send({ event: MykoEvent.QueryCancel, data: { tx } })
      }),
    )

    return [tx, responses$]
  }

  /** Update server-side window for an active query subscription */
  setQueryWindow(tx: string, window: QueryWindow | null): void {
    const active = this.activeQueries.get(tx)
    if (!active) return

    const updated = {
      ...active,
      ...(window ? { window } : {}),
    } as WrappedQuery
    if (!window) {
      delete (updated as { window?: QueryWindow }).window
    }
    this.activeQueries.set(tx, updated)
    this.logConnection('query_window_set', {
      tx,
      queryId: active.queryId,
      queryItemType: active.queryItemType,
      queryName: this.activeQueryNames.get(tx) ?? active.queryId,
      window,
    })

    this.send({ event: MykoEvent.QueryWindow, data: { tx, window } as unknown } as MykoMessage)
  }

  /** Start a view subscription, returns [tx, responses$] */
  private startView<V extends View<unknown>>(
    view: V,
    options?: QueryWatchOptions,
  ): [string, Observable<QueryResponseMessage>] {
    if (!view.viewId || !view.viewItemType || !(view as { view?: unknown }).view) {
      const details = {
        ctor: (view as { constructor?: { name?: string } }).constructor?.name ?? 'unknown',
        keys: Object.keys((view as Record<string, unknown>) ?? {}),
        viewId: (view as { viewId?: unknown }).viewId,
        viewItemType: (view as { viewItemType?: unknown }).viewItemType,
      }
      this.logConnection('view_shape_invalid', details)
      throw new Error(
        `Invalid view shape for ${details.ctor}: expected { viewId, viewItemType, view }`,
      )
    }

    const tx = uuid()
    const window = options?.window ?? undefined
    const viewName =
      (view as { constructor?: { name?: string } }).constructor?.name ?? view.viewId
    const wrappedView = {
      view: { ...view.view, tx, createdAt: new Date().toISOString() },
      viewId: view.viewId,
      viewItemType: view.viewItemType,
      ...(window ? { window } : {}),
    } as WrappedView

    this.activeViews.set(tx, wrappedView)
    this.activeViewNames.set(tx, viewName)
    this.subscriptionStartMs.set(tx, this.nowMs())
    this.firstResponseLogged.delete(tx)
    this.logConnection('view_subscribe', {
      tx,
      viewId: wrappedView.viewId,
      viewItemType: wrappedView.viewItemType,
      viewName,
      window: wrappedView.window ?? null,
      activeViews: this.activeViews.size,
    })
    this.send({ event: MykoEvent.View, data: wrappedView })

    const responseRoute = new Subject<QueryResponseMessage>()
    const errorRoute = new Subject<QueryErrorMessage>()
    this.viewResponseRoutes.set(tx, responseRoute)
    this.viewErrorRoutes.set(tx, errorRoute)

    const responses$ = new Observable<QueryResponseMessage>((subscriber) => {
      const responseSub = responseRoute.subscribe({
        next: (response) => subscriber.next(response),
        error: (error) => subscriber.error(error),
      })

      const errorSub = errorRoute.subscribe((error) => {
        subscriber.error(new Error(error.data.message))
      })

      return () => {
        responseSub.unsubscribe()
        errorSub.unsubscribe()
      }
    }).pipe(
      finalize(() => {
        this.logConnection('view_cancel', {
          tx,
          viewId: wrappedView.viewId,
          viewName,
          activeViewsBefore: this.activeViews.size,
        })
        this.viewResponseRoutes.delete(tx)
        this.viewErrorRoutes.delete(tx)
        responseRoute.complete()
        errorRoute.complete()
        this.activeViews.delete(tx)
        this.activeViewNames.delete(tx)
        this.subscriptionStartMs.delete(tx)
        this.firstResponseLogged.delete(tx)
        this.send({ event: MykoEvent.ViewCancel, data: { tx } })
      }),
    )

    return [tx, responses$]
  }

  /** Update server-side window for an active view subscription */
  setViewWindow(tx: string, window: QueryWindow | null): void {
    const active = this.activeViews.get(tx)
    if (!active) return

    const updated = {
      ...active,
      ...(window ? { window } : {}),
    } as WrappedView
    if (!window) {
      delete (updated as { window?: QueryWindow }).window
    }
    this.activeViews.set(tx, updated)
    this.logConnection('view_window_set', {
      tx,
      viewId: active.viewId,
      viewName: this.activeViewNames.get(tx) ?? active.viewId,
      window,
    })

    this.send({ event: MykoEvent.ViewWindow, data: { tx, window } as unknown } as MykoMessage)
  }

  /** Watch a query and receive live updates with automatic deduplication */
  watchQuery<Q extends Query<unknown>>(
    query: Q,
    options?: QueryWatchOptions,
  ): Observable<QueryResult<Q>> {
    const cacheKey = queryCacheKey(query, options)

    const existing = this.sharedQueries.get(cacheKey)
    if (existing) return existing as Observable<QueryResult<Q>>

    const [, responses$] = this.startQuery(query, options)

    const shared$ = responses$.pipe(
      scan((acc, update) => {
        if (BigInt(update.data.sequence) === 0n) acc.clear()
        for (const id of update.data.deletes) acc.delete(id)
        for (const wrapped of update.data.upserts) {
          const item = wrapped.item as { id: string }
          if (item?.id) acc.set(item.id, wrapped)
        }
        return acc
      }, new Map<string, WrappedItem>()),
      map((items) => [...items.values()].map((w) => w.item) as QueryResult<Q>),
      finalize(() => {
        this.sharedQueries.delete(cacheKey)
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
      // Defensive copy: shareReplay replays the same array reference to all
      // subscribers, so a mutation (e.g. .shift()) by one subscriber would
      // corrupt the shared value for others. Cloning per-subscriber prevents this.
      map((x) => (Array.isArray(x) ? (x.slice() as QueryResult<Q>) : x)),
    )

    this.sharedQueries.set(cacheKey, shared$)
    return shared$
  }

  /** Watch a view and receive live updates with automatic deduplication */
  watchView<V extends View<unknown>>(
    view: V,
    options?: QueryWatchOptions,
  ): Observable<ViewResult<V>> {
    const cacheKey = viewCacheKey(view, options)

    const existing = this.sharedViews.get(cacheKey)
    if (existing) return existing as Observable<ViewResult<V>>

    const [, responses$] = this.startView(view, options)

    const shared$ = responses$.pipe(
      scan((acc, update) => {
        if (BigInt(update.data.sequence) === 0n) acc.clear()
        for (const id of update.data.deletes) acc.delete(id)
        for (const wrapped of update.data.upserts) {
          const item = wrapped.item as { id: string }
          if (item?.id) acc.set(item.id, wrapped)
        }
        return acc
      }, new Map<string, WrappedItem>()),
      map((items) => [...items.values()].map((w) => w.item) as ViewResult<V>),
      finalize(() => {
        this.sharedViews.delete(cacheKey)
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
      map((x) => (Array.isArray(x) ? (x.slice() as ViewResult<V>) : x)),
    )

    this.sharedViews.set(cacheKey, shared$)
    return shared$
  }

  /** Watch a query and receive raw diff events */
  watchQueryDiff<Q extends Query<unknown>>(
    query: Q,
    options?: QueryWatchOptions,
  ): Observable<QueryDiff<QueryItem<Q>>> {
    const cacheKey = queryCacheKey(query, options)
    const existing = this.sharedQueryDiffs.get(cacheKey)
    if (existing) return existing as Observable<QueryDiff<QueryItem<Q>>>

    const [, responses$] = this.startQuery(query, options)
    const shared$ = responses$.pipe(
      map((r) => ({
        sequence: BigInt(r.data.sequence),
        deletes: r.data.deletes.slice(),
        upserts: r.data.upserts.map(
          (w: WrappedItem) => w.item,
        ) as QueryItem<Q>[],
      })),
      finalize(() => {
        this.sharedQueryDiffs.delete(cacheKey)
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
      map((diff) => ({
        sequence: diff.sequence,
        deletes: diff.deletes.slice(),
        upserts: diff.upserts.slice(),
      })),
    )
    this.sharedQueryDiffs.set(cacheKey, shared$)
    return shared$
  }

  /** Watch a view and receive raw diff events */
  watchViewDiff<V extends View<unknown>>(
    view: V,
    options?: QueryWatchOptions,
  ): Observable<QueryDiff<ViewItem<V>>> {
    const cacheKey = viewCacheKey(view, options)
    const existing = this.sharedViewDiffs.get(cacheKey)
    if (existing) return existing as Observable<QueryDiff<ViewItem<V>>>

    const [, responses$] = this.startView(view, options)
    const shared$ = responses$.pipe(
      map((r) => ({
        sequence: BigInt(r.data.sequence),
        deletes: r.data.deletes.slice(),
        upserts: r.data.upserts.map(
          (w: WrappedItem) => w.item,
        ) as ViewItem<V>[],
      })),
      finalize(() => {
        this.sharedViewDiffs.delete(cacheKey)
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
      map((diff) => ({
        sequence: diff.sequence,
        deletes: diff.deletes.slice(),
        upserts: diff.upserts.slice(),
      })),
    )
    this.sharedViewDiffs.set(cacheKey, shared$)
    return shared$
  }

  /**
   * Start a live query with a mutable server-side window.
   * Use `setWindow` to scroll without re-subscribing.
   */
  watchQueryWindowed<Q extends Query<unknown>>(
    query: Q,
    options?: QueryWatchOptions,
  ): {
    tx: string
    results$: Observable<QueryResult<Q>>
    windowInfo$: Observable<QueryWindowInfo>
    setWindow: (window: QueryWindow | null) => void
  } {
    const [tx, responses$] = this.startQuery(query, options)
    const sharedResponses$ = responses$.pipe(
      shareReplay({ bufferSize: 1, refCount: true }),
    )
    const results$ = sharedResponses$.pipe(
      scan((state, update) => {
        const data = update.data as QueryResponseMessage['data'] & {
          total_count?: number
          totalCount?: number
          window?: QueryWindow | null
          changes?: Array<
            | { kind: 'upsert'; item: WrappedItem }
            | { kind: 'delete'; id: string }
            | {
                kind: 'windowOrder'
                ids: string[]
                totalCount?: number
                total_count?: number
                window?: QueryWindow | null
              }
          >
        }

        if (BigInt(update.data.sequence) === 0n) {
          state.cache.clear()
          state.visibleIds = []
        }

        for (const id of update.data.deletes) state.cache.delete(id)
        for (const wrapped of update.data.upserts) {
          const item = wrapped.item as { id?: string }
          if (item?.id) state.cache.set(item.id, wrapped)
        }

        const order = data.changes?.find(
          (change: NonNullable<typeof data.changes>[number]): change is Extract<NonNullable<typeof data.changes>[number], { kind: 'windowOrder' }> =>
            change.kind === 'windowOrder',
        )
        if (order) {
          state.visibleIds = order.ids.slice()
        } else {
          // Fallback when window-order diffs are unavailable: derive visible ids
          // from current cache contents (in insertion order).
          state.visibleIds = [...state.cache.keys()]
        }

        return state
      }, {
        cache: new Map<string, WrappedItem>(),
        visibleIds: [] as string[],
      }),
      map((state) =>
        state.visibleIds
          .map((id) => state.cache.get(id)?.item)
          .filter((item): item is JsonValue => item !== undefined) as QueryResult<Q>,
      ),
      map((x) => x.slice() as QueryResult<Q>),
    )
    const windowInfo$ = sharedResponses$.pipe(
      map((update) => {
        const data = update.data as QueryResponseMessage['data'] & {
          total_count?: number
          totalCount?: number
          window?: QueryWindow | null
          changes?: Array<
            | { kind: 'upsert'; item: WrappedItem }
            | { kind: 'delete'; id: string }
            | {
                kind: 'windowOrder'
                ids: string[]
                totalCount?: number
                total_count?: number
                window?: QueryWindow | null
              }
          >
        }
        const order = data.changes?.find(
          (change: NonNullable<typeof data.changes>[number]): change is Extract<NonNullable<typeof data.changes>[number], { kind: 'windowOrder' }> =>
            change.kind === 'windowOrder',
        )
        const orderTotalCount = order?.totalCount ?? order?.total_count
        return {
          totalCount:
            typeof data.totalCount === 'number'
              ? data.totalCount
              : typeof data.total_count === 'number'
                ? data.total_count
              : typeof orderTotalCount === 'number'
                ? orderTotalCount
                : null,
          window: data.window ?? order?.window ?? null,
        } satisfies QueryWindowInfo
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
    )

    return {
      tx,
      results$,
      windowInfo$,
      setWindow: (window) => this.setQueryWindow(tx, window),
    }
  }

  /** Start a live view with a mutable server-side window. */
  watchViewWindowed<V extends View<unknown>>(
    view: V,
    options?: QueryWatchOptions,
  ): {
    tx: string
    results$: Observable<ViewResult<V>>
    windowInfo$: Observable<QueryWindowInfo>
    setWindow: (window: QueryWindow | null) => void
  } {
    const [tx, responses$] = this.startView(view, options)
    const sharedResponses$ = responses$.pipe(
      shareReplay({ bufferSize: 1, refCount: true }),
    )
    const results$ = sharedResponses$.pipe(
      scan((state, update) => {
        const data = update.data as QueryResponseMessage['data'] & {
          total_count?: number
          totalCount?: number
          window?: QueryWindow | null
          changes?: Array<
            | { kind: 'upsert'; item: WrappedItem }
            | { kind: 'delete'; id: string }
            | {
                kind: 'windowOrder'
                ids: string[]
                totalCount?: number
                total_count?: number
                window?: QueryWindow | null
              }
          >
        }

        if (BigInt(update.data.sequence) === 0n) {
          state.cache.clear()
          state.visibleIds = []
        }

        for (const id of update.data.deletes) state.cache.delete(id)
        for (const wrapped of update.data.upserts) {
          const item = wrapped.item as { id?: string }
          if (item?.id) state.cache.set(item.id, wrapped)
        }

        const order = data.changes?.find(
          (change: NonNullable<typeof data.changes>[number]): change is Extract<NonNullable<typeof data.changes>[number], { kind: 'windowOrder' }> =>
            change.kind === 'windowOrder',
        )
        if (order) {
          state.visibleIds = order.ids.slice()
        } else {
          state.visibleIds = [...state.cache.keys()]
        }

        return state
      }, {
        cache: new Map<string, WrappedItem>(),
        visibleIds: [] as string[],
      }),
      map((state) =>
        state.visibleIds
          .map((id) => state.cache.get(id)?.item)
          .filter((item): item is JsonValue => item !== undefined) as ViewResult<V>,
      ),
      map((x) => x.slice() as ViewResult<V>),
    )
    const windowInfo$ = sharedResponses$.pipe(
      map((update) => {
        const data = update.data as QueryResponseMessage['data'] & {
          total_count?: number
          totalCount?: number
          window?: QueryWindow | null
          changes?: Array<
            | { kind: 'upsert'; item: WrappedItem }
            | { kind: 'delete'; id: string }
            | {
                kind: 'windowOrder'
                ids: string[]
                totalCount?: number
                total_count?: number
                window?: QueryWindow | null
              }
          >
        }
        const order = data.changes?.find(
          (change: NonNullable<typeof data.changes>[number]): change is Extract<NonNullable<typeof data.changes>[number], { kind: 'windowOrder' }> =>
            change.kind === 'windowOrder',
        )
        const orderTotalCount = order?.totalCount ?? order?.total_count
        return {
          totalCount:
            typeof data.totalCount === 'number'
              ? data.totalCount
              : typeof data.total_count === 'number'
                ? data.total_count
                : typeof orderTotalCount === 'number'
                  ? orderTotalCount
                  : null,
          window: data.window ?? order?.window ?? null,
        } satisfies QueryWindowInfo
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
    )
    return {
      tx,
      results$,
      windowInfo$,
      setWindow: (window) => this.setViewWindow(tx, window),
    }
  }

  /**
   * Subscribe a single report with direct callbacks, bypassing RxJS entirely.
   *
   * `watchReport` returns an Observable wrapped in pipe(map, finalize,
   * shareReplay, map) — each subscriber walks that operator chain, allocates
   * a Subject per tx, and walks Subscriber chains on emission. This bypass
   * stores callbacks in a flat Map and lets routeMessage call them directly,
   * which matters when a workspace mounts hundreds of anchor subscriptions
   * in a single Svelte microtask.
   *
   * Caller gets a dispose function. No deduplication — the caller owns the
   * subscription lifetime.
   */
  subscribeReport<R extends Report<unknown>>(
    report: R,
    onValue: (value: ReportResult<R>) => void,
    onError?: (error: Error) => void,
  ): () => void {
    const tx = uuid()
    const reportName =
      (report as { constructor?: { name?: string } }).constructor?.name ??
      report.reportId
    const wrappedReport: WrappedReport = {
      report: { ...report.report, tx },
      reportId: report.reportId,
    }

    this.activeReports.set(tx, wrappedReport)
    this.activeReportNames.set(tx, reportName)
    if (this.willLogConnection('report_subscribe')) {
      this.logConnection('report_subscribe', {
        tx,
        reportId: wrappedReport.reportId,
        reportName,
        report: report.report,
        activeReports: this.activeReports.size,
      })
    }
    this.send({ event: MykoEvent.Report, data: wrappedReport })

    this.reportValueCallbacks.set(tx, (r) => {
      onValue(r.data.response as ReportResult<R>)
    })
    if (onError) {
      this.reportErrorCallbacks.set(tx, (r) => {
        onError(new Error(r.data.message))
      })
    }

    let disposed = false
    return () => {
      if (disposed) return
      disposed = true
      this.reportValueCallbacks.delete(tx)
      this.reportErrorCallbacks.delete(tx)
      this.activeReports.delete(tx)
      this.activeReportNames.delete(tx)
      this.send({ event: MykoEvent.ReportCancel, data: { tx } })
    }
  }


  /** Watch a report with automatic deduplication */
  watchReport<R extends Report<unknown>>(
    report: R,
  ): Observable<ReportResult<R>> {
    const cacheKey = reportCacheKey(report)

    const existing = this.sharedReports.get(cacheKey)
    if (existing) return existing as Observable<ReportResult<R>>

    const tx = uuid()
    const reportName =
      (report as { constructor?: { name?: string } }).constructor?.name ??
      report.reportId
    const wrappedReport: WrappedReport = {
      report: { ...report.report, tx },
      reportId: report.reportId,
    }

    this.activeReports.set(tx, wrappedReport)
    this.activeReportNames.set(tx, reportName)
    this.logConnection('report_subscribe', {
      tx,
      reportId: wrappedReport.reportId,
      reportName,
      report: report.report,
      activeReports: this.activeReports.size,
    })
    this.send({ event: MykoEvent.Report, data: wrappedReport })

    const responseRoute = new Subject<ReportResponseMessage>()
    this.reportResponseRoutes.set(tx, responseRoute)

    const shared$ = responseRoute.pipe(
      map((r) => r.data.response as ReportResult<R>),
      finalize(() => {
        this.logConnection('report_cancel', {
          tx,
          reportId: wrappedReport.reportId,
          reportName,
          activeReportsBefore: this.activeReports.size,
        })
        this.sharedReports.delete(cacheKey)
        this.activeReports.delete(tx)
        this.activeReportNames.delete(tx)
        this.reportResponseRoutes.delete(tx)
        responseRoute.complete()
        this.send({ event: MykoEvent.ReportCancel, data: { tx } })
      }),
      shareReplay({ bufferSize: 1, refCount: true }),
      // Defensive copy: prevent one subscriber's mutations from affecting others
      map((x) => {
        if (Array.isArray(x)) return x.slice() as ReportResult<R>
        if (x && typeof x === 'object') {
          return { ...(x as Record<string, unknown>) } as ReportResult<R>
        }
        return x
      }),
    )

    this.sharedReports.set(cacheKey, shared$)
    return shared$
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Commands & Events
  // ─────────────────────────────────────────────────────────────────────────────

  /** Send an event to the server */
  sendEvent(event: MEvent): void {
    // Pulses are latency-sensitive; bypass batching and send immediately.
    if (event.itemType === 'Pulse') {
      this.flushPendingEventBatch()
      this.sendNow({ event: MykoEvent.Event, data: event })
      return
    }
    this.sendEventBatch([event])
  }

  /** Send a batch of events to the server */
  sendEventBatch(events: MEvent[]): void {
    if (events.length === 0) return

    const buffered: MEvent[] = []
    const immediatePulses: MEvent[] = []
    for (const event of events) {
      if (event.itemType === 'Pulse') {
        immediatePulses.push(event)
      } else {
        buffered.push(event)
      }
    }

    if (buffered.length > 0) {
      this.pendingEventBatch.push(...buffered)
    }

    if (immediatePulses.length > 0) {
      this.flushPendingEventBatch()
      for (const pulse of immediatePulses) {
        this.sendNow({ event: MykoEvent.Event, data: pulse })
      }
      return
    }

    if (this.pendingEventBatch.length >= this.eventBatchMaxSize) {
      this.flushPendingEventBatch()
      return
    }
    this.scheduleEventBatchFlush()
  }

  /** Send a command and wait for response */
  sendCommand<C extends Command<unknown>>(
    command: C,
  ): Promise<CommandResult<C>> {
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
    return this.endpointSockets.has(this.endpointKey(address))
  }

  private parseAddress(address: string): { host: string; port: number } | null {
    try {
      const url = new URL(address)
      const port = url.port
        ? parseInt(url.port, 10)
        : url.protocol === 'wss:'
          ? 443
          : 80
      return { host: url.hostname.toLowerCase(), port }
    } catch {
      return null
    }
  }

  private endpointKey(address: string): string {
    const parsed = this.parseAddress(address)
    if (!parsed) return address
    const host = this.hostsEquivalent(parsed.host, 'localhost')
      ? 'localhost'
      : parsed.host
    return `${host}:${parsed.port}`
  }

  private hostsEquivalent(a: string, b: string): boolean {
    if (a === b) return true

    const loopback = new Set(['localhost', '127.0.0.1', '::1'])
    return loopback.has(a) && loopback.has(b)
  }

  private closeAllSockets(): void {
    for (const timer of this.reconnectTimers.values()) clearTimeout(timer)
    this.reconnectTimers.clear()
    this.endpointSockets.clear()

    for (const m of this.sockets.values()) {
      m.ws.onclose = null
      m.ws.onerror = null
      m.ws.onopen = null
      m.ws.onmessage = null
      m.ws.close()
    }
    this.sockets.clear()
    this.setCurrentServer(null, 'all sockets closed')
    this.setConnectionStatus(
      ConnectionStatus.Disconnected,
      'all sockets closed',
    )
  }

  private createSocket(address: string, reconnectOnClose = true): void {
    const endpointKey = this.endpointKey(address)
    if (this.endpointSockets.has(endpointKey)) return
    this.endpointSockets.set(endpointKey, address)
    const reconnectTimer = this.reconnectTimers.get(endpointKey)
    if (reconnectTimer) {
      clearTimeout(reconnectTimer)
      this.reconnectTimers.delete(endpointKey)
    }

    this.logConnection('socket_connecting', {
      address,
      openServers: this.getOpenServers(),
      knownServers: this.getServers(),
    })
    const ws = new WebSocket(address)
    ws.binaryType = 'arraybuffer' // Receive binary messages as ArrayBuffer for CBOR
    const managed: ManagedSocket = {
      ws,
      address,
      endpointKey,
      reconnectOnClose,
    }
    this.sockets.set(address, managed)

    if (this.sockets.size === 1) {
      this.setConnectionStatus(
        ConnectionStatus.Connecting,
        `connecting to ${address}`,
      )
    }

    ws.onopen = () => {
      if (this.sockets.get(address) !== managed) {
        ws.close()
        return
      }

      if (!this.currentServer) {
        this.setCurrentServer(address, 'socket open')
        this.setConnectionStatus(
          ConnectionStatus.Connected,
          `connected to ${address}`,
        )
        this.flushQueue()
        this.resendSubscriptions()
        if (this.peerDiscoveryEnabled) this.startPeerDiscovery()
      }
    }

    ws.onclose = () => {
      if (this.sockets.get(address) !== managed) return

      this.sockets.delete(address)
      this.endpointSockets.delete(endpointKey)

      if (this.currentServer === address) {
        const next = this.getFirstOpenSocket()
        if (next) {
          this.setCurrentServer(
            next.address,
            `failover from ${address} to ${next.address}`,
          )
          this.setConnectionStatus(
            ConnectionStatus.Connected,
            `main failover to ${next.address}`,
          )
          this.resendSubscriptions()
          return // Peer discovery will re-add this server when it's back
        }
        this.setCurrentServer(null, `disconnected from ${address}`)
        this.setConnectionStatus(
          ConnectionStatus.Disconnected,
          `no open servers after ${address} closed`,
        )
      }

      // Only retry explicitly configured sockets when completely disconnected.
      // Discovered peers are expected to reappear via peer discovery.
      if (
        managed.reconnectOnClose &&
        this.shouldReconnect &&
        !this.hasOpenConnection()
      ) {
        this.scheduleReconnect(address, endpointKey)
      }
    }

    ws.onerror = () => {}

    ws.onmessage = (event) => {
      if (this.sockets.get(address) !== managed) return
      this.onMessage(event.data)
    }
  }

  private scheduleReconnect(address: string, endpointKey: string): void {
    if (this.reconnectTimers.has(endpointKey)) return

    this.logConnection('reconnect_scheduled', { address, delayMs: 1000 })
    const timer = setTimeout(() => {
      this.reconnectTimers.delete(endpointKey)
      if (
        this.shouldReconnect &&
        !this.hasOpenConnection() &&
        !this.endpointSockets.has(endpointKey)
      ) {
        this.logConnection('reconnect_attempt', { address })
        this.createSocket(address)
      }
    }, 1000)

    this.reconnectTimers.set(endpointKey, timer)
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // Private: Message Handling
  // ─────────────────────────────────────────────────────────────────────────────

  private onMessage(data: string | ArrayBuffer | Blob): void {
    this.downMsgCounter.next()

    try {
      let message: MykoMessage

      if (typeof data === 'string') {
        // JSON text message
        message = JSON.parse(data) as MykoMessage
      } else if (data instanceof ArrayBuffer) {
        // Binary CBOR message
        message = cborDecoder.decode(new Uint8Array(data)) as MykoMessage
      } else if (data instanceof Blob) {
        // Handle Blob asynchronously - convert to ArrayBuffer first
        data.arrayBuffer().then((buffer) => {
          const decoded = cborDecoder.decode(
            new Uint8Array(buffer),
          ) as MykoMessage
          recordWsInbound(eventKindShort((decoded as { event: string }).event))
          ensureWsTimingLogger()
          this.routeMessage(decoded)
        })
        return
      } else {
        return
      }

      recordWsInbound(eventKindShort((message as { event: string }).event))
      ensureWsTimingLogger()
      this.routeMessage(message)
    } catch {
      // Ignore parse errors
    }
  }

  private routeMessage(message: MykoMessage): void {
    switch (message.event) {
      case MykoEvent.QueryResponse:
        if (this.willLogConnection('query_response')) {
          this.maybeLogFirstResponseTiming('query', message.data.tx, {
            queryId: this.activeQueries.get(message.data.tx)?.queryId,
            queryName:
              this.activeQueryNames.get(message.data.tx) ??
              this.activeQueries.get(message.data.tx)?.queryId ??
              'unknown',
            sequence: message.data.sequence,
            upserts: message.data.upserts.length,
            deletes: message.data.deletes.length,
          })
          this.logConnection('query_response', {
            tx: message.data.tx,
            queryId: this.activeQueries.get(message.data.tx)?.queryId,
            queryItemType: this.activeQueries.get(message.data.tx)?.queryItemType,
            queryName:
              this.activeQueryNames.get(message.data.tx) ??
              this.activeQueries.get(message.data.tx)?.queryId ??
              'unknown',
            sequence: message.data.sequence,
            upserts: message.data.upserts.length,
            deletes: message.data.deletes.length,
            changes: (message.data as { changes?: unknown[] }).changes?.length ?? 0,
            totalCount:
              (message.data as { totalCount?: number; total_count?: number }).totalCount ??
              (message.data as { totalCount?: number; total_count?: number }).total_count ??
              null,
            window: (message.data as { window?: QueryWindow | null }).window ?? null,
          })
        }
        this.queryResponseRoutes.get(message.data.tx)?.next(message)
        this.queryResponses.next(message)
        break
      case MykoEvent.ViewResponse: {
        // ViewResponse and QueryResponse share the same payload shape.
        const viewMessage = message as unknown as QueryResponseMessage
        if (this.willLogConnection('view_response')) {
          this.maybeLogFirstResponseTiming(
            'view',
            viewMessage.data.tx,
            {
              viewId: this.activeViews.get(viewMessage.data.tx)?.viewId,
              viewName:
                this.activeViewNames.get(viewMessage.data.tx) ??
                this.activeViews.get(viewMessage.data.tx)?.viewId ??
                'unknown',
              sequence: viewMessage.data.sequence,
              upserts: viewMessage.data.upserts.length,
              deletes: viewMessage.data.deletes.length,
            },
          )
          this.logConnection('view_response', {
            tx: viewMessage.data.tx,
            sequence: viewMessage.data.sequence,
            upserts: viewMessage.data.upserts.length,
            deletes: viewMessage.data.deletes.length,
            changes: (viewMessage.data as { changes?: unknown[] }).changes?.length ?? 0,
            totalCount: (viewMessage.data as { totalCount?: number; total_count?: number })
              .totalCount ??
              (viewMessage.data as { totalCount?: number; total_count?: number }).total_count ??
              null,
            window: (viewMessage.data as { window?: QueryWindow | null }).window ?? null,
          })
        }
        this.viewResponseRoutes.get(viewMessage.data.tx)?.next(viewMessage)
        this.queryResponses.next(viewMessage)
        break
      }
      case MykoEvent.ReportResponse:
        this.reportValueCallbacks.get(message.data.tx)?.(message)
        this.reportResponseRoutes.get(message.data.tx)?.next(message)
        this.reportResponses.next(message)
        break
      case MykoEvent.CommandResponse:
        this.commandResponses.next(message as CommandResponseMessage)
        break
      case MykoEvent.CommandError:
        this.commandErrors.next(message as CommandErrorMessage)
        break
      case MykoEvent.QueryError:
        this.queryErrorRoutes.get(message.data.tx)?.next(message as QueryErrorMessage)
        this.queryErrors.next(message as QueryErrorMessage)
        this.logConnection('query_error', {
          tx: message.data.tx,
          message: message.data.message,
          queryId: this.activeQueries.get(message.data.tx)?.queryId,
          queryItemType: this.activeQueries.get(message.data.tx)?.queryItemType,
          queryName:
            this.activeQueryNames.get(message.data.tx) ??
            this.activeQueries.get(message.data.tx)?.queryId ??
            'unknown',
        })
        break
      case MykoEvent.ViewError:
        this.viewErrorRoutes.get(message.data.tx)?.next(message as unknown as QueryErrorMessage)
        this.queryErrors.next(message as unknown as QueryErrorMessage)
        this.logConnection('view_error', {
          tx: message.data.tx,
          message: message.data.message,
          viewId: this.activeViews.get(message.data.tx)?.viewId,
          viewItemType: this.activeViews.get(message.data.tx)?.viewItemType,
          viewName:
            this.activeViewNames.get(message.data.tx) ??
            this.activeViews.get(message.data.tx)?.viewId ??
            'unknown',
        })
        break
      case MykoEvent.ReportError:
        this.reportErrorCallbacks.get(message.data.tx)?.(message as ReportErrorMessage)
        this.reportErrorRoutes.get(message.data.tx)?.next(message as ReportErrorMessage)
        this.reportErrors.next(message as ReportErrorMessage)
        this.logConnection('report_error', {
          tx: message.data.tx,
          message: message.data.message,
          reportId: this.activeReports.get(message.data.tx)?.reportId,
          reportName:
            this.activeReportNames.get(message.data.tx) ??
            this.activeReports.get(message.data.tx)?.reportId ??
            'unknown',
        })
        break
      case MykoEvent.Ping:
        this.pingResponses.next(message as PingMessage)
        break
      case MykoEvent.Command:
        this.commandIncoming.next(message as CommandIncomingMessage)
        break
    }
  }

  private scheduleEventBatchFlush(): void {
    if (this.eventBatchFlushScheduled) return
    this.eventBatchFlushScheduled = true
    queueMicrotask(() => {
      this.eventBatchFlushScheduled = false
      this.flushPendingEventBatch()
    })
  }

  private flushPendingEventBatch(): void {
    if (this.pendingEventBatch.length === 0) return

    while (this.pendingEventBatch.length > 0) {
      const batch = this.pendingEventBatch.splice(0, this.eventBatchMaxSize)
      this.sendNow({ event: EVENT_BATCH, data: batch } as unknown as MykoMessage)
    }
  }

  private send(message: MykoMessage): void {
    if ((message as { event: string }).event !== EVENT_BATCH) {
      this.flushPendingEventBatch()
    }
    this.sendNow(message)
  }

  private messageTx(message: MykoMessage): string | null {
    const data = (message as { data?: unknown }).data as
      | { tx?: string }
      | undefined
    return typeof data?.tx === 'string' ? data.tx : null
  }

  private sendNow(message: MykoMessage): void {
    const event = (message as { event: string }).event
    const tx = this.messageTx(message)
    if (this.currentServer) {
      const managed = this.sockets.get(this.currentServer)
      if (managed?.ws.readyState === WebSocket.OPEN) {
        const encoded =
          this.protocol === MykoProtocol.CBOR
            ? cborEncoder.encode(message)
            : JSON.stringify(message)
        managed.ws.send(encoded)
        this.upMsgCounter.next()
        // Tap for ws_timing summary. Use the short kind suffix (after
        // `ws:m:`) for a tighter log line.
        recordWsOutbound(eventKindShort(event))
        ensureWsTimingLogger()
        return
      }
    }
    this.messageQueue.push(message)
    this.logConnection('ws_enqueue', {
      event,
      tx,
      queueDepth: this.messageQueue.length,
      currentServer: this.currentServer,
    })
  }

  private flushQueue(): void {
    const queue = this.messageQueue
    this.messageQueue = []
    this.logConnection('ws_flush_queue', {
      queuedMessages: queue.length,
      currentServer: this.currentServer,
    })
    for (const msg of queue) this.send(msg)
  }

  private withReconnectSequenceReset(query: WrappedQuery): WrappedQuery {
    const queryPayload = query.query
    if (
      queryPayload &&
      typeof queryPayload === 'object' &&
      !Array.isArray(queryPayload)
    ) {
      return {
        ...query,
        query: {
          ...(queryPayload as Record<string, JsonValue>),
          seq: 0 as JsonValue,
        },
      }
    }
    return query
  }

  private resendSubscriptions(): void {
    for (const q of this.activeQueries.values()) {
      this.send({
        event: MykoEvent.Query,
        data: this.withReconnectSequenceReset(q),
      })
    }
    for (const v of this.activeViews.values()) {
      this.send({ event: MykoEvent.View, data: v })
    }
    for (const r of this.activeReports.values()) {
      this.send({ event: MykoEvent.Report, data: r })
    }
  }

  private setConnectionStatus(status: ConnectionStatus, reason: string): void {
    this.connectionStatusSubject.next(status)
    this.logConnection('status', {
      status,
      reason,
      currentServer: this.currentServer,
      openServers: this.getOpenServers(),
    })
  }

  private setCurrentServer(server: string | null, reason: string): void {
    this.currentServer = server
    this.currentServerSubject.next(server)
    this.logConnection('current_server', { server, reason })
  }

  private nowMs(): number {
    if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
      return performance.now()
    }
    return Date.now()
  }

  private maybeLogFirstResponseTiming(
    kind: 'query' | 'view',
    tx: string,
    details: Record<string, unknown>,
  ): void {
    if (this.firstResponseLogged.has(tx)) return
    const startedAt = this.subscriptionStartMs.get(tx)
    if (startedAt === undefined) return
    const elapsedMs = this.nowMs() - startedAt
    this.firstResponseLogged.add(tx)
    this.logConnection(`${kind}_first_response_timing`, {
      tx,
      subscribeToFirstResponseMs: Number(elapsedMs.toFixed(2)),
      ...details,
    })
  }

  private connectionLogLevel(
    event: string,
  ): 'info' | 'debug' | 'verbose' | 'warn' | 'error' {
    // Main lifecycle state transitions remain visible at info.
    const infoEvents = new Set(['status', 'current_server'])
    if (infoEvents.has(event)) return 'info'

    // Subscription setup and first-response timing are debug-level diagnostics.
    const debugEvents = new Set([
      'socket_connecting',
      'peer_discovery_started',
      'peer_discovery_update',
      'query_subscribe',
      'view_subscribe',
      'report_subscribe',
      'query_cancel',
      'view_cancel',
      'report_cancel',
      'query_first_response_timing',
      'view_first_response_timing',
      'reconnect_scheduled',
      'reconnect_attempt',
      'query_shape_invalid',
    ])
    if (debugEvents.has(event)) return 'debug'

    // Follow-up response churn and transport internals are verbose-level.
    const verboseEvents = new Set([
      'ws_enqueue',
      'ws_flush_queue',
      'query_response',
      'view_response',
      'report_response',
      'query_publish_ms',
      'view_publish_ms',
    ])
    if (verboseEvents.has(event)) return 'verbose'

    if (event.endsWith('_error')) return 'error'

    return 'debug'
  }

  private static resolveDefaultConnectionLogLevel(): ConnectionLogLevel {
    const readGlobal = (): string | undefined => {
      const globalObj = globalThis as Record<string, unknown>
      const value = globalObj.MYKO_CLIENT_LOG_LEVEL
      return typeof value === 'string' ? value : undefined
    }

    const readProcessEnv = (): string | undefined => {
      const processLike = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process
      return processLike?.env?.MYKO_CLIENT_LOG_LEVEL
    }

    const value = (readGlobal() ?? readProcessEnv() ?? '').toLowerCase()
    switch (value) {
      case 'silent':
      case 'error':
      case 'warn':
      case 'info':
      case 'debug':
      case 'verbose':
        return value
      default:
        return 'warn'
    }
  }

  private shouldLogConnection(level: ConnectionLogLevel): boolean {
    const priority: Record<ConnectionLogLevel, number> = {
      silent: 0,
      error: 1,
      warn: 2,
      info: 3,
      debug: 4,
      verbose: 5,
    }
    return priority[level] <= priority[this.connectionLogLevelThreshold]
  }

  /**
   * Check whether a given event would be logged at the current verbosity.
   * Use this to gate expensive details-object construction at hot call sites
   * (e.g. per-response log calls during scene-load bursts).
   */
  willLogConnection(event: string): boolean {
    const level = this.connectionLogLevel(event)
    return this.shouldLogConnection(level)
  }

  private logConnection(event: string, details: Record<string, unknown>): void {
    const level = this.connectionLogLevel(event)
    if (!this.shouldLogConnection(level)) return
    const maybeVerbose = (
      console as unknown as { verbose?: (...args: unknown[]) => void }
    ).verbose
    const logger: (...args: unknown[]) => void =
      level === 'info'
        ? console.info
        : level === 'warn'
          ? console.warn
          : level === 'error'
            ? console.error
            : level === 'verbose' || level === 'debug'
              ? maybeVerbose ?? console.debug ?? console.log
              : console.log
    const levelPrefix = level === 'verbose' ? '[verbose] ' : ''

    logger(`${levelPrefix}[MykoClient] ${event}`, {
      tsIso: new Date().toISOString(),
      tsMs: Date.now(),
      ...details,
    })
  }
}
