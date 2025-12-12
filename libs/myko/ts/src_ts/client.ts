/**
 * Pure TypeScript WebSocket client for Myko servers
 *
 * This replaces the NAPI-based Rust client for browser compatibility.
 */

import {
  MykoEvent,
  type MEvent,
  type MykoMessage,
  type QueryReturn,
  type ReportReturn,
  type WrappedItem,
  type WrappedQuery,
  type WrappedReport,
} from '@myko/rs'
import {
  filter,
  finalize,
  map,
  Observable,
  ReplaySubject,
  scan,
  shareReplay,
  Subject,
} from 'rxjs'
import { v4 as uuid } from 'uuid'

/** Connection status enum */
export enum ConnectionStatus {
  Connected = 'Connected',
  Disconnected = 'Disconnected',
  Connecting = 'Connecting',
}

/** Extract result type from a query factory */
export type QueryResult<Q> = Q extends QueryReturn<infer R> ? R : unknown[]

/** Extract item type from a query factory (unwrapped from array) */
export type QueryItem<Q> =
  Q extends QueryReturn<infer R> ? (R extends (infer I)[] ? I : R) : unknown

/** Extract result type from a report factory */
export type ReportResult<R> = R extends ReportReturn<infer T> ? T : unknown

/** Diff event for incremental query updates */
export type QueryDiff<T> = {
  /** Sequence number (0 = initial state, reset map) */
  sequence: bigint
  /** IDs of deleted items */
  deletes: string[]
  /** New or updated items */
  upserts: T[]
}

// Message type aliases from MykoMessage discriminated union
type QueryResponseMessage = Extract<
  MykoMessage<unknown>,
  { event: typeof MykoEvent.QueryResponse }
>
type ReportResponseMessage = Extract<
  MykoMessage<unknown>,
  { event: typeof MykoEvent.ReportResponse }
>
type CommandResponseMessage = Extract<
  MykoMessage<unknown>,
  { event: typeof MykoEvent.CommandResponse }
>
type CommandErrorMessage = Extract<
  MykoMessage<unknown>,
  { event: typeof MykoEvent.CommandError }
>

/** Command factory type for type-safe commands */
export type CommandReturn<T> = {
  command: Record<string, unknown>
  commandId: string
  $res?: () => T
}

/**
 * MykoClient - Pure TypeScript reactive client for Myko servers
 */
export class MykoClient {
  private address: string | null = null
  private ws: WebSocket | null = null
  private reconnectTimeout: ReturnType<typeof setTimeout> | null = null
  private shouldReconnect = true

  private connectionStatusSubject = new ReplaySubject<ConnectionStatus>(1)
  private queryResponses = new Subject<QueryResponseMessage>()
  private reportResponses = new Subject<ReportResponseMessage>()
  private commandResponses = new Subject<CommandResponseMessage>()
  private commandErrors = new Subject<CommandErrorMessage>()

  // Track active subscriptions for reconnection (by tx)
  private activeQueries = new Map<string, WrappedQuery>()
  private activeReports = new Map<string, WrappedReport>()

  // Shared report observables for deduplication (by cache key)
  private sharedReports = new Map<string, Observable<unknown>>()

  // Message queue for when not connected
  private messageQueue: MykoMessage<unknown>[] = []

  constructor() {
    this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
  }

  /** Create a stable cache key from a query/report factory */
  private getCacheKey(
    type: 'query' | 'report',
    factory: {
      query?: Record<string, unknown>
      report?: Record<string, unknown>
      queryId?: string
      reportId?: string
    },
  ): string {
    const id = type === 'query' ? factory.queryId : factory.reportId
    const args = type === 'query' ? factory.query : factory.report
    return `${type}:${id}:${JSON.stringify(args)}`
  }

  /** Set server address (e.g., 'ws://localhost:5155/myko') */
  setAddress(address: string | null): void {
    const wasConnected = this.ws !== null
    console.log('[MykoClient] setAddress:', address, 'wasConnected:', wasConnected)

    // Clean up existing connection
    if (this.ws) {
      console.log('[MykoClient] setAddress: closing existing ws')
      // Null handlers to prevent async onclose from triggering reconnect
      this.ws.onclose = null
      this.ws.onerror = null
      this.ws.close()
      this.ws = null
    }

    if (this.reconnectTimeout) {
      console.log('[MykoClient] setAddress: clearing reconnect timeout')
      clearTimeout(this.reconnectTimeout)
      this.reconnectTimeout = null
    }

    this.address = address
    this.shouldReconnect = true

    if (address) {
      this.connect()
    } else if (wasConnected) {
      this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
    }
  }

  /** Get current connection status */
  getConnectionStatus(): ConnectionStatus {
    if (!this.ws) return ConnectionStatus.Disconnected
    switch (this.ws.readyState) {
      case WebSocket.CONNECTING:
        return ConnectionStatus.Connecting
      case WebSocket.OPEN:
        return ConnectionStatus.Connected
      default:
        return ConnectionStatus.Disconnected
    }
  }

  /** Observable of connection status changes */
  get connectionStatus$(): Observable<ConnectionStatus> {
    return this.connectionStatusSubject.asObservable()
  }

  /**
   * Watch a query and receive live updates
   * @param queryFactory Query from queries.* (e.g., queries.GetAllServers({}))
   */
  watchQuery<Q extends QueryReturn<unknown>>(
    queryFactory: Q,
  ): Observable<QueryResult<Q>> {
    const tx = uuid()

    const wrappedQuery: WrappedQuery = {
      query: { ...queryFactory.query, tx, createdAt: new Date().toISOString() },
      queryId: queryFactory.queryId,
      queryItemType: queryFactory.queryItemType,
    }

    // Track for reconnection
    this.activeQueries.set(tx, wrappedQuery)

    // Send immediately if connected
    this.send({ event: MykoEvent.Query, data: wrappedQuery })

    return this.queryResponses.pipe(
      filter((r) => r.data.tx === tx),

      scan((acc, update) => {
        // Reset on sequence 0
        if (BigInt(update.data.sequence) === 0n) {
          acc.clear()
        }

        // Process deletes
        for (const id of update.data.deletes) {
          acc.delete(id)
        }

        // Process upserts
        for (const wrapped of update.data.upserts) {
          const item = wrapped.item as { id: string }
          if (item && typeof item === 'object' && 'id' in item) {
            acc.set(item.id, wrapped)
          }
        }

        return acc
      }, new Map<string, WrappedItem<unknown>>()),

      map((items) => [...items.values()].map((w) => w.item) as QueryResult<Q>),

      shareReplay({ bufferSize: 1, refCount: true }),

      finalize(() => {
        this.activeQueries.delete(tx)
        this.send({ event: MykoEvent.QueryCancel, data: { tx } })
      }),
    )
  }

  /**
   * Watch a query and receive raw diff events for incremental updates.
   * Use this with SvelteMap or similar reactive Map implementations.
   *
   * Note: For query deduplication, use SvelteMykoClient.query() which
   * shares the underlying SvelteMap across multiple consumers.
   *
   * @param queryFactory Query from queries.* (e.g., queries.GetAllServers({}))
   */
  watchQueryDiff<Q extends QueryReturn<unknown>>(
    queryFactory: Q,
  ): Observable<QueryDiff<QueryItem<Q>>> {
    const tx = uuid()

    const wrappedQuery: WrappedQuery = {
      query: { ...queryFactory.query, tx, createdAt: new Date().toISOString() },
      queryId: queryFactory.queryId,
      queryItemType: queryFactory.queryItemType,
    }

    // Track for reconnection
    this.activeQueries.set(tx, wrappedQuery)

    // Send immediately if connected
    this.send({ event: MykoEvent.Query, data: wrappedQuery })

    return this.queryResponses.pipe(
      filter((r) => r.data.tx === tx),

      map((r) => ({
        sequence: BigInt(r.data.sequence),
        deletes: r.data.deletes,
        upserts: r.data.upserts.map((w) => w.item) as QueryItem<Q>[],
      })),

      finalize(() => {
        this.activeQueries.delete(tx)
        this.send({ event: MykoEvent.QueryCancel, data: { tx } })
      }),
    )
  }

  /**
   * Watch a report and receive live updates.
   *
   * Multiple calls with the same report args will share a single subscription,
   * and the subscription is only cancelled when the last subscriber unsubscribes.
   *
   * @param reportFactory Report from reports.* (e.g., reports.CountAllTargets({}))
   */
  watchReport<R extends ReportReturn<unknown>>(
    reportFactory: R,
  ): Observable<ReportResult<R>> {
    const cacheKey = this.getCacheKey('report', reportFactory)

    // Return existing shared observable if available
    const existing = this.sharedReports.get(cacheKey)
    if (existing) {
      return existing as Observable<ReportResult<R>>
    }

    const tx = uuid()

    const wrappedReport: WrappedReport = {
      report: { ...reportFactory.report, tx },
      reportId: reportFactory.reportId,
    }

    // Track for reconnection
    this.activeReports.set(tx, wrappedReport)

    // Send immediately if connected
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

  /** Send an event to the server */
  sendEvent(event: MEvent): void {
    this.send({ event: MykoEvent.Event, data: event })
  }

  /**
   * Send a command to the server and wait for a response.
   *
   * @param commandFactory Command from commands.* (e.g., commands.DeleteMachine({ machineId: '...' }))
   * @returns Promise that resolves with the command result or rejects with an error
   */
  sendCommand<C extends CommandReturn<unknown>>(
    commandFactory: C,
  ): Promise<C extends CommandReturn<infer R> ? R : unknown> {
    type Result = C extends CommandReturn<infer R> ? R : unknown

    const tx = uuid()

    const wrappedCommand = {
      command: {
        ...commandFactory.command,
        tx,
        createdAt: new Date().toISOString(),
      },
      commandId: commandFactory.commandId,
    }

    return new Promise<Result>((resolve, reject) => {
      // Set up response/error listeners
      const responseSub = this.commandResponses
        .pipe(filter((r) => r.data.tx === tx))
        .subscribe((r) => {
          cleanup()
          resolve(r.data.response as Result)
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

      // Send the command
      this.send({ event: MykoEvent.Command, data: wrappedCommand })
    })
  }

  /** Disconnect from the server */
  disconnect(): void {
    this.shouldReconnect = false
    if (this.ws) {
      // Null handlers to prevent async onclose from triggering reconnect
      this.ws.onclose = null
      this.ws.onerror = null
      this.ws.close()
      this.ws = null
    }
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout)
      this.reconnectTimeout = null
    }
    this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
  }

  private connect(): void {
    if (!this.address) return

    console.log('[MykoClient] connect() called, existing ws:', this.ws?.readyState)

    // Close any existing connection before creating a new one
    if (this.ws) {
      console.log('[MykoClient] connect: closing existing ws, readyState:', this.ws.readyState)
      this.ws.onclose = null // Prevent triggering reconnect from this close
      this.ws.close()
      this.ws = null
    }

    this.connectionStatusSubject.next(ConnectionStatus.Connecting)

    try {
      console.log('[MykoClient] creating new WebSocket to:', this.address)
      this.ws = new WebSocket(this.address)

      this.ws.onopen = () => {
        console.log('[MykoClient] ws.onopen')
        this.connectionStatusSubject.next(ConnectionStatus.Connected)
        this.flushQueue()
        this.resendSubscriptions()
      }

      this.ws.onclose = () => {
        console.log('[MykoClient] ws.onclose, shouldReconnect:', this.shouldReconnect)
        this.ws = null
        this.connectionStatusSubject.next(ConnectionStatus.Disconnected)

        if (this.shouldReconnect && this.address) {
          this.scheduleReconnect()
        }
      }

      this.ws.onerror = (err) => {
        console.log('[MykoClient] ws.onerror', err)
        // Error handling is done in onclose
      }

      this.ws.onmessage = (event) => {
        this.onMessage(event.data)
      }
    } catch (err) {
      console.log('[MykoClient] connect catch:', err)
      this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
      if (this.shouldReconnect) {
        this.scheduleReconnect()
      }
    }
  }

  private scheduleReconnect(): void {
    console.log('[MykoClient] scheduleReconnect called, hasTimeout:', !!this.reconnectTimeout, 'wsState:', this.ws?.readyState)

    // Don't schedule if already scheduled or already connecting
    if (this.reconnectTimeout) {
      console.log('[MykoClient] scheduleReconnect: already have timeout, skipping')
      return
    }
    if (this.ws?.readyState === WebSocket.CONNECTING) {
      console.log('[MykoClient] scheduleReconnect: ws is CONNECTING, skipping')
      return
    }

    console.log('[MykoClient] scheduleReconnect: scheduling reconnect in 1s')
    this.reconnectTimeout = setTimeout(() => {
      console.log('[MykoClient] reconnect timeout fired, shouldReconnect:', this.shouldReconnect)
      this.reconnectTimeout = null
      if (this.shouldReconnect && this.address) {
        this.connect()
      }
    }, 1000)
  }

  private onMessage(data: string | ArrayBuffer | Blob): void {
    // Only handle string messages (JSON)
    if (typeof data !== 'string') return

    try {
      const message = JSON.parse(data) as MykoMessage<unknown>

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
      }
    } catch {
      // Ignore parse errors
    }
  }

  private send(message: MykoMessage<unknown>): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message))
    } else {
      this.messageQueue.push(message)
    }
  }

  private flushQueue(): void {
    const queue = this.messageQueue
    this.messageQueue = []
    for (const message of queue) {
      this.send(message)
    }
  }

  private resendSubscriptions(): void {
    // Resend all active queries
    for (const wrappedQuery of this.activeQueries.values()) {
      this.send({ event: MykoEvent.Query, data: wrappedQuery })
    }

    // Resend all active reports
    for (const wrappedReport of this.activeReports.values()) {
      this.send({ event: MykoEvent.Report, data: wrappedReport })
    }
  }
}
