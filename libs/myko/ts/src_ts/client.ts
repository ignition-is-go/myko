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

    // Clean up existing connection
    if (this.ws) {
      this.shouldReconnect = false
      this.ws.close()
      this.ws = null
    }

    if (this.reconnectTimeout) {
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

  /** Disconnect from the server */
  disconnect(): void {
    this.shouldReconnect = false
    if (this.ws) {
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

    this.connectionStatusSubject.next(ConnectionStatus.Connecting)

    try {
      this.ws = new WebSocket(this.address)

      this.ws.onopen = () => {
        this.connectionStatusSubject.next(ConnectionStatus.Connected)
        this.flushQueue()
        this.resendSubscriptions()
      }

      this.ws.onclose = () => {
        this.ws = null
        this.connectionStatusSubject.next(ConnectionStatus.Disconnected)

        if (this.shouldReconnect && this.address) {
          this.scheduleReconnect()
        }
      }

      this.ws.onerror = () => {
        // Error handling is done in onclose
      }

      this.ws.onmessage = (event) => {
        this.onMessage(event.data)
      }
    } catch {
      this.connectionStatusSubject.next(ConnectionStatus.Disconnected)
      if (this.shouldReconnect) {
        this.scheduleReconnect()
      }
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimeout) return

    this.reconnectTimeout = setTimeout(() => {
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
