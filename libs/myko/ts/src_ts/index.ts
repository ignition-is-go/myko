/**
 * @myko/ts - TypeScript client for Myko servers
 *
 * Minimal wrapper providing RxJS Observables over the native Rust client.
 * All business logic lives in the Rust client (myko-rs).
 */

import type { ConnectionStatus, QueryReturn, ReportReturn } from '@myko/rs'
import { Observable, ReplaySubject, share } from 'rxjs'
import { MykoClient as NativeMykoClient } from '../index'

// Re-export all Rust-generated types from @myko/rs
export * from '@myko/rs'

/** Extract result type from a query factory */
type QueryResult<Q> = Q extends QueryReturn<infer R> ? R : unknown[]

/** Extract result type from a report factory */
type ReportResult<R> = R extends ReportReturn<infer T> ? T : unknown

/**
 * MykoClient - Reactive client for Myko servers
 */
export class MykoClient {
  private client: NativeMykoClient

  constructor() {
    this.client = new NativeMykoClient()
  }

  /** Set server address (e.g., 'ws://localhost:5155/myko') */
  setAddress(address: string | null): void {
    this.client.setAddress(address ?? undefined)
  }

  /** Get current connection status */
  async getConnectionStatus(): Promise<ConnectionStatus> {
    const json = await this.client.getConnectionStatus()
    return JSON.parse(json) as ConnectionStatus
  }

  /** Observable of connection status changes */
  get connectionStatus$(): Observable<ConnectionStatus> {
    return new Observable<ConnectionStatus>((subscriber) => {
      this.client.onConnectionStatus((err: Error | null, json: string) => {
        if (err) {
          subscriber.error(err)
        } else {
          subscriber.next(JSON.parse(json) as ConnectionStatus)
        }
      })
    }).pipe(share({ connector: () => new ReplaySubject(1) }))
  }

  /**
   * Watch a query and receive live updates
   * @param queryFactory Query from queries.* (e.g., queries.GetAllServers({}))
   */
  watchQuery<Q extends QueryReturn<unknown>>(
    queryFactory: Q,
  ): Observable<QueryResult<Q>> {
    const queryJson = JSON.stringify(queryFactory)

    return new Observable<QueryResult<Q>>((subscriber) => {
      this.client.watchQuery(
        queryJson,
        (err: Error | null, itemsJson: string) => {
          if (err) {
            subscriber.error(err)
          } else {
            subscriber.next(JSON.parse(itemsJson) as QueryResult<Q>)
          }
        },
      )
    }).pipe(share({ connector: () => new ReplaySubject(1) }))
  }

  /** Send an event to the server */
  async sendEvent(event: Record<string, unknown>): Promise<void> {
    const result = await this.client.sendEvent(JSON.stringify(event))
    if (result) {
      throw new Error(result)
    }
  }

  /**
   * Watch a report and receive live updates
   * @param reportFactory Report from reports.* (e.g., reports.CountAllTargets({}))
   */
  watchReport<R extends ReportReturn<unknown>>(
    reportFactory: R,
  ): Observable<ReportResult<R>> {
    const reportJson = JSON.stringify(reportFactory)

    return new Observable<ReportResult<R>>((subscriber) => {
      this.client.watchReport(
        reportJson,
        (err: Error | null, resultJson: string) => {
          if (err) {
            subscriber.error(err)
          } else {
            subscriber.next(JSON.parse(resultJson) as ReportResult<R>)
          }
        },
      )
    }).pipe(share({ connector: () => new ReplaySubject(1) }))
  }
}
