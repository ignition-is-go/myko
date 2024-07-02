import type { Observable } from 'rxjs'
import { v4 as uuid } from 'uuid'

/**
 * Represents a generic report.
 * @template T - The type of the report result.
 */
export class MReport<T> {
  /**
   * The result of the report.
   */
  $reportResult: T
  /**
   * The transaction ID of the report.
   */
  readonly tx: string
  constructor() {
    this.tx = uuid()
  }
}

/**
 * Represents the result type of a report.
 * @template T - The type of the report.
 */
export type MReportResult<T> = T extends MReport<infer R> ? Promise<R> : never

/**
 * Represents the live result type of a report.
 * @template T - The type of the report.
 */
export type MLiveReportResult<T> =
  T extends MReport<infer R> ? Observable<R> : never

/**
 * Represents a report handler.
 * @template H - The type of the report.
 */
export interface MReportHandler<H> {
  /**
   * Executes the report and returns the live report result.
   * @param report - The report to execute.
   * @returns The live report result.
   */
  execute(report: H): MLiveReportResult<H>
}
