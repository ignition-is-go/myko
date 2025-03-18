import type { Observable } from 'rxjs'
import { MYKO_REPORT_ID_KEY, MYKO_REPORT_ITEM_TYPE_KEY } from '../constants'
import type { MWrappedReport } from '../wrappers'
import { WithContext } from './base'

/**
 * Represents a generic report.
 * @template T - The type of the report result.
 */
export class MReport<T> extends WithContext {
  /**
   * The result of the report.
   */
  $reportResult: T

  constructor() {
    super()
  }

  static fromWrappedReport<R>(wrappedReport: MWrappedReport): MReport<R> {
    const { report: reportInner, reportId } = wrappedReport
    const report = new MReport<R>()

    Object.assign(report, reportInner)
    Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, report)
    Reflect.defineMetadata(MYKO_REPORT_ITEM_TYPE_KEY, reportId, report)

    return report
  }

  wrap(): MWrappedReport {
    const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, this)
    if (!reportId) {
      throw new Error('Could not get query ID from Metadata')
    }

    return {
      report: this,
      reportId: reportId,
    }
  }
}

export type MReportType<T> = T extends MReport<infer R> ? R : never

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
