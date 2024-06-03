import { MYKO_REPORT_ID_KEY, MYKO_REPORT_ITEM_TYPE_KEY } from '../constants'
import { ID, MReport } from '../types'

/**
 * Represents a wrapped report.
 */
export interface MWrappedReport {
  report: MReport<any>
  reportId: ID
}

/**
 * Wraps a report with its ID.
 * @param report - The report to be wrapped.
 * @returns The wrapped report.
 * @throws Error if the query ID cannot be retrieved from metadata.
 */
export const wrapReport = (report: MReport<unknown>): MWrappedReport => {
  const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, report)
  if (!reportId) {
    throw new Error('Could not get query ID from Metadata')
  }

  return {
    report,
    reportId: reportId,
  }
}

/**
 * Unwraps a wrapped report.
 * @param wrappedQuery - The wrapped report to be unwrapped.
 * @returns The unwrapped report.
 */
export const unwrapReport = (
  wrappedQuery: MWrappedReport,
): MReport<unknown> => {
  const { report, reportId } = wrappedQuery
  Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, report)
  Reflect.defineMetadata(MYKO_REPORT_ITEM_TYPE_KEY, reportId, report)
  return report
}
