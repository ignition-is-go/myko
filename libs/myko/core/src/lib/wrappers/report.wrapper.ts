import { MReport, type ID } from '../types'

/**
 * Represents a wrapped report.
 */
export type MWrappedReport = {
  report: MReport<any>
  reportId: ID
}

/**
 * Wraps a report with its ID.
 * @param report - The report to be wrapped.
 * @returns The wrapped report.
 * @throws Error if the query ID cannot be retrieved from metadata.\
 * @deprecated use MReport.wrap instead.
 */
export const wrapReport = (report: MReport<unknown>): MWrappedReport => {
  return report.wrap()
}

/**
 * Unwraps a wrapped report.
 * @param wrappedQuery - The wrapped report to be unwrapped.
 * @returns The unwrapped report.
 * @deprecated use MReport.fromWrappedReport instead.
 */
export const unwrapReport = (wrappedQuery: MWrappedReport): MReport<unknown> => {
  return MReport.fromWrappedReport(wrappedQuery)
}
