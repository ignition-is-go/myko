import { MYKO_REPORT_ID_KEY, MYKO_REPORT_ITEM_TYPE_KEY } from '../constants'
import { ID, MQuery, MReport } from '../types'

export interface MWrappedReport {
  report: MReport<any>
  reportId: ID
}

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

export const unwrapReport = (
  wrappedQuery: MWrappedReport,
): MReport<unknown> => {
  const { report, reportId } = wrappedQuery
  Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, report)
  Reflect.defineMetadata(MYKO_REPORT_ITEM_TYPE_KEY, reportId, report)
  return report
}
