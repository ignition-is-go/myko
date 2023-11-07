import { MYKO_HANDLER_REPORT_ID_KEY, MYKO_REPORT_ID_KEY } from '../constants'
import { MReport, MReportHandler, MReportResult } from '../types/report'

export const MykoReport =
  <R>(reportId: string) =>
  <T extends MReport<R>>(target: new (...args: any[]) => T) => {
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, withType)
    return withType
  }

export const MykoReportHandler = <T extends MReport<MReportResult<T>>>(
  command: new (...args: any[]) => T,
) => {
  return (target: new (...args: any[]) => MReportHandler<T>) => {
    const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, command)
    Reflect.defineMetadata(MYKO_HANDLER_REPORT_ID_KEY, reportId, target)
  }
}
