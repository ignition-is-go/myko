import 'reflect-metadata'
import { reportBus } from '../busses'
import { MYKO_HANDLER_REPORT_ID_KEY, MYKO_REPORT_ID_KEY } from '../constants'
import type { MReport, MReportHandler } from '../types/report'

/**
 * Decorator for defining a Myko report.
 * @param {string} reportId - The unique identifier for the report.
 * @returns {Function} - The decorator function.
 */
export const MykoReport =
  <R>(reportId: string) =>
  <T extends MReport<R>>(target: new (...args: any[]) => T): any => {
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, withType)
    return withType
  }

export const MykoReportHandler = <T extends MReport<T['$reportResult']>>(
  command: new (...args: any[]) => T,
): ((target: new (...args: any[]) => MReportHandler<T>) => void) => {
  return (target: new (...args: any[]) => MReportHandler<T>) => {
    const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, command)
    Reflect.defineMetadata(MYKO_HANDLER_REPORT_ID_KEY, reportId, target)
    reportBus.registerHandler(target)
  }
}
