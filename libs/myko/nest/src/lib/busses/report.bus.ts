import { Injectable } from '@nestjs/common'
import {
  AMykoReportBus,
  MReport,
  MykoReportHandlerType,
  MLiveReportResult,
  MYKO_REPORT_ID_KEY,
  MYKO_HANDLER_REPORT_ID_KEY,
} from '@myko/core'
import { ModuleRef } from '@nestjs/core'
import { LoggerService } from '@rship/logging'

@Injectable()
export class MykoReportBus extends AMykoReportBus {
  constructor(
    private moduleRef: ModuleRef,
    private logger: LoggerService,
  ) {
    super()
  }

  watch<T extends MReport<unknown>>(report: T): MLiveReportResult<T> {
    const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, report)
    const handler = this.handlers.get(reportId)

    const err = `Handler not Provided for ${report.constructor.name} [${reportId}]. Check your module's providers array, and that the command is decorated with @MykoReport(id: string)`

    if (!handler) {
      this.logger
        .getLogger('MykoReportBus')
        .dev.error({ message: err, data: report })
      throw new Error(err)
    }

    return handler.execute(report) as MLiveReportResult<T>
  }

  protected registerHandler(handler: MykoReportHandlerType): void {
    const instance = this.moduleRef.get(handler, {
      strict: false,
    })

    if (!instance) {
      throw new Error(`Cannot find instance of ${handler.constructor.name}`)
    }
    const reportId = Reflect.getMetadata(MYKO_HANDLER_REPORT_ID_KEY, handler)

    this.bind(instance, reportId)
  }
}
