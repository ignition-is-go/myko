import {
  AMykoReportBus,
  MYKO_HANDLER_REPORT_ID_KEY,
  MykoReportHandlerType,
} from '@myko/core'
import { Injectable } from '@nestjs/common'
import { ModuleRef } from '@nestjs/core'

@Injectable()
export class MykoReportBus extends AMykoReportBus {
  constructor(private moduleRef: ModuleRef) {
    super()
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
