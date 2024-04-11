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
import { firstValueFrom, shareReplay } from 'rxjs'

@Injectable()
export class MykoReportBus extends AMykoReportBus {
  constructor(
    private moduleRef: ModuleRef,
    private logger: LoggerService,
  ) {
    super()
  }

  cache = new Map<string, MLiveReportResult<MReport<unknown>>>()

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

    const clone: MReport<unknown> = {
      ...report,
      tx: undefined,
    }

    const hash = JSON.stringify(clone)

    const txKey = `${reportId}:${report.tx}`
    const cacheKey = `${reportId}:${hash}`

    if (this.cache.has(cacheKey)) {
      console.log('returning cached report for', cacheKey)
      return this.cache.get(cacheKey) as MLiveReportResult<T>
    }

    const obs = handler.execute(report) as MLiveReportResult<T>

    console.time(txKey)
    firstValueFrom(obs).then((res) => {
      console.timeEnd(txKey)
    })

    this.cache.set(cacheKey, obs.pipe(shareReplay(1)))

    return obs
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
