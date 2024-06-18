import {
  AMykoReportBus,
  MLiveReportResult,
  MReport,
  MYKO_HANDLER_REPORT_ID_KEY,
  MYKO_REPORT_ID_KEY,
  MykoReportHandlerType,
} from '@myko/core'
import { Injectable } from '@nestjs/common'
import { ModuleRef } from '@nestjs/core'
import { map, shareReplay } from 'rxjs'

@Injectable()
export class MykoReportBus extends AMykoReportBus {
  constructor(private moduleRef: ModuleRef) {
    super()
  }

  cache = new Map<string, MLiveReportResult<MReport<unknown>>>()

  watch<T extends MReport<unknown>>(report: T): MLiveReportResult<T> {
    const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, report)
    const handler = this.handlers.get(reportId)

    const err = `Handler not Provided for ${report.constructor.name} [${reportId}]. Check your module's providers array, and that the command is decorated with @MykoReport(id: string)`

    if (!handler) {
      console.error(err)
      throw new Error(err)
    }

    const clone: MReport<unknown> = {
      ...report,
      tx: undefined,
    }

    const hash = JSON.stringify(clone)

    const txKey = `${reportId}:${report.tx}`
    const cacheKey = `${reportId}:${hash}`

    if (this.cache.has(cacheKey) && !process.env.MYKO_DISABLE_REPORT_CACHE) {
      return this.cache.get(cacheKey).pipe() as MLiveReportResult<T>
    }

    const obs = handler.execute(report).pipe(
      shareReplay(1),
      map((x) => {
        // clone the object
        if (x instanceof Array) return x.slice()
        if (x instanceof Object) return { ...x }
        return x
      }),
    ) as MLiveReportResult<T>

    this.cache.set(cacheKey, obs)

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
