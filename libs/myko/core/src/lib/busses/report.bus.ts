import {
  finalize,
  firstValueFrom,
  map,
  ReplaySubject,
  share,
  tap,
  throwError,
} from 'rxjs'
import { MYKO_HANDLER_REPORT_ID_KEY, MYKO_REPORT_ID_KEY } from '../constants'
import {
  tapN,
  type ContextPhantom,
  type MLiveReportResult,
  type MReport,
  type MReportHandler,
  type MReportResult,
  type Type,
} from '../types'
import { ObservableBus } from './observable.bus'

export type MykoReportHandlerType = Type<MReportHandler<MReport<unknown>>>

/**
 * Abstract class representing a report bus in the Myko core library.
 * @template T - The type of report.
 */
export abstract class AMykoReportBus extends ObservableBus<MReport<unknown>> {
  constructor(
    private options: { disableCache?: boolean } = { disableCache: false },
  ) {
    super()
  }

  /**
   * Map containing the registered report handlers.
   */
  protected handlers: Map<string, MReportHandler<MReport<unknown>>> = new Map()

  /**
   * Binds a report handler to a specific ID.
   * @param handler - The report handler to bind.
   * @param id - The ID to bind the handler to.
   */
  protected bind<U>(handler: MReportHandler<MReport<U>>, id: string): void {
    this.handlers.set(id, handler)
  }

  /**
   * Registers multiple report handlers.
   * @param handlers - An array of report handlers to register.
   */
  register(handlers: MykoReportHandlerType[]): void {
    handlers.forEach((h) => this.registerHandler(h))
  }

  /**
   * Abstract method to watch a report and return a live report result.
   * @param report - The report to watch.
   * @returns The live report result.
   */
  private cache = new Map<string, MLiveReportResult<MReport<unknown>>>()

  watch<T extends MReport<unknown> & ContextPhantom>(
    report: T,
  ): MLiveReportResult<T> {
    const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, report)
    const handler = this.handlers.get(reportId)

    const err = `Handler not Provided for [${reportId}]`

    if (!handler) {
      return throwError(() => new Error(err)) as unknown as MLiveReportResult<T>
    }

    const clone: MReport<unknown> = {
      ...report,
      tx: undefined,
    }

    const hash = JSON.stringify(clone)

    const cacheKey = `${reportId}:${hash}`

    if (this.cache.has(cacheKey) && !this.options?.disableCache) {
      return this.cache.get(cacheKey)!.pipe() as MLiveReportResult<T>
    }

    this.on_prepare?.(report.tx, reportId)
    const obs = handler.execute(report).pipe(
      tapN(1, () => this.on_result?.(report.tx, reportId)),
      share({
        connector: () => new ReplaySubject(1),
      }),
      tap(() => this.on_follow_up?.(report.tx, reportId)),
      map((x) => {
        // clone the object
        if (x instanceof Array) return x.slice()
        if (x instanceof Object) return { ...x }
        return x
      }),
      finalize(() => {
        this.cache.delete(cacheKey)
      }),
    ) as MLiveReportResult<T>

    this.cache.set(cacheKey, obs)

    return obs
  }

  /**
   * Executes a report and returns the report result.
   * @param report - The report to execute.
   * @returns The report result.
   */
  execute<U, T extends MReport<U> & ContextPhantom>(
    report: T,
  ): MReportResult<T> {
    return firstValueFrom(this.watch(report)) as MReportResult<T>
  }

  /**
   * Abstract method to register a report handler.
   * @param handler - The report handler to register.
   */
  protected abstract registerHandler(handler: MykoReportHandlerType): void
}

export class MReportBus extends AMykoReportBus {
  registerHandler(handler: MykoReportHandlerType): void {
    const reportId = Reflect.getMetadata(MYKO_HANDLER_REPORT_ID_KEY, handler)
    this.bind(new handler(), reportId)
  }
}

export const reportBus: MReportBus = new MReportBus()
