import 'reflect-metadata'
import { firstValueFrom } from 'rxjs'
import type {
  MLiveReportResult,
  MReport,
  MReportHandler,
  MReportResult,
  Type,
} from '../types'
import { ObservableBus } from './observable.bus'

export type MykoReportHandlerType = Type<MReportHandler<MReport<unknown>>>

/**
 * Abstract class representing a report bus in the Myko core library.
 * @template T - The type of report.
 */
export abstract class AMykoReportBus extends ObservableBus<MReport<unknown>> {
  constructor() {
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
  abstract watch<T extends MReport<unknown>>(report: T): MLiveReportResult<T>

  /**
   * Executes a report and returns the report result.
   * @param report - The report to execute.
   * @returns The report result.
   */
  execute<U, T extends MReport<U>>(report: T): MReportResult<T> {
    return firstValueFrom(this.watch(report)) as MReportResult<T>
  }

  /**
   * Abstract method to register a report handler.
   * @param handler - The report handler to register.
   */
  protected abstract registerHandler(handler: MykoReportHandlerType): void
}
