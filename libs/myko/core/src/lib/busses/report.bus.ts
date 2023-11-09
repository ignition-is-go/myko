import 'reflect-metadata'
import { firstValueFrom } from 'rxjs'
import {
  Type,
  MItem,
  MReport,
  MReportHandler,
  MReportResult,
  MLiveReportResult,
} from '../types'
import { ObservableBus } from './observable.bus'

export type MykoReportHandlerType = Type<MReportHandler<MReport<unknown>>>

export abstract class AMykoReportBus extends ObservableBus<MReport<unknown>> {
  constructor() {
    super()
  }

  protected handlers = new Map<string, MReportHandler<MReport<unknown>>>()

  protected bind<U>(handler: MReportHandler<MReport<U>>, id: string): void {
    this.handlers.set(id, handler)
  }

  register(handlers: MykoReportHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  abstract watch<T extends MReport<unknown>>(report: T): MLiveReportResult<T>

  execute<U, T extends MReport<U>>(report: T): MReportResult<T> {
    return firstValueFrom(this.watch(report)) as MReportResult<T>
  }

  protected abstract registerHandler(handler: MykoReportHandlerType): void
}
