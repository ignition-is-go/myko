import { Observable } from 'rxjs'
import { v4 as uuid } from 'uuid'

export class MReport<T> {
  $reportResult: T
  readonly tx: string
  constructor() {
    this.tx = uuid()
  }
}

export type MReportResult<T> = T extends MReport<infer R> ? R : never

export type MLiveReportResult<T> = T extends MReport<infer R>
  ? Observable<R>
  : never

export interface MReportHandler<H> {
  execute(report: H): MLiveReportResult<H>
}
