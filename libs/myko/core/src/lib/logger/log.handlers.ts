import { map, type Observable } from 'rxjs'
import { MykoQueryHandler, MykoReportHandler } from '../decorators'
import { repo } from '../registry'
import type { MQueryHandler, MReportHandler } from '../types'
import { GetLogs, Log, Loggers } from './log.type'
import { nameStream } from './registry'

@MykoQueryHandler(GetLogs)
export class GetLogsHandler implements MQueryHandler<GetLogs> {
  execute(query: GetLogs): Observable<Log[]> {
    return repo(Log)
      .watch({ serverId: query.serverId })
      .pipe(
        map((x) => x.sort((a, b) => a.timestamp.localeCompare(b.timestamp))),
      )
  }
}

@MykoReportHandler(Loggers)
export class LoggersHandler implements MReportHandler<Loggers> {
  execute(): Observable<string[]> {
    return nameStream.pipe(map((x) => Array.from(x.keys()).sort()))
  }
}
