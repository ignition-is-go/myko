import {
  commandBus,
  getHostId,
  GetLogs,
  liveRepo,
  Log,
  Loggers,
  type LogLevel,
  type MCommandHandler,
  type MQueryHandler,
  type MReportHandler,
  MykoCommandHandler,
  MykoQueryHandler,
  MykoReportHandler,
  PeerCommand,
  PeerReport,
  reportBus,
  ServerLogLevel,
  SetLogLevel,
} from '@myko/core'
import { logFilter, nameStream } from '@myko/core/src/lib/logger/registry'
import { map, type Observable } from 'rxjs'

@MykoQueryHandler(GetLogs)
export class GetLogsHandler implements MQueryHandler<GetLogs> {
  execute(query: GetLogs): Observable<Log[]> {
    return liveRepo(Log)
      .watch({ serverId: query.serverId })
      .pipe(map((x) => x.sort((a, b) => a.timestamp.localeCompare(b.timestamp))))
  }
}

@MykoReportHandler(Loggers)
export class LoggersHandler implements MReportHandler<Loggers> {
  execute(): Observable<string[]> {
    return nameStream.pipe(map((x) => Array.from(x.keys()).sort()))
  }
}

@MykoCommandHandler(SetLogLevel)
export class SetLogLevelHandler implements MCommandHandler<SetLogLevel> {
  async execute(command: SetLogLevel): Promise<void> {
    if (command.serverId === getHostId()) {
      logFilter.set(command.level)
      return
    }

    commandBus.execute(new PeerCommand(command, command.serverId).withContext(command))
  }
}

@MykoReportHandler(ServerLogLevel)
export class ServerLogLevelHandler implements MReportHandler<ServerLogLevel> {
  execute(report): Observable<LogLevel> {
    if (report.serverId === getHostId()) {
      return logFilter.stream()
    }

    return reportBus.watch(new PeerReport(report, report.serverId).withContext(report)) as Observable<LogLevel>
  }
}
