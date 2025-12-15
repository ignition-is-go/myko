import {
  GetClientsByQuery,
  getHostId,
  type MReport,
  MykoCommandError,
  MykoCommandHandler,
  MykoLogger,
  MykoQueryHandler,
  MykoReportHandler,
  PeerCommand,
  PeerQuery,
  PeerReport,
  queryBus,
  reportBus,
  type MCommandHandler,
  type MLiveQueryResult,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import { distinctUntilChanged, EMPTY, type Observable, switchMap } from 'rxjs'
import { peers } from '../registry/peer.registry'

@MykoQueryHandler(PeerQuery)
export class PeerQueryHandler implements MQueryHandler<PeerQuery> {
  execute(query: PeerQuery): MLiveQueryResult<PeerQuery> {
    try {
      if (query.peerId === getHostId()) {
        return queryBus.watch(query.query.withContext(query))
      }

      const peer = peers.watchPeer(query.peerId)

      return peer.pipe(
        switchMap((client) => {
          if (!client) {
            return EMPTY
          }
          return client.watchQuery(query.query)
        }),
      )
    } catch (_e) {
      console.warn('Cant Execute Peer Query', query)
      return EMPTY
    }
  }
}

@MykoCommandHandler(PeerCommand)
export class PeerCommandHandler implements MCommandHandler<PeerCommand> {
  async execute(command: PeerCommand): Promise<void> {
    const peer = peers.getPeer(command.peerId)
    if (!peer) {
      throw new MykoCommandError(
        command.tx,
        `Peer Not Found for ${command.command.getTag()}`,
      )
    }

    const clients = await queryBus.execute(
      new GetClientsByQuery({ serverId: getHostId() }).withContext(command),
    )

    if (!clients.find((x) => x.id === command.commandClientId)) {
      return
    }

    peer.sendCommand(command.command)
  }
}
@MykoReportHandler(PeerReport)
export class PeerReportHandler
  implements MReportHandler<PeerReport<MReport<unknown>>>
{
  execute(report: PeerReport<MReport<unknown>>): Observable<unknown> {
    if (report.peerId === getHostId()) {
      return reportBus.watch(report.report.withContext(report))
    }

    const peer = peers.watchPeer(report.peerId)

    const logger = new MykoLogger('PeerReportHandler')

    return peer.pipe(
      distinctUntilChanged(),
      switchMap((peer) => {
        if (!peer) {
          logger.warn(
            `Peer Not Found, returning empty stream for ${report.report.getTag()}`,
          )
          return EMPTY
        }
        logger.info(`returning report for ${report.report.getTag()}`)
        return peer.watchReport(report.report)
      }),
    )
  }
}
