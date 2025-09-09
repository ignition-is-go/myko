import {
  GetClientsByQuery,
  MykoCommandError,
  MykoCommandHandler,
  MykoQueryHandler,
  MykoReportHandler,
  PeerCommand,
  PeerQuery,
  PeerReport,
  getHostId,
  queryBus,
  type MCommandHandler,
  type MLiveQueryResult,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import { EMPTY, Observable, switchMap } from 'rxjs'
import { peers } from '../registry/peer.registry'

@MykoQueryHandler(PeerQuery)
export class PeerQueryHandler implements MQueryHandler<PeerQuery> {
  execute(query: PeerQuery): MLiveQueryResult<PeerQuery> {
    try {
      if (query.peerId === getHostId()) {
        return queryBus.watch(query.query.withContext(query))
      }

      const peer = peers.getPeer(query.peerId)

      if (!peer) {
        return EMPTY
      }

      const clients = queryBus.watch(
        new GetClientsByQuery({ serverId: getHostId() }).withContext(query),
      )

      return clients.pipe(
        switchMap((cs) => {
          if (cs.find((x) => x.id === query.commandClientId)) {
            return peer.watchQuery(query.query)
          }

          return EMPTY
        }),
      )
    } catch (e) {
      console.warn('Cant Execute Peer Query', query)
      return EMPTY
    }
  }
}

@MykoCommandHandler(PeerCommand)
export class PeerCommandHandler implements MCommandHandler<PeerCommand> {
  constructor() {}
  async execute(command: PeerCommand): Promise<void> {
    const peer = peers.getPeer(command.peerId)
    if (!peer) {
      throw new MykoCommandError(
        command.tx,
        'Peer Not Found for ' + command.command.getTag(),
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
export class PeerReportHandler implements MReportHandler<PeerReport<any>> {
  execute(report: PeerReport<any>): Observable<any> {
    const peer = peers.getPeer(report.peerId)

    if (!peer) {
      return EMPTY
    }

    const clients = queryBus.watch(
      new GetClientsByQuery({ serverId: getHostId() }).withContext(report),
    )

    return clients.pipe(
      switchMap((cs) => {
        if (cs.find((x) => x.id === report.commandClientId)) {
          return peer.watchReport(report.report)
        } else {
          return EMPTY
        }
      }),
    )
  }
}
