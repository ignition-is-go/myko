import {
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
import { EMPTY, Observable } from 'rxjs'
import { PeerClientRegistry, peers } from '../registry/peer.registry'

@MykoQueryHandler(PeerQuery)
export class PeerQueryHandler implements MQueryHandler<PeerQuery> {
  execute(query: PeerQuery): MLiveQueryResult<PeerQuery> {
    try {
      if (query.peerId === getHostId()) {
        return queryBus.watch(query.query)
      }

      const peer = peers.getPeer(query.peerId)

      if (!peer) {
        return EMPTY
      }

      return peer.watchQuery(query.query)
    } catch (e) {
      console.warn('Cant Execute Peer Query', query)
      return EMPTY
    }
  }
}

@MykoCommandHandler(PeerCommand)
export class PeerCommandHandler implements MCommandHandler<PeerCommand> {
  constructor(private peers: PeerClientRegistry) {}
  async execute(command: PeerCommand): Promise<void> {
    const peer = this.peers.getPeer(command.peerId)
    if (!peer) {
      throw new MykoCommandError(command.tx, 'Peer Not Found')
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

    return peer.watchReport(report.report)
  }
}
