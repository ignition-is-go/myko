import {
  Client,
  ClientCommand,
  ClientStatus,
  DeleteClientsByServerId,
  eventBus,
  GetClientsByIds,
  GetClientsByQuery,
  getHostId,
  liveRepo,
  makeDel,
  type MCommandHandler,
  type MLiveReportResult,
  type MQueryHandler,
  type MReportHandler,
  MykoCommandError,
  MykoCommandHandler,
  MykoQueryHandler,
  MykoReportHandler,
  queryBus,
} from '@myko/core'
import { wrapCommandOnlyWS } from '@myko/ws'
import { map, type Observable } from 'rxjs'
import { getTx, peers } from '../registry'

@MykoCommandHandler(ClientCommand)
export class ClientCommandHandler implements MCommandHandler<ClientCommand> {
  async execute(command: ClientCommand): Promise<void> {
    if (!command.client) {
      throw new MykoCommandError(command.tx, 'Client Not Found')
    }

    if (command.client.serverId !== getHostId()) {
      // forward to server
      console.log('forwarding to server')
      const peer = peers.getPeer(command.client.serverId)

      if (!peer) {
        throw new MykoCommandError(command.tx, 'Peer Not Found')
      }
      return peer.sendCommand(command)
    }

    getTx().next({
      clientId: command.client.id,
      data: wrapCommandOnlyWS(command.command),
    })
  }
}

@MykoQueryHandler(GetClientsByIds)
export class GetClientsByIdsHandler implements MQueryHandler<GetClientsByIds> {
  execute(query: GetClientsByIds): Observable<any> {
    return liveRepo(Client).watchIds(query.ids)
  }
}

@MykoQueryHandler(GetClientsByQuery)
export class GetClientsByQueryHandler
  implements MQueryHandler<GetClientsByQuery>
{
  execute(query: GetClientsByQuery): Observable<any> {
    return liveRepo(Client).watch(query.partial)
  }
}

@MykoCommandHandler(DeleteClientsByServerId)
export class DeleteClientsByServerIdHandler
  implements MCommandHandler<DeleteClientsByServerId>
{
  async execute(command: DeleteClientsByServerId): Promise<void> {
    const clients = await liveRepo(Client).get({ serverId: command.serverId })
    eventBus.publishAll(clients.map((c) => makeDel(c, command.tx)))
  }
}

@MykoReportHandler(ClientStatus)
export class ClientStatusHandler implements MReportHandler<ClientStatus> {
  execute(report: ClientStatus): MLiveReportResult<ClientStatus> {
    return queryBus.watch(new GetClientsByQuery({}).withContext(report)).pipe(
      map((clients) => {
        return {
          online: clients.some((c) => c.id === report.clientId),
        }
      }),
    )
  }
}
