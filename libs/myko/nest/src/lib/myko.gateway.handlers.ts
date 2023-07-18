import {
  MykoQueryHandler,
  GetServers,
  MQueryHandler,
  ServerRepo,
  MLiveQueryResult,
  GetConnectedServer,
  GetServersByQuery,
  Server,
  MykoCommandHandler,
  MCommandHandler,
  MykoProtocol,
  GetClientsByIds,
  ClientRepo,
  wrapCommand,
} from '@myko/core'
import { Inject, Injectable } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { Observable, map } from 'rxjs'
import { MykoCommandError, SERVER_TOKEN } from '../types'
import { ClientCommand, wrapCommandOnlyWS } from '@myko/ws'
import { encoders, clientProtocols } from './registry/client.protocols'
import { SocketRegistry } from './registry/socket.registry'
import { peerRegistry } from './registry/peer.registry'

@MykoQueryHandler(GetServers)
export class GetServersHandler implements MQueryHandler<GetServers> {
  constructor(private repo: ServerRepo) {}

  execute(query: GetServers): MLiveQueryResult<GetServers> {
    return this.repo.watch({})
  }
}

@MykoQueryHandler(GetConnectedServer)
export class GetConnectedServerHandler
  implements MQueryHandler<GetConnectedServer>
{
  constructor(
    private repo: ServerRepo,
    private config: ConfigService,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}

  execute(query: GetConnectedServer): MLiveQueryResult<GetConnectedServer> {
    return this.repo.watchId(this.server.id).pipe(map((x) => [x]))
  }
}

@MykoQueryHandler(GetServersByQuery)
export class GetServersByQueryHandler
  implements MQueryHandler<GetServersByQuery>
{
  constructor(private repo: ServerRepo) {}

  execute(query: GetServersByQuery): MLiveQueryResult<GetServersByQuery> {
    return this.repo.watch(query.query)
  }
}

@Injectable()
export class ServerSagas {
  constructor(private repo: ServerRepo) {}
}

@MykoCommandHandler(ClientCommand)
export class ClientCommandHandler implements MCommandHandler<ClientCommand> {
  constructor(
    private reg: SocketRegistry,
    private clients: ClientRepo,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}

  async execute(command: ClientCommand): Promise<void> {
    const client = this.clients.getId(command.clientId)

    if (client.serverId !== this.server.id) {
      // forward to server
      const peer = peerRegistry.getPeer(client.serverId)

      if (!peer) {
        throw new MykoCommandError(command.tx, 'Exec Not Connected')
      }
      return peer.sendCommand(command)
    }

    const sockets = this.reg.get(command.clientId)

    if (!sockets || sockets.size === 0) {
      throw new MykoCommandError(command.tx, 'Exec Not Connected')
    }
    sockets.forEach((socket) => {
      socket.send(
        encoders.get(clientProtocols.get(socket) ?? MykoProtocol.JSON)(
          wrapCommandOnlyWS(command.command, this.server.id),
        ),
      )
    })
  }
}

@MykoQueryHandler(GetClientsByIds)
export class GetClientsByIdsHandler implements MQueryHandler<GetClientsByIds> {
  constructor(private repo: ClientRepo) {}
  execute(query: GetClientsByIds): Observable<any> {
    return this.repo.watchIds(query.ids)
  }
}
