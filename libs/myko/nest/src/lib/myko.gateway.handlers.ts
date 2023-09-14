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
  GetServersByClientIds,
  GetPeerServers,
  GetClientsByQuery,
} from '@myko/core'
import { Inject, Injectable } from '@nestjs/common'
import { Observable, map, of, switchMap } from 'rxjs'
import { MykoCommandError, SERVER_TOKEN } from '../types'
import {
  ClientCommand,
  PeerCommand,
  PeerQuery,
  wrapCommandOnlyWS,
} from '@myko/ws'
import { encoders, clientProtocols } from './registry/client.protocols'
import { SocketRegistry } from './registry/socket.registry'
import { peerRegistry } from './registry/peer.registry'
import { uniq } from 'ramda'
import { MykoQueryBus } from './busses'
import { LoggerService } from '@rship/logging'

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
  constructor(@Inject(SERVER_TOKEN) private server: Server) {}

  execute(query: GetConnectedServer): MLiveQueryResult<GetConnectedServer> {
    return of(this.server).pipe(map((x) => [x]))
  }
}

@MykoQueryHandler(GetPeerServers)
export class GetPeerServersHandler implements MQueryHandler<GetPeerServers> {
  constructor(
    private repo: ServerRepo,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}

  execute(query: GetPeerServers): MLiveQueryResult<GetPeerServers> {
    return this.repo
      .watchFilter((s) => s.id !== this.server.id)
      .pipe(
        map((x) =>
          x.filter((peer) => peerRegistry.getPeer(peer.id) !== undefined),
        ),
      )
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

@MykoQueryHandler(GetServersByClientIds)
export class GetServersByClientIdsHandler
  implements MQueryHandler<GetServersByClientIds>
{
  constructor(private servers: ServerRepo, private clients: ClientRepo) {}

  execute(
    query: GetServersByClientIds,
  ): MLiveQueryResult<GetServersByClientIds> {
    return this.clients.watchIds(query.clientIds).pipe(
      map((clients) => clients.map((c) => c.serverId)),
      map(uniq),
      switchMap((serverIds) => this.servers.watchIds(serverIds)),
    )
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
        throw new MykoCommandError(command.tx, 'Peer Not Found')
      }
      return peer.sendCommand(command)
    }

    const sockets = this.reg.get(command.clientId)

    if (!sockets) {
      throw new MykoCommandError(command.tx, 'Exec Not Connected')
    }
    sockets.send(
      encoders.get(clientProtocols.get(sockets) ?? MykoProtocol.JSON)(
        wrapCommandOnlyWS(command.command, this.server.id),
      ),
    )
  }
}

@MykoQueryHandler(GetClientsByIds)
export class GetClientsByIdsHandler implements MQueryHandler<GetClientsByIds> {
  constructor(private repo: ClientRepo) {}
  execute(query: GetClientsByIds): Observable<any> {
    return this.repo.watchIds(query.ids)
  }
}

@MykoQueryHandler(GetClientsByQuery)
export class GetClientsByQueryHandler
  implements MQueryHandler<GetClientsByQuery>
{
  constructor(private repo: ClientRepo) {}
  execute(query: GetClientsByQuery): Observable<any> {
    return this.repo.watch(query.partial)
  }
}

@MykoQueryHandler(PeerQuery)
export class PeerQueryHandler implements MQueryHandler<PeerQuery> {
  constructor(
    @Inject(SERVER_TOKEN) private server: Server,
    private query: MykoQueryBus,
    private logger: LoggerService,
  ) {}
  execute(query: PeerQuery): MLiveQueryResult<PeerQuery> {
    if (query.peerId === this.server.id) {
      return this.query.watch(query.query)
    }

    if (!peerRegistry.getPeer(query.peerId)) {
      this.logger.getLogger('PeerQueryHandler').dev.error('Peer Not Found')
      return of([])
    }
    return peerRegistry.getPeer(query.peerId).watchQuery(query.query)
  }
}

@MykoCommandHandler(PeerCommand)
export class PeerCommandHandler implements MCommandHandler<PeerCommand> {
  constructor(@Inject(SERVER_TOKEN) private server: Server) {}
  async execute(command: PeerCommand): Promise<void> {
    const peer = peerRegistry.getPeer(command.peerId)
    if (!peer) {
      throw new MykoCommandError(command.tx, 'Peer Not Found')
    }
    peerRegistry.getPeer(command.peerId).sendCommand(command.command)
  }
}
