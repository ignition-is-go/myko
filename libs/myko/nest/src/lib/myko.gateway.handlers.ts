import {
  ClientRepo,
  ConnectedToLeader,
  DeleteClientsByServerId,
  EventContainer,
  GetClientsByIds,
  GetClientsByQuery,
  GetConnectedServer,
  GetEventLog,
  GetItemsByTypeAndIds,
  GetPeerServers,
  GetServers,
  GetServersByClientIds,
  GetServersByQuery,
  GroupLeader,
  IsLeader,
  MCommandHandler,
  MItem,
  MLiveQueryResult,
  MQueryHandler,
  MReportHandler,
  MykoCommandHandler,
  MykoProtocol,
  MykoQueryHandler,
  MykoReportHandler,
  PeerAlive,
  PeerLastSeen,
  Server,
  ServerEventLog,
  ServerRepo,
  getEvents,
  makeDel,
} from '@myko/core'
import { watchIds } from '@myko/core/src/lib/registry'
import {
  ClientCommand,
  PeerCommand,
  PeerQuery,
  wrapCommandOnlyWS,
} from '@myko/ws'
import { Inject, Injectable } from '@nestjs/common'
import { DateTime } from 'luxon'
import { uniq } from 'ramda'
import {
  Observable,
  combineLatest,
  debounceTime,
  filter,
  firstValueFrom,
  interval,
  map,
  of,
  startWith,
  switchMap,
} from 'rxjs'
import { MykoCommandError, SERVER_TOKEN } from '../types'
import { MykoEventBus, MykoQueryBus, MykoReportBus } from './busses'
import { clientProtocols, encoders } from './registry/client.protocols'
import { PeerClientRegistry } from './registry/peer.registry'
import { SocketRegistry } from './registry/socket.registry'

@MykoQueryHandler(GetServers)
export class GetServersHandler implements MQueryHandler<GetServers> {
  constructor(private repo: ServerRepo) {}

  execute(_: GetServers): MLiveQueryResult<GetServers> {
    return this.repo.watch({})
  }
}

@MykoQueryHandler(GetConnectedServer)
export class GetConnectedServerHandler
  implements MQueryHandler<GetConnectedServer>
{
  constructor(@Inject(SERVER_TOKEN) private server: Server) {}

  execute(_: GetConnectedServer): MLiveQueryResult<GetConnectedServer> {
    return of(this.server).pipe(map((x) => [x]))
  }
}

@MykoQueryHandler(GetPeerServers)
export class GetPeerServersHandler implements MQueryHandler<GetPeerServers> {
  constructor(
    private repo: ServerRepo,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}

  execute(_: GetPeerServers): MLiveQueryResult<GetPeerServers> {
    return this.repo.watchFilter(
      (s) => s.id !== this.server.id && s.groupId === this.server.groupId,
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
  constructor(
    private servers: ServerRepo,
    private clients: ClientRepo,
  ) {}

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
    private peers: PeerClientRegistry,
  ) {}

  async execute(command: ClientCommand): Promise<void> {
    const client = this.clients.getId(command.clientId)

    if (!client) {
      throw new MykoCommandError(command.tx, 'Client Not Found')
    }

    if (client.serverId !== this.server.id) {
      // forward to server
      console.log('forwarding to server')
      const peer = await firstValueFrom(this.peers.getPeer(client.serverId))

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
        wrapCommandOnlyWS(command.command),
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

@MykoCommandHandler(DeleteClientsByServerId)
export class DeleteClientsByServerIdHandler
  implements MCommandHandler<DeleteClientsByServerId>
{
  constructor(
    private clients: ClientRepo,
    private events: MykoEventBus,
  ) {}
  async execute(command: DeleteClientsByServerId): Promise<void> {
    const clients = this.clients.get({ serverId: command.serverId })
    this.events.publishAll(clients.map((c) => makeDel(c, command.tx)))
  }
}

@MykoQueryHandler(PeerQuery)
export class PeerQueryHandler implements MQueryHandler<PeerQuery> {
  constructor(
    @Inject(SERVER_TOKEN) private server: Server,
    private query: MykoQueryBus,
    private peers: PeerClientRegistry,
  ) {}
  execute(query: PeerQuery): MLiveQueryResult<PeerQuery> {
    try {
      if (query.peerId === this.server.id) {
        return this.query.watch(query.query)
      }

      return this.peers.getPeer(query.peerId).pipe(
        switchMap((peer) => {
          if (!peer) {
            return of([])
          }
          return peer.watchQuery(query.query)
        }),
      )
    } catch (e) {
      console.warn('Cant Execute Peer Query', query)
      return of([])
    }
  }
}

@MykoCommandHandler(PeerCommand)
export class PeerCommandHandler implements MCommandHandler<PeerCommand> {
  constructor(private peers: PeerClientRegistry) {}
  async execute(command: PeerCommand): Promise<void> {
    const peer = await firstValueFrom(this.peers.getPeer(command.peerId))
    if (!peer) {
      throw new MykoCommandError(command.tx, 'Peer Not Found')
    }

    peer.sendCommand(command.command)
  }
}

@MykoQueryHandler(GetEventLog)
export class GetEventLogHandler implements MQueryHandler<GetEventLog> {
  execute(query: GetEventLog): Observable<EventContainer[]> {
    const time = query.time

    const all = [...getEvents.values()]

    return combineLatest(
      all.map((fn) => fn(time).pipe(startWith([] as EventContainer[]))),
    ).pipe(
      map((x) =>
        x
          .flat()
          .filter((x) => x.event.tx !== undefined)
          .sort((a, b) => a.id.localeCompare(b.id)),
      ),
      debounceTime(50),
    )
  }
}

@MykoQueryHandler(GetItemsByTypeAndIds)
export class GetItemByTypeAndIdHandler
  implements MQueryHandler<GetItemsByTypeAndIds>
{
  constructor() {}
  execute(query: GetItemsByTypeAndIds): Observable<MItem[]> {
    const watchTypeByIds = watchIds.get(query.type)
    return watchTypeByIds(query.ids)
  }
}

@MykoReportHandler(GroupLeader)
export class GroupLeaderHandler implements MReportHandler<GroupLeader> {
  constructor(
    private servers: ServerRepo,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}
  execute(_: GroupLeader): Observable<Server> {
    return this.servers.watch({ groupId: this.server.groupId }).pipe(
      map((x) => {
        return x.sort((a, b) => a.startedAt.localeCompare(b.startedAt))[0]
      }),
    )
  }
}

@MykoReportHandler(IsLeader)
export class IsLeaderHandler implements MReportHandler<IsLeader> {
  constructor(
    private servers: ServerRepo,
    private report: MykoReportBus,
  ) {}
  execute(report: IsLeader): Observable<boolean> {
    return this.servers
      .watchId(report.serverId)
      .pipe(
        switchMap((server) =>
          server
            ? this.report
                .watch(new GroupLeader(server.groupId))
                .pipe(map((leader) => leader?.id === server.id))
            : of(false),
        ),
      )
  }
}

@MykoReportHandler(PeerAlive)
export class PeerAliveHandler implements MReportHandler<PeerAlive> {
  constructor(
    private query: MykoQueryBus,
    private peers: PeerClientRegistry,
  ) {}
  execute(report: PeerAlive) {
    return this.peers.getPeer(report.peerId).pipe(
      switchMap((peer) => {
        return peer
          ? interval(1000).pipe(
              switchMap((_) => peer.ping().catch((e) => false)),
            )
          : of(false)
      }),
    ) as Observable<number | false>
  }
}

@MykoReportHandler(PeerLastSeen)
export class PeerLastSeenHandler implements MReportHandler<PeerLastSeen> {
  constructor(
    private query: MykoQueryBus,
    private report: MykoReportBus,
  ) {}
  execute(report: PeerLastSeen) {
    return this.report.watch(new PeerAlive(report.peerId)).pipe(
      filter((x) => x !== false),
      map((x) => DateTime.utc().toISO()),
    )
  }
}

@MykoReportHandler(ConnectedToLeader)
export class ConnectedToLeaderHandler
  implements MReportHandler<ConnectedToLeader>
{
  constructor(
    private report: MykoReportBus,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}
  execute(_: ConnectedToLeader) {
    return this.report.watch(new IsLeader(this.server.id))
  }
}

@MykoReportHandler(ServerEventLog)
export class ServerEventLogHandler implements MReportHandler<ServerEventLog> {
  constructor(
    private events: MykoEventBus,
    private query: MykoQueryBus,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}
  execute(report: ServerEventLog) {
    return this.events.subject$.pipe(
      filter((x) => x.sourceId == this.server.id),
    )
  }
}
