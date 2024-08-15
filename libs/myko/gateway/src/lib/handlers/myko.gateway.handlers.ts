import {
  Client,
  ConnectedToLeader,
  DeleteClientsByServerId,
  EntitySearch,
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
  MItem,
  MykoCommandError,
  MykoCommandHandler,
  MykoLogger,
  MykoQueryHandler,
  MykoReportHandler,
  PeerAlive,
  PeerLastSeen,
  Server,
  ServerEventLog,
  eventBus,
  getEvents,
  makeDel,
  onAllInit,
  queryBus,
  repo,
  repoName,
  reportBus,
  type MCommandHandler,
  type MLiveQueryResult,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import {
  ClientCommand,
  PeerCommand,
  PeerQuery,
  wrapCommandOnlyWS,
} from '@myko/ws'
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
import { getServer, getTx } from '../registry'
import { PeerClientRegistry, peers } from '../registry/peer.registry'

onAllInit(() => {
  try {
    getServer()
  } catch (e) {
    new MykoLogger('Gateway Handler').info('Not a Server, Skipping Purge')
    return
  }

  const server = getServer()

  const prev = repo(Server).get({
    address: server.address,
    port: server.port,
  })

  for (const p of prev) {
    eventBus.publishDel(p, 'server-start')
  }

  eventBus.publishSet(server, 'server-start')
})

@MykoQueryHandler(GetServers)
export class GetServersHandler implements MQueryHandler<GetServers> {
  execute(_: GetServers): MLiveQueryResult<GetServers> {
    return repo(Server).watch({})
  }
}

@MykoQueryHandler(GetConnectedServer)
export class GetConnectedServerHandler
  implements MQueryHandler<GetConnectedServer>
{
  execute(_: GetConnectedServer): MLiveQueryResult<GetConnectedServer> {
    return of(getServer()).pipe(map((x) => [x]))
  }
}

@MykoQueryHandler(GetPeerServers)
export class GetPeerServersHandler implements MQueryHandler<GetPeerServers> {
  execute(_: GetPeerServers): MLiveQueryResult<GetPeerServers> {
    return repo(Server).watchFilter(
      (s) => s.id !== getServer().id && s.groupId === getServer().groupId,
    )
  }
}

@MykoQueryHandler(GetServersByQuery)
export class GetServersByQueryHandler
  implements MQueryHandler<GetServersByQuery>
{
  execute(query: GetServersByQuery): MLiveQueryResult<GetServersByQuery> {
    return repo(Server).watch(query.query)
  }
}

@MykoQueryHandler(GetServersByClientIds)
export class GetServersByClientIdsHandler
  implements MQueryHandler<GetServersByClientIds>
{
  execute(
    query: GetServersByClientIds,
  ): MLiveQueryResult<GetServersByClientIds> {
    return repo(Client)
      .watchIds(query.clientIds)
      .pipe(
        map((clients) => clients.map((c) => c.serverId)),
        map(uniq),
        switchMap((serverIds) => repo(Server).watchIds(serverIds)),
      )
  }
}

@MykoCommandHandler(ClientCommand)
export class ClientCommandHandler implements MCommandHandler<ClientCommand> {
  async execute(command: ClientCommand): Promise<void> {
    if (!command.client) {
      throw new MykoCommandError(command.tx, 'Client Not Found')
    }

    if (command.client.serverId !== getServer().id) {
      // forward to server
      console.log('forwarding to server')
      const peer = await firstValueFrom(peers.getPeer(command.client.serverId))

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
    return repo(Client).watchIds(query.ids)
  }
}

@MykoQueryHandler(GetClientsByQuery)
export class GetClientsByQueryHandler
  implements MQueryHandler<GetClientsByQuery>
{
  execute(query: GetClientsByQuery): Observable<any> {
    return repo(Client).watch(query.partial)
  }
}

@MykoCommandHandler(DeleteClientsByServerId)
export class DeleteClientsByServerIdHandler
  implements MCommandHandler<DeleteClientsByServerId>
{
  async execute(command: DeleteClientsByServerId): Promise<void> {
    const clients = repo(Client).get({ serverId: command.serverId })
    eventBus.publishAll(clients.map((c) => makeDel(c, command.tx)))
  }
}

@MykoQueryHandler(PeerQuery)
export class PeerQueryHandler implements MQueryHandler<PeerQuery> {
  execute(query: PeerQuery): MLiveQueryResult<PeerQuery> {
    try {
      if (query.peerId === getServer().id) {
        return queryBus.watch(query.query)
      }

      return peers.getPeer(query.peerId).pipe(
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
    return repoName(query.type).watchIds(query.ids)
  }
}

@MykoReportHandler(GroupLeader)
export class GroupLeaderHandler implements MReportHandler<GroupLeader> {
  execute(_: GroupLeader): Observable<Server> {
    return repo(Server)
      .watch({ groupId: getServer().groupId })
      .pipe(
        map((x) => {
          return x.sort((a, b) => a.startedAt.localeCompare(b.startedAt))[0]
        }),
      )
  }
}

@MykoReportHandler(IsLeader)
export class IsLeaderHandler implements MReportHandler<IsLeader> {
  execute(report: IsLeader): Observable<boolean> {
    return repo(Server)
      .watchId(report.serverId)
      .pipe(
        switchMap((server) =>
          server
            ? reportBus
                .watch(new GroupLeader(server.groupId))
                .pipe(map((leader) => leader?.id === server.id))
            : of(false),
        ),
      )
  }
}

@MykoReportHandler(PeerAlive)
export class PeerAliveHandler implements MReportHandler<PeerAlive> {
  execute(report: PeerAlive) {
    return peers.getPeer(report.peerId).pipe(
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
  execute(report: PeerLastSeen) {
    return reportBus.watch(new PeerAlive(report.peerId)).pipe(
      filter((x) => x !== false),
      map((x) => DateTime.utc().toISO()),
    )
  }
}

@MykoReportHandler(ConnectedToLeader)
export class ConnectedToLeaderHandler
  implements MReportHandler<ConnectedToLeader>
{
  execute(_: ConnectedToLeader) {
    return reportBus.watch(new IsLeader(getServer().id))
  }
}

@MykoReportHandler(ServerEventLog)
export class ServerEventLogHandler implements MReportHandler<ServerEventLog> {
  execute(report: ServerEventLog) {
    return eventBus.subject$.pipe(filter((x) => x.sourceId == getServer().id))
  }
}

@MykoReportHandler(EntitySearch)
export class EntitySearchHandler implements MReportHandler<EntitySearch<any>> {
  execute(report: EntitySearch<any>): Observable<any> {
    if (report.query.length === 0 && report.opts?.showAllOnEmpty) {
      return repoName(report.entityType).watch({})
    }

    return repoName(report.entityType).watchSearch(report.query)
  }
}
