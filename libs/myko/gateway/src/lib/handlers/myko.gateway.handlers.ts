import {
  Client,
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
  MItem,
  MykoCommandError,
  MykoCommandHandler,
  MykoLogger,
  MykoQueryHandler,
  MykoReportHandler,
  PeerAlive,
  PeerLastSeen,
  RegisterPeer,
  Server,
  ServerEventLog,
  eventBus,
  getEvents,
  getHostId,
  getServer,
  makeDel,
  onAllInit,
  queryBus,
  repo,
  repoName,
  reportBus,
  type MCommandHandler,
  type MLiveQueryResult,
  type MLiveReportResult,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import {
  ClientCommand,
  ClientStatus,
  PeerCommand,
  PeerQuery,
  PeerReport,
  wrapCommandOnlyWS,
} from '@myko/ws'
import { DateTime } from 'luxon'
import { uniq } from 'ramda'
import {
  EMPTY,
  Observable,
  combineLatest,
  debounceTime,
  filter,
  interval,
  map,
  of,
  startWith,
  switchMap,
} from 'rxjs'
import { getClients, getTx } from '../registry'
import { PeerClientRegistry, peers } from '../registry/peer.registry'

onAllInit(async () => {
  try {
    getServer()
  } catch (e) {
    new MykoLogger('Gateway Handler').info('Not a Server, Skipping Purge')
    return
  }

  const server = getServer()

  const prev = await repo(Server).get({
    privateAddress: server.privateAddress,
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
    return repo(Server).watch({ id: getHostId() })
  }
}

@MykoQueryHandler(GetPeerServers)
export class GetPeerServersHandler implements MQueryHandler<GetPeerServers> {
  execute(_: GetPeerServers): MLiveQueryResult<GetPeerServers> {
    return repo(Server)
      .watchId(getHostId())
      .pipe(
        switchMap((me) => {
          if (!me) {
            return EMPTY
          }
          return repo(Server).watchFilter(
            (s) => s.groupId === me.groupId && s.id !== me.id,
          )
        }),
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
    const clients = await repo(Client).get({ serverId: command.serverId })
    eventBus.publishAll(clients.map((c) => makeDel(c, command.tx)))
  }
}

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

@MykoReportHandler(ClientStatus)
export class ClientStatusHandler implements MReportHandler<ClientStatus> {
  execute(report: ClientStatus): MLiveReportResult<ClientStatus> {
    return getClients().pipe(
      map((clients) => {
        return {
          online: clients.some((c) => c === report.clientId),
        }
      }),
    )
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

@MykoReportHandler(PeerAlive)
export class PeerAliveHandler implements MReportHandler<PeerAlive> {
  execute(report: PeerAlive) {
    return interval(1000).pipe(
      switchMap((_) => {
        const peer = peers.getPeer(report.peerId)
        if (!peer) {
          return of(false) as Observable<number | false>
        }

        return peer.ping().catch((_e) => false)
      }),
    ) as Observable<number | false>
  }
}

@MykoReportHandler(PeerLastSeen)
export class PeerLastSeenHandler implements MReportHandler<PeerLastSeen> {
  execute(report: PeerLastSeen) {
    return reportBus.watch(new PeerAlive(report.peerId)).pipe(
      filter((x) => x !== false),
      map((_x) => DateTime.utc().toISO()),
    )
  }
}

@MykoReportHandler(ServerEventLog)
export class ServerEventLogHandler implements MReportHandler<ServerEventLog> {
  execute(_report: ServerEventLog) {
    return eventBus.subject$.pipe(filter((x) => x.sourceId == getHostId()))
  }
}

@MykoReportHandler(EntitySearch)
export class EntitySearchHandler implements MReportHandler<EntitySearch<any>> {
  execute(report: EntitySearch<any>): Observable<any> {
    return repoName(report.entityType).watchSearch(
      report.query,
      {
        showAllOnEmpty: report.opts?.showAllOnEmpty,
      },
      {
        query: report.filter,
      },
    )
  }
}

@MykoCommandHandler(RegisterPeer)
export class RegisterPeerHandler implements MCommandHandler<RegisterPeer> {
  async execute(command: RegisterPeer) {
    peers.addPeer(command.server.privateAddress, command.server.port)
  }
}
