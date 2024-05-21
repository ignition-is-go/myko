import { ID, Server, ServerRepo } from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Inject, Injectable } from '@nestjs/common'
import { LoggerService } from '@rship/logging'
import { Observable, map } from 'rxjs'
import * as WebSocket from 'ws'
import { SERVER_TOKEN } from '../../types'
import { MykoEventBus } from '../busses'
import { MykoAuthService } from '../services'

@Injectable()
export class PeerClientRegistry {
  constructor(
    private servers: ServerRepo,
    @Inject(SERVER_TOKEN) private me: Server,
    private auth: MykoAuthService,
    private events: MykoEventBus,
    private logger: LoggerService,
  ) {}

  private peers = new Map<ID, WSMClient>()

  assertPeer(
    server: Server,
    me: Server,
    token: string,
    {
      onConnect,
      onDisconnect,
    }: { onConnect: () => void; onDisconnect: () => void },
  ) {
    if (this.peers.has(server.id)) {
      return
    }

    const client = new WSMClient(
      server.address,
      server.port,
      (url) => new WebSocket(url),
      {
        onDisconnect: () => {
          onDisconnect()
        },
        onConnect: () => {
          onConnect()
        },
      },
      { reconnect: false, secure: false },
    )

    client.setUser(token)

    this.peers.set(server.id, client)
  }

  getPeer(serverId: ID): Observable<WSMClient | null> {
    const log = this.logger.getLogger('getPeer').dev

    return this.servers.watchId(serverId).pipe(
      map((server: Server | undefined) => {
        if (this.peers.has(serverId)) {
          return this.peers.get(serverId)
        }

        if (!server) {
          return undefined
        }

        log.debug(
          `Connecting to Peer: ${server.address}:${server.port} ${server.id},`,
        )

        const client = new WSMClient(
          server.address,
          server.port,
          (url) => new WebSocket(url),
          {
            onDisconnect: () => {
              log.debug(
                `Peer Disconnected: ${server.address}:${server.port} ${server.id}`,
              )
              this.events.publishDel(server, 'peer-disconnected')
              this.peers.delete(serverId)
            },
            onConnect: () => {
              log.debug(
                `Connected to Peer: ${server.address}:${server.port} ${server.id},`,
              )
            },
          },
          { reconnect: false, secure: false },
        )

        client.setUser(this.auth.getPeerToken())

        this.peers.set(serverId, client)

        return client
      }),
    )
  }

  size() {
    return this.peers.size
  }

  all() {
    return this.peers.entries()
  }
}
