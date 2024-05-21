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
            onDisconnect: (_, willAttemptReconnect) => {
              log.debug(
                `Peer Disconnected: ${server.address}:${server.port} ${server.id}`,
              )

              if (!willAttemptReconnect) {
                this.events.publishDel(server, 'peer-disconnected')
                this.peers.delete(serverId)
              }
            },
            onConnect: () => {
              log.debug(
                `Connected to Peer: ${server.address}:${server.port} ${server.id},`,
              )
            },
          },
          { reconnect: true, maxReconnectAttempts: 30, secure: false },
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
