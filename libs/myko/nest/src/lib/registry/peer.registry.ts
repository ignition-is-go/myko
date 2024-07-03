import { Server, ServerEventLog, ServerRepo, type ID } from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Inject, Injectable, OnModuleInit } from '@nestjs/common'
import { Observable, Subscription, map } from 'rxjs'
import WebSocket from 'ws'
import { SERVER_TOKEN } from '../../types'
import { MykoEventBus, PeerEventBus } from '../busses'
import { MykoAuthService } from '../services'

@Injectable()
export class PeerClientRegistry implements OnModuleInit {
  constructor(
    private servers: ServerRepo,
    @Inject(SERVER_TOKEN) private me: Server,
    private auth: MykoAuthService,
    private events: MykoEventBus,
    private peerEvents: PeerEventBus,
  ) {}

  private peers = new Map<string, WSMClient>()
  private peerEventListenerSubs = new Map<string, Subscription>()

  private assertPeerEventListener(server: Server) {
    const key = makePeerKey(server)

    if (this.peerEventListenerSubs.has(key)) {
      return
    }

    if (!this.peers.has(key)) {
      return
    }

    const client = this.peers.get(key)

    const sub = client.watchReport(new ServerEventLog()).subscribe((e) => {
      this.peerEvents.next(e)
    })

    this.peerEventListenerSubs.set(key, sub)
  }

  private teardownPeerEventListener(server: Server) {
    const key = makePeerKey(server)

    if (!this.peerEventListenerSubs.has(key)) {
      return
    }

    const sub = this.peerEventListenerSubs.get(key)
    sub.unsubscribe()
    this.peerEventListenerSubs.delete(key)
  }

  onModuleInit() {
    this.servers
      .watchFilter(
        (s) =>
          s.groupId === this.me.groupId &&
          (s.address !== this.me.address || s.port !== this.me.port),
      )
      .pipe()
      .subscribe((e) => {
        e.forEach((s) => this.assertClient(s))
      })
  }

  private assertClient(server: Server) {
    if (this.peers.has(makePeerKey(server))) {
      return this.peers.get(makePeerKey(server))
    }

    const client = new WSMClient(
      server.address,
      server.port,
      (url) => new WebSocket(url, { timeout: 1000 }),
      {
        onDisconnect: (_, willAttemptReconnect) => {
          this.events.publishDel(server, 'peer-disconnected')
          this.peers.delete(makePeerKey(server))
          this.teardownPeerEventListener(server)
        },
        onConnect: () => {
          console.debug(
            `Connected to Peer: ${server.address}:${server.port} ${server.id},`,
          )
        },
      },
      { reconnect: true, maxReconnectAttempts: 5, secure: false },
    )

    client.setUser(this.auth.getPeerToken())

    this.peers.set(makePeerKey(server), client)

    this.assertPeerEventListener(server)

    return client
  }

  getPeer(serverId: ID): Observable<WSMClient | undefined> {
    const obs: Observable<WSMClient | undefined> = this.servers
      .watchId(serverId)
      .pipe(
        map((server) => {
          if (!server) {
            console.debug(`Peer Not Found or Disappeared - ${serverId}`)
            return undefined
          }

          return this.assertClient(server)
        }),
      )

    return obs
  }

  size() {
    return this.peers.size
  }

  all() {
    return this.peers.entries()
  }
}

const makePeerKey = (server: Server) => `${server.address}:${server.port}`
