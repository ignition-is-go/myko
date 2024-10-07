import {
  MykoLogger,
  Server,
  ServerEventLog,
  eventBus,
  onAllInit,
  repo,
  type ID,
} from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Observable, Subscription, map } from 'rxjs'
import WebSocket from 'ws'

import { getServer } from '@myko/core/src/lib/registry/self.registry'
import { peerBus } from '../bus/peer.bus'
import { getAuth } from './auth.registry'

export class PeerClientRegistry {
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
      peerBus.next(e)
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

  onModuleInit() {}

  private assertClient(server: Server) {
    if (this.peers.has(makePeerKey(server))) {
      return this.peers.get(makePeerKey(server))
    }

    const client = new WSMClient(
      (url) => new WebSocket(url, { timeout: 1000 }),
      {
        onTerminated: () => {
          new MykoLogger('Peer Registry').info(
            `Peer Disconnected - ${server.address}:${server.port}`,
          )

          eventBus.publishDel(server, 'disconnected')

          this.teardownPeerEventListener(server)
        },
        onError: (e) => {
          new MykoLogger('Peer Registry').error(
            `Error Connecting to Peer - ${server.address}:${server.port}`,
            e,
          )
        },
        onLog: (...l) => {
          new MykoLogger('Peer Registry').info(
            `Peer Log - ${server.address}:${server.port}`,
            ...l,
          )
        },
      },
      { secure: false, reconnect: false },
    )

    client.connect(server.address, server.port)
    client.setUser(getAuth().getPeerToken())

    this.peers.set(makePeerKey(server), client)

    this.assertPeerEventListener(server)

    return client
  }

  getPeer(serverId: ID): Observable<WSMClient | undefined> {
    const obs: Observable<WSMClient | undefined> = repo(Server)
      .watchId(serverId)
      .pipe(
        map((server) => {
          if (!server) {
            new MykoLogger('Peer Registry').info(
              `Peer Not Found or Disappeared - ${serverId}`,
            )
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

  start() {
    try {
      getServer()
    } catch (e) {
      new MykoLogger('Peer Registry').info(
        'Not a Gateway, Skipping Peer Registry',
      )
      return
    }

    repo(Server)
      .watchFilter(
        (s) =>
          s.groupId === getServer().groupId &&
          (s.address !== getServer().address || s.port !== getServer().port),
      )
      .pipe()
      .subscribe((e) => {
        e.forEach((s) => this.assertClient(s))
      })
  }
}

const makePeerKey = (server: Server) => `${server.address}:${server.port}`

export const peers = new PeerClientRegistry()

onAllInit(() => {
  peers.start()
})
