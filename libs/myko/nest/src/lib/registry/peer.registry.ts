import { ID, Server } from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Observable, ReplaySubject, Subject, shareReplay, tap } from 'rxjs'
import * as WebSocket from 'ws'

export class PeerRegistry {
  private peers = new Map<ID, WSMClient>()

  private sub = new ReplaySubject<WSMClient[]>(1)

  private publish() {
    this.sub.next([...this.peers.values()])
  }

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
      me.id,
      (url) => new WebSocket(url),
      {
        onDisconnect: () => {
          onDisconnect()
          const sizeBefore = this.peers.size
          this.peers.delete(server.id)
          if (sizeBefore !== this.peers.size) {
            this.publish()
          }
        },
        onConnect: () => {
          onConnect()
          this.publish()
        },
      },
    )

    client.setUser(token)

    this.peers.set(server.id, client)
  }

  getPeer(id: ID) {
    return this.peers.get(id)
  }

  getAllPeers(): Observable<WSMClient[]> {
    return this.sub.pipe()
  }
}

export const peerRegistry = new PeerRegistry()
