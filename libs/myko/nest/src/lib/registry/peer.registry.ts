import { ID, Server } from '@myko/core'
import { WSMClient } from '@myko/ws'
import * as WebSocket from 'ws'

export class PeerRegistry {
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
      me.id,
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

  getPeer(id: ID) {
    return this.peers.get(id)
  }

  size() {
    return this.peers.size
  }

  all() {
    return this.peers.entries()
  }
}

export const peerRegistry = new PeerRegistry()
