import {
  GetConnectedServer,
  GetPeerServers,
  MykoLogger,
  RegisterPeer,
  Server,
  ServerEventLog,
  eventBus,
  getServer,
  onAllInit,
  repo,
  type ID,
} from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Observable, Subscription, firstValueFrom, map } from 'rxjs'
import WebSocket from 'ws'

import { peerBus } from '../bus/peer.bus'
import { getAuth } from './auth.registry'

export class PeerClientRegistry {
  private peers = new Map<string, WSMClient>()
  private peerEventListenerSubs = new Map<string, Subscription>()
  private gossipSubs = new Map<string, Subscription>()

  private assertPeerEventListener(address: string, port: number) {
    const key = makePeerKey(address, port)

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

    const gossipSub = client.watchQuery(new GetPeerServers()).subscribe((s) => {
      for (const server of s) {
        if (server.id === getServer().id) {
          continue
        }

        this.assertPeer(server.address, server.port)
      }
    })

    this.peerEventListenerSubs.set(key, sub)
    this.gossipSubs.set(key, gossipSub)
  }

  private teardownPeerEventListener(server: Server) {
    const key = makePeerKey(server.address, server.port)

    if (this.gossipSubs.has(key)) {
      const sub = this.gossipSubs.get(key)
      sub.unsubscribe()
      this.gossipSubs.delete(key)
    }

    if (this.peerEventListenerSubs.has(key)) {
      const sub = this.peerEventListenerSubs.get(key)
      sub.unsubscribe()
      this.peerEventListenerSubs.delete(key)
    }

    this.peers.delete(key)
  }

  onModuleInit() {}

  assertPeer(address: string, port: number) {
    if (this.peers.has(makePeerKey(address, port))) {
      return this.peers.get(makePeerKey(address, port))
    }

    const client = new WSMClient(
      (url) => new WebSocket(url, { timeout: 1000 }),
      {
        onTerminated: () => {
          new MykoLogger('Peer Registry').info(
            `Peer Disconnected - ${address}:${port}`,
          )

          const server = repo(Server).get({ address, port: port }).shift()

          if (!server) {
            return
          }

          eventBus.publishDel(server, 'disconnected')

          this.teardownPeerEventListener(server)
        },
        onError: (e) => {
          new MykoLogger('Peer Registry').error(
            `Error Connecting to Peer - ${address}:${port}`,
            e,
          )
        },
        onLog: (...l) => {
          new MykoLogger('Peer Registry').info(...l)
        },
        onServerConnect: (url) => {
          firstValueFrom(client.watchQuery(new GetConnectedServer())).then(
            (s) => {
              const server = s.shift()

              if (!s) {
                return
              }
              eventBus.publishSet(server, 'peer-connected')
            },
          )

          client.sendCommand(new RegisterPeer(getServer()))
        },
      },
      { secure: false, reconnect: true, singleSocket: true },
    )

    client.connect(address, port)
    client.setUser(getAuth().getPeerToken())

    this.peers.set(makePeerKey(address, port), client)

    this.assertPeerEventListener(address, port)

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

          return this.assertPeer(server.address, server.port)
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
    const peers = process.env['MYKO_PEERS']
    new MykoLogger('Peer Registry').info('Starting with Peers:', peers)

    if (!peers) {
      return
    }

    const peerList = peers.split(',')

    peerList.forEach((p) => {
      const [address, port] = p.split(':')
      this.assertPeer(address, parseInt(port))
    })
  }
}

const makePeerKey = (address: string, port: number) => `${address}:${port}`

export const peers = new PeerClientRegistry()

onAllInit(() => {
  peers.start()
})
