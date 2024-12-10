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
import {
  Observable,
  Subject,
  filter,
  firstValueFrom,
  map,
  takeUntil,
} from 'rxjs'

import { peerBus } from '../bus/peer.bus'
import { getAuth } from './auth.registry'

export class PeerClientRegistry {
  private peers = new Map<string, WSMClient>()
  private unsubs = new Subject<ID>()

  private assertPeerEventListener(address: string, port: number) {
    const key = makePeerKey(address, port)

    if (!this.peers.has(key)) {
      return
    }

    const client = this.peers.get(key)

    const unsub = this.unsubs.pipe(filter((x) => x === key))

    client
      .watchReport(new ServerEventLog())
      .pipe(takeUntil(unsub))
      .subscribe((e) => {
        peerBus.next(e)
      })

    client
      .watchQuery(new GetPeerServers())
      .pipe(takeUntil(unsub))
      .subscribe((s) => {
        for (const server of s) {
          if (server.id === getServer().id) {
            continue
          }

          this.assertPeer(server.address, server.port)
        }
      })
  }

  private teardownPeerEventListener(server: Server) {
    const key = makePeerKey(server.address, server.port)

    this.unsubs.next(key)
    this.peers.delete(key)
  }

  assertPeer(address: string, port: number) {
    if (this.peers.has(makePeerKey(address, port))) {
      return this.peers.get(makePeerKey(address, port))
    }

    const client = new WSMClient(
      (url) => new WebSocket(url),
      {
        onTerminated: async () => {
          new MykoLogger('Peer Registry').info(
            `Peer Disconnected - ${address}:${port}`,
          )

          const server = (
            await repo(Server).get({ address, port: port })
          ).shift()

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
          new MykoLogger('Peer Registry').info(l.join(' '))
        },
        onServerConnect: (_url) => {
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

    if (!peers) {
      new MykoLogger('Peer Registry').info('No Peers Specified')
      return
    }
    new MykoLogger('Peer Registry').info(`Starting with Peers:  ${peers}`)

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
