import {
  GetConnectedServer,
  GetPeerServers,
  MykoLogger,
  Server,
  ServerEventLog,
  eventBus,
  onAllInit,
  queryBus,
  repo,
  type ID,
} from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Subject, filter, firstValueFrom, takeUntil } from 'rxjs'

import { peerBus } from '../bus/peer.bus'
import { getAuth } from './auth.registry'

export class PeerClientRegistry {
  private peerClients = new Map<string, WSMClient>()
  private unsubs = new Subject<ID>()
  private logger = new MykoLogger('Peer Registry')

  private connectedAddresses = new Set<string>()

  private assertPeerEventListener(server: Server) {
    const key = server.id

    if (!this.peerClients.has(key)) {
      return
    }

    const client = this.peerClients.get(key)!

    const unsub = this.unsubs.pipe(filter((x) => x === key))

    client
      .watchReport(new ServerEventLog())
      .pipe(takeUntil(unsub))
      .subscribe((e) => {
        peerBus.next(e)
      })
  }

  private teardownPeerEventListener(server: Server) {
    this.unsubs.next(server.id)
    this.peerClients.delete(server.id)
  }

  async addPeer(server: Server) {
    const { address, port, id } = server

    const addressKey = `${address}:${port}`

    if (this.connectedAddresses.has(addressKey)) {
      return
    }

    this.connectedAddresses.add(addressKey)

    const client = new WSMClient(
      (url) => new WebSocket(url),
      {
        onTerminated: async () => {
          this.logger.error(`Peer Terminated - ${address}:${port}`)

          this.connectedAddresses.delete(addressKey)

          this.teardownPeerEventListener(server)

          eventBus.publishDel(server, 'server-disconnected')

          // this fires if the socket fails to open as well, so we know there is no server at this address
        },
        onError: (e) => {
          this.logger.error(`Error Connecting to Peer`, e)
        },
        onLog: (...l) => {
          this.logger.info(l.join(' '))
        },
        onServerConnect: (_url) => {
          firstValueFrom(
            client
              .watchQuery(new GetConnectedServer())
              .pipe(filter((x) => x.length > 0)),
          ).then(async (s) => {
            const connectedServer = s.shift()

            if (!connectedServer) {
              this.logger.error(
                `Connected Server Not Found - ${address}:${port}`,
              )
              client.disconnect()

              return
            }

            if (connectedServer.id !== id) {
              this.logger.error(
                `Connected Server ID Mismatch - ${address}:${port}`,
              )
              client.disconnect()

              eventBus.publishDel(server, 'server-is-old')

              return
            }

            this.logger.info(`Found Peer @ ${address}:${port}`)

            this.peerClients.set(connectedServer.id, client)

            this.assertPeerEventListener(connectedServer)
          })
        },
      },
      { secure: false, reconnect: false, singleSocket: true },
    )

    client.connect(address, port)
    client.setUser(getAuth().getPeerToken())
  }

  getPeer(serverId: ID): WSMClient | undefined {
    return this.peerClients.get(serverId)
  }

  assertPeer(serverId: ID) {
    if (this.peerClients.has(serverId)) {
      return
    }

    repo(Server)
      .getId(serverId)
      .then((server) => {
        if (!server) {
          return
        }

        this.addPeer(server)
      })
  }

  size() {
    return this.peerClients.size
  }

  all() {
    return this.peerClients.entries()
  }
}

export const peers = new PeerClientRegistry()

onAllInit(async () => {
  queryBus.watch(new GetPeerServers()).subscribe((q) => {
    for (const server of q) {
      peers.addPeer(server)
    }
  })
})
