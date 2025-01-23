import {
  GetConnectedServer,
  GetPeerServers,
  MykoLogger,
  Server,
  ServerEventLog,
  eventBus,
  getHostId,
  onAllInit,
  queryBus,
  repo,
  type ID,
} from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Subject, filter, firstValueFrom, takeUntil } from 'rxjs'

import { peerBus } from '../bus/peer.bus'
import { startDockerDiscovery } from '../discovery/docker.discovery'
import { startUdpDiscovery } from '../discovery/udp.discovery'
import { getAuth } from './auth.registry'

export class PeerClientRegistry {
  private peerClients = new Map<string, WSMClient>()
  private unsubs = new Subject<ID>()
  private logger = new MykoLogger('Peer Registry')

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

  async addPeer(privateAddress: string, port: number) {
    const client = new WSMClient(
      (url) => new WebSocket(url),
      {
        onTerminated: async () => {
          this.logger.info(`Peer Disconnected - ${privateAddress}:${port}`)

          const cleanup = await repo(Server).get({
            privateAddress: privateAddress,
            port,
          })

          const legacyCleaup = await repo(Server).get({
            address: privateAddress,
            port,
          })

          for (const c of [...cleanup, ...legacyCleaup]) {
            eventBus.publishDel(c, 'disconnected')
            this.peerClients.delete(c.id)
            this.teardownPeerEventListener(c)
          }
        },
        onError: (e) => {
          this.logger.error(
            `Error Connecting to Peer - ${privateAddress}:${port}`,
            e,
          )
        },
        onLog: (...l) => {
          this.logger.info(l.join(' '))
        },
        onServerConnect: (_url) => {
          firstValueFrom(client.watchQuery(new GetConnectedServer())).then(
            async (s) => {
              const connectedServer = s.shift()

              if (!connectedServer) {
                this.logger.error('Connected Server Not Found')
                client.disconnect()
                return
              }

              if (connectedServer.id === getHostId()) {
                this.logger.error('Somehow Connected to Self: Disconnecting')
                client.disconnect()
                return
              }

              const olds = await repo(Server).get({
                privateAddress: connectedServer.privateAddress,
                port: connectedServer.port,
              })

              const legacyOlds = await repo(Server).get({
                address: connectedServer.privateAddress,
                port: connectedServer.port,
              })

              const toDelete = [...olds, ...legacyOlds].filter(
                (x) => x.id !== connectedServer.id,
              )

              for (const o of toDelete) {
                eventBus.publishDel(o, 'disconnected')
                this.peerClients.delete(o.id)
                this.teardownPeerEventListener(o)
              }

              this.peerClients.set(connectedServer.id, client)

              this.assertPeerEventListener(connectedServer)
            },
          )
        },
      },
      { secure: false, reconnect: false, singleSocket: true },
    )

    client.connect(privateAddress, port)
    client.setUser(getAuth().getPeerToken())
  }

  getPeer(serverId: ID): WSMClient | undefined {
    return this.peerClients.get(serverId)
  }

  size() {
    return this.peerClients.size
  }

  all() {
    return this.peerClients.entries()
  }

  async start() {
    const peers = process.env['MYKO_PEERS']

    if (!peers) {
      return
    }

    const peerList = peers.split(',')

    peerList.forEach((p) => {
      const [address, port] = p.split(':')
      new MykoLogger('Environment Peers').info(
        `Found Peer - ${address}:${port}`,
      )
      this.addPeer(address, parseInt(port))
    })
  }
}

export const peers = new PeerClientRegistry()

onAllInit(() => {
  peers.start()

  const ENABLE_UDP_DISCOVERY = process.env['MYKO_ENABLE_UDP_DISCOVERY']

  const ENABLE_DOCKER_DISOCVERY = process.env['MYKO_ENABLE_DOCKER_DISCOVERY']

  if (ENABLE_UDP_DISCOVERY) {
    startUdpDiscovery((server) => {
      new MykoLogger('UDP Discovery').info(
        `Found Peer - ${server.address}:${server.port}`,
      )
      peers.addPeer(server.address, server.port)
    })
  }

  if (ENABLE_DOCKER_DISOCVERY) {
    startDockerDiscovery((d) => {
      new MykoLogger('Docker Discovery').info(
        `Found Peer - ${d.address}:${d.port}`,
      )
      peers.addPeer(d.address, d.port)
    })
  }

  queryBus.watch(new GetPeerServers()).subscribe((q) => {
    for (const server of q) {
      peers.addPeer(server.privateAddress, server.port)
    }
  })
})
