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
  serverSchema,
  type ID,
} from '@myko/core'
import { WSMClient } from '@myko/ws'
import { Subject, filter, firstValueFrom, takeUntil } from 'rxjs'

import { peerBus } from '../bus/peer.bus'
import { getAuth } from './auth.registry'
import { startDockerDiscovery } from './discovery/docker.discovery'
import { startUdpDiscovery } from './discovery/udp.discovery'

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

    client
      .watchQuery(new GetPeerServers())
      .pipe(takeUntil(unsub))
      .subscribe((s) => {
        for (const server of s) {
          if (server.id === getServer().id) {
            continue
          }

          this.addPeer(server.address, server.port)
        }
      })
  }

  private teardownPeerEventListener(server: Server) {
    this.unsubs.next(server.id)
    this.peerClients.delete(server.id)
  }

  async addPeer(address: string, port: number) {
    const url = `http://${address}:${port}/server`

    this.logger.info(`Fetching Peer Info - ${url}`)

    const serverInfo = await fetch(`http://${address}:${port}/server`)
      .then((x) => x.json())
      .catch((e) => {
        this.logger.error(`Error Fetching Peer Info - ${address}:${port}`, e)
        return null
      })

    if (!serverInfo) {
      return
    }

    const parsed = serverSchema.safeParse(serverInfo)

    if (!parsed.success) {
      this.logger.error(
        `Peer Server Info Invalid - ${address}:${port}`,
        parsed.error.format(),
      )
      return
    }

    const server = parsed.data as Server

    if (server.id === getServer().id) {
      this.logger.info(`Peer is Self - ${address}:${port}`)
      return
    }

    if (this.peerClients.has(server.id)) {
      this.logger.info(
        `Peer Already Connected - ${address}:${port} [${server.id}]`,
      )
      return
    }

    this.logger.info(`Connecting to Peer - ${address}:${port} [${server.id}]`)

    const client = new WSMClient(
      (url) => new WebSocket(url),
      {
        onTerminated: async () => {
          this.logger.info(`Peer Disconnected - ${address}:${port}`)

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
          this.logger.error(`Error Connecting to Peer - ${address}:${port}`, e)
        },
        onLog: (...l) => {
          this.logger.info(l.join(' '))
        },
        onServerConnect: (_url) => {
          firstValueFrom(client.watchQuery(new GetConnectedServer())).then(
            (s) => {
              const connected = s.shift()

              if (!connected) {
                this.logger.error('Connected Server Not Found')
                client.disconnect()
                return
              }

              const me = getServer()

              if (connected.id === me.id) {
                this.logger.error('Somehow Connected to Self: Disconnecting')
                client.disconnect()
                return
              }

              if (connected.id !== server.id) {
                this.logger.error('Connected to Wrong Server: Disconnecting')
                client.disconnect()
                return
              }

              this.peerClients.set(connected.id, client)

              this.assertPeerEventListener(connected)

              eventBus.publishSet(connected, 'peer-connected')
            },
          )

          client.sendCommand(new RegisterPeer(getServer()))
        },
      },
      { secure: false, reconnect: true, singleSocket: true },
    )

    client.connect(address, port)
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

  start() {
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
})
