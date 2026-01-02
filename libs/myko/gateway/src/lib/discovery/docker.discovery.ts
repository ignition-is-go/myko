import type { Server } from '@myko/core'
import * as dns from 'node:dns/promises'

export const startDockerDiscovery = (
  onDiscovered: (server: Pick<Server, 'address' | 'port'>) => void,
) => {
  const serviceName = process.env.MYKO_SERVICE_NAME
  const POLL_INTERVAL = process.env.MYKO_DOCKER_POLL_INTERVAL || 15000

  if (!serviceName) {
    throw new Error('Service Name Not Specified')
  }

  setInterval(
    () => discoverPeers(serviceName, onDiscovered),
    Number(POLL_INTERVAL),
  )
}

let peers: string[] = []

async function discoverPeers(
  serviceName: string,
  onDiscovered: Parameters<typeof startDockerDiscovery>[0],
) {
  try {
    // Resolve all A records for tasks.<service_name>
    const records = await dns.resolve4(`tasks.${serviceName}`)

    const MYKO_PORT = process.env.MYKO_PORT

    if (!MYKO_PORT) {
      throw new Error('MYKO_PORT not specified')
    }

    peers = [...new Set(records)] // Deduplicate

    const servers = peers.map((address) => ({
      address,
      port: Number(MYKO_PORT),
    }))

    servers.forEach(onDiscovered)
  } catch (err) {
    console.error('Discovery error:', err)
    peers = []
  }
}
