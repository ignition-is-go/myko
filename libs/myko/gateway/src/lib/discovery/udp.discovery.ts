import { getServer, type Server, serverSchema } from '@myko/core'
import { createSocket } from 'node:dgram'
import os from 'node:os'

export const startUdpDiscovery = (onDiscovered: (server: Server) => void) => {
  console.log('Starting Discovery')

  const socket = createSocket('udp4')

  socket.on('message', (msg, _rinfo) => {
    const me = getServer()

    const data = JSON.parse(msg.toString())

    const parsed = serverSchema.safeParse(data)

    if (!parsed.success) {
      return
    }

    const otherServer = parsed.data as Server

    if (otherServer.id === me.id) {
      return
    }

    onDiscovered(otherServer)
  })

  socket.bind(5153, undefined, () => {
    console.log('Listening')

    try {
      socket.setBroadcast(true)
    } catch (e) {
      console.error('Cannot Set to Broadcast', e)
    }

    setInterval(() => {
      for (const address of broadcastAddresses) {
        const me = getServer()
        socket.send(JSON.stringify(me), 5153, address, (err) => {
          if (err) {
            console.error(err)
          }
        })
      }
    }, 1000)
  })

  const broadcastAddresses = getLocalBroadcastAddresses()
}

const getLocalBroadcastAddresses = () => {
  const interfaces = os.networkInterfaces()
  const broadcastAddresses = new Set<string>()

  for (const interfaceName of Object.keys(interfaces)) {
    const addresses = interfaces[interfaceName]
    if (!addresses) continue

    for (const addrInfo of addresses) {
      if (addrInfo.family === 'IPv4' && !addrInfo.internal) {
        const ipParts = addrInfo.address.split('.').map(Number)
        const maskParts = addrInfo.netmask.split('.').map(Number)

        if (ipParts.length !== 4 || maskParts.length !== 4) continue

        const broadcastParts = ipParts.map((ipPart, index) => {
          const mask = maskParts[index]
          return (ipPart | (255 - mask)) & 0xff
        })

        broadcastAddresses.add(broadcastParts.join('.'))
      }
    }
  }

  return Array.from(broadcastAddresses)
}
