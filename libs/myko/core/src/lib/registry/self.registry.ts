import { v4 } from 'uuid'
import type { Server } from '../modules'
import type { ID } from '../types'

let hostId: ID | null = null
let server: Server | null = null

export const getServer = (): Server => {
  if (!server) {
    throw new Error('Server not initialized')
  }

  return server
}

export const setServer = (s: Server): void => {
  if (server) {
    throw new Error('Server already initialized')
  }
  server = s
}

export const clearServer = (): void => {
  server = null
}

export const getHostId = (): string => {
  if (!hostId) {
    hostId = v4()
  }

  return hostId
}
