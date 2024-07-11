import type { Server } from '../modules'

let server: Server | null

export const getServer = (): Server => {
  if (!server) {
    throw new Error('Server not initialized')
  }

  return server
}

export const setServer = (s: Server) => {
  if (server) {
    throw new Error('Server already initialized')
  }
  server = s
}

export const clearServer = () => {
  server = null
}
