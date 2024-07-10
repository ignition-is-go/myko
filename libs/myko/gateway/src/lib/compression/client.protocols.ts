import { ID, MykoProtocol } from '@myko/core'
import { pack, unpack } from 'msgpackr'
import type { WSMMessage } from '../../../../ws/src'

export const clientProtocols = new Map<ID, MykoProtocol>()

const decoders = {
  [MykoProtocol.JSON]: (data) => JSON.parse(data),
  [MykoProtocol.MSGPACK]: (data) => {
    try {
      return unpack(data)
    } catch (e) {
      return JSON.parse(data)
    }
  },
}

const encoders = {
  [MykoProtocol.JSON]: (data) => JSON.stringify(data),
  [MykoProtocol.MSGPACK]: (data) => pack(data),
}

export const parse = (clientId: ID, data: any): WSMMessage => {
  if (!clientProtocols.has(clientId)) {
    clientProtocols.set(clientId, MykoProtocol.JSON)
  }

  const protocol = clientProtocols.get(clientId) as MykoProtocol

  return decoders[protocol](data)
}

export const serialize = (clientId: ID, data: WSMMessage) => {
  if (!clientProtocols.has(clientId)) {
    clientProtocols.set(clientId, MykoProtocol.JSON)
  }
  const protocol = clientProtocols.get(clientId) as MykoProtocol

  return encoders[protocol](data)
}
