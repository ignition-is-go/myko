import { type ID, MykoProtocol } from '@myko/core'
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

/**
 * Detect if data is binary (msgpack) or text (JSON).
 * Binary data starts with msgpack format bytes, JSON starts with '{' or '['
 */
const detectProtocol = (data: any): MykoProtocol => {
  // If it's a string, it's JSON
  if (typeof data === 'string') {
    return MykoProtocol.JSON
  }

  // If it's a Buffer/ArrayBuffer/Uint8Array, check the first byte
  if (data instanceof Buffer || data instanceof Uint8Array || data instanceof ArrayBuffer) {
    const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : data
    if (bytes.length > 0) {
      const firstByte = bytes[0]
      // JSON starts with '{' (0x7B) or '[' (0x5B)
      if (firstByte === 0x7B || firstByte === 0x5B) {
        return MykoProtocol.JSON
      }
      // Otherwise assume msgpack
      return MykoProtocol.MSGPACK
    }
  }

  // Default to JSON
  return MykoProtocol.JSON
}

export const parse = (clientId: ID, data: any): WSMMessage => {
  // Auto-detect protocol from incoming data
  const detectedProtocol = detectProtocol(data)

  // Update client's protocol if this is binary data (msgpack)
  if (detectedProtocol === MykoProtocol.MSGPACK) {
    clientProtocols.set(clientId, MykoProtocol.MSGPACK)
  } else if (!clientProtocols.has(clientId)) {
    clientProtocols.set(clientId, MykoProtocol.JSON)
  }

  const result = decoders[detectedProtocol](data)
  console.log('[PARSE]', detectedProtocol, 'event:', result?.event, 'itemType:', result?.data?.itemType)
  return result
}

export const serialize = (clientId: ID, data: WSMMessage) => {
  if (!clientProtocols.has(clientId)) {
    clientProtocols.set(clientId, MykoProtocol.JSON)
  }
  const protocol = clientProtocols.get(clientId) as MykoProtocol

  return encoders[protocol](data)
}
