import { MykoProtocol } from '@myko/core'
import { pack, unpack } from 'msgpackr'
import * as WebSocket from 'ws'

export const clientProtocols = new Map<WebSocket, MykoProtocol>()

export const decoders = new Map<MykoProtocol, (data: any) => any>()
export const encoders = new Map<MykoProtocol, (data: any) => any>()

decoders.set(MykoProtocol.JSON, (data) => JSON.parse(data))
decoders.set(MykoProtocol.MSGPACK, (data) => {
  try {
    return unpack(data)
  } catch (e) {
    return JSON.parse(data)
  }
})

encoders.set(MykoProtocol.JSON, (data) => JSON.stringify(data))
encoders.set(MykoProtocol.MSGPACK, (data) => pack(data))
