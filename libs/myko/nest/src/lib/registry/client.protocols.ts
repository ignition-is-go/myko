import { MykoProtocol } from '@myko/core'
import * as WebSocket from 'ws'
import { Decoder, Encoder } from '@msgpack/msgpack'

export const clientProtocols = new Map<WebSocket, MykoProtocol>()

export const decoders = new Map<MykoProtocol, (data: any) => any>()
export const encoders = new Map<MykoProtocol, (data: any) => any>()

const decoder = new Decoder()
const encoder = new Encoder()
decoders.set(MykoProtocol.JSON, (data) => JSON.parse(data))
decoders.set(MykoProtocol.MSGPACK, (data) => decoder.decode(data))

encoders.set(MykoProtocol.JSON, (data) => JSON.stringify(data))
encoders.set(MykoProtocol.MSGPACK, (data) => encoder.encode(data))
