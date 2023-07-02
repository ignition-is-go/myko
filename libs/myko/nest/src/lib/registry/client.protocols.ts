import { MykoProtocol } from '@myko/core'
import * as WebSocket from 'ws'

export const clientProtocols = new Map<WebSocket, MykoProtocol>()
