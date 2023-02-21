import { ID } from '@myko/core'
import { Injectable } from '@nestjs/common'

@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> {}
