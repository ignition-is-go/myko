import type { MEvent } from '@myko/core'
import { Subject } from 'rxjs'

export class PeerEventBus extends Subject<MEvent> {}

export const peerBus = new PeerEventBus()
