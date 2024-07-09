import type { MEvent } from '@myko/core'
import { Injectable } from '@nestjs/common'
import { Subject } from 'rxjs'

@Injectable()
export class PeerEventBus extends Subject<MEvent> {}
