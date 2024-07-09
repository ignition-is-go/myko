import { MQueryBus } from '@myko/core'
import { Injectable } from '@nestjs/common'

@Injectable()
export class MykoQueryBus extends MQueryBus {
  constructor() {
    super()
  }
}
