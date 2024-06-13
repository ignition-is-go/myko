import { ILogObj, Logger } from 'tslog'

import { loggerDefaultOptions } from '@myko/core'

export class MykoLogger extends Logger<ILogObj> {
  constructor(name?: string) {
    super({
      ...loggerDefaultOptions,
      name,
    })
  }
}
