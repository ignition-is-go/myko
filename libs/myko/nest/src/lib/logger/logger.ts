import { ILogObj, Logger } from 'tslog'

export class MykoLogger extends Logger<ILogObj> {
  constructor(name?: string) {
    super({
      name,
    })
  }
}
