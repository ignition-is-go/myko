import { ILogObj, Logger } from 'tslog'

export const log: Logger<ILogObj> = new Logger({
  hideLogPositionForProduction: true,
  stylePrettyLogs: true,
})
