import { ILogObj, ISettingsParam } from 'tslog'

export const loggerDefaultOptions: ISettingsParam<ILogObj> = {
  hideLogPositionForProduction: true,
  stylePrettyLogs: true,
}
