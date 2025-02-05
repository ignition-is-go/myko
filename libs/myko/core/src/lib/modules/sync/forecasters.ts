import * as Z from 'zod'
import type { ForecastResolutionContext, ForecastResolver } from './types'

export const linearForecaster = Z.object({
  type: Z.literal('linear'),
  slope: Z.number().describe('literal amount change per ms'),
  sampleRate: Z.number(),
})

export type LinearForecaster = Z.infer<typeof linearForecaster>

export const resolveLinearForecast: ForecastResolver<LinearForecaster> = ({
  deltaMs,
  value,
  forecaster,
}) => {
  const v = forecaster.slope * deltaMs + value

  return {
    deltaMs: 0,
    forecaster: {
      slope: forecaster.slope,
      type: 'linear',
      sampleRate: forecaster.sampleRate,
    },
    value: v,
  }
}

export const parabolicForecaster = Z.object({
  type: Z.literal('parabolic'),
  a: Z.number(),
  b: Z.number(),
  c: Z.number(),
  sampleRate: Z.number(),
})

export type ParabolicForecaster = Z.infer<typeof parabolicForecaster>

export const resolveParabolicForecast: ForecastResolver<
  ParabolicForecaster
> = ({ deltaMs, value, forecaster }) => {
  const v =
    forecaster.a * deltaMs ** 2 + forecaster.b * deltaMs + forecaster.c + value

  return {
    deltaMs: 0,
    forecaster: {
      a: forecaster.a,
      b: forecaster.b,
      c: forecaster.c,
      type: 'parabolic',
      sampleRate: forecaster.sampleRate,
    },
    value: v,
  }
}

export const nullForecaster = Z.object({
  type: Z.literal('null').describe(
    'no forecasting - updates only happen on sync',
  ),
})

export type NullForecaster = Z.infer<typeof nullForecaster>

export const resolveNullForecast: ForecastResolver<NullForecaster> = ({
  value,
}) => ({ value, deltaMs: 0, forecaster: { type: 'null' } })

export const forecasters = Z.union([
  nullForecaster,
  linearForecaster, //
  parabolicForecaster,
])

export type Forecaster = Z.infer<typeof forecasters>

export const resolveForecast = ({
  deltaMs,
  value,
  forecaster,
}: ForecastResolutionContext<Forecaster>) => {
  const validForecaster = forecasters.safeParse(forecaster)

  if (!validForecaster.success) {
    throw new Error('Invalid forecaster')
  }

  switch (forecaster.type) {
    case 'null':
      return resolveNullForecast({ value, forecaster, deltaMs })
    case 'linear':
      return resolveLinearForecast({ value, forecaster, deltaMs })
    case 'parabolic':
      return resolveParabolicForecast({ value, forecaster, deltaMs })
  }
}
