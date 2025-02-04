import Z from 'zod'
import { forecasters, type Forecaster } from './forecasters'

export const forecastResolutionContext = Z.object({
  value: Z.number(),
  deltaMs: Z.number(),
  forecaster: forecasters,
})

export type ForecastResolutionContext<T extends Forecaster> = {
  readonly value: number
  readonly deltaMs: number
  readonly forecaster: T
} & Z.infer<typeof forecastResolutionContext>

export type ForecastResolver<T extends Forecaster> = (
  context: ForecastResolutionContext<T>,
) => ForecastResolutionContext<T>
