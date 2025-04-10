import type { ID } from '@myko/core'

export const spans = new Map<ID, any>()

export const eventCounts = new Map<string, number>()

export const txCounts = new Map<ID, number>()

export const txTimes = new Map<ID, number>()

export const transactionStartTimes = new Map<ID, number>()
export const transactionAttributes = new Map<
  ID,
  { [key: string]: string | number }
>()

export const txResults = new Map<ID, number>()
