import type { ID } from '@myko/core'
import type { Span } from '@opentelemetry/api'

export const eventCounts = new Map<string, number>()

export const tagCounts = new Map<ID, number>()

export const tagTimes = new Map<ID, number>()

export const spansByTx = new Map<ID, Span>()
export const spansByCall = new Map<ID, Span>()

export const callStartTimes = new Map<ID, number>()
export const transactionAttributes = new Map<
  ID,
  { [key: string]: string | number }
>()

export const tagResults = new Map<ID, number>()

export const callLineageCounts = new Map<ID, number>()

export const lineageResults = new Map<ID, number>()
