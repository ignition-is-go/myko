import type { HistoryProvider } from '../history'

let historyProvider: HistoryProvider | null = null

export const getHistoryProvider = () => {
  if (!historyProvider) {
    throw new Error('History provider not set')
  }

  return historyProvider
}

export const setHistoryProvider = (provider: HistoryProvider) => {
  historyProvider = provider
}
