import type { MykoWsAdapterOptions } from '../adapters/types'

let tx: MykoWsAdapterOptions['tx'] | null
let rx: MykoWsAdapterOptions['rx'] | null
let clients: MykoWsAdapterOptions['clients'] | null

export const setAdapterBusses = (
  args: Pick<MykoWsAdapterOptions, 'rx' | 'tx' | 'clients'>,
) => {
  if (tx) {
    throw new Error('tx bus already set')
  }
  if (rx) {
    throw new Error('rx bus already set')
  }

  if (clients) {
    throw new Error('clients bus already set')
  }

  tx = args.tx
  rx = args.rx
  clients = args.clients
}

export const getTx = () => {
  if (!tx) {
    throw new Error('tx bus not set')
  }

  return tx
}

export const getRx = () => {
  if (!rx) {
    throw new Error('rx bus not set')
  }

  return rx
}

export const getClients = () => {
  if (!clients) {
    throw new Error('clients bus not set')
  }

  return clients
}
