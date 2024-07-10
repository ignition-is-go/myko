import type { MykoWsAdapterOptions } from '../adapters/types'

let tx: MykoWsAdapterOptions['tx'] | null
let rx: MykoWsAdapterOptions['rx'] | null

export const setAdapterBusses = (
  args: Pick<MykoWsAdapterOptions, 'rx' | 'tx'>,
) => {
  if (tx) {
    throw new Error('tx bus already set')
  }
  if (rx) {
    throw new Error('rx bus already set')
  }

  tx = args.tx
  rx = args.rx
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
