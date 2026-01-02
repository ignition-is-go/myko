import type { MItem, MItemConstructor } from '../types'

export const module = (_item: MItemConstructor<MItem>) => {}

export const modules = (...items: MItemConstructor<MItem>[]) =>
  items.forEach(module)
