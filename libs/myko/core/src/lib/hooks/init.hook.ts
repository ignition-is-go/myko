const hooks: Map<string, Set<() => void>> = new Map()

const isRegistered: Set<string> = new Set()
const isInit: Set<string> = new Set()

const initWatchers: Set<
  (entity: string, egistered: number, inited: number) => void
> = new Set()

export const onInit = (itemTypes: string[], cb: () => void): void => {
  const key = itemTypes.sort().join(':')

  if (!hooks.has(key)) {
    hooks.set(key, new Set())
  }

  hooks.get(key).add(cb)
}

export const isAllInit = (): {
  done: boolean
  registered: number
  inited: number
} => {
  return {
    done: isInit.size === isRegistered.size,
    registered: isRegistered.size,
    inited: isInit.size,
  }
}

export const leftToInit = (): string[] => {
  let registered = new Set(isRegistered)
  for (let i of isInit) {
    registered.delete(i)
  }
  return [...registered]
}

export const watchInit: (
  cb: (entity: string, registered: number, inited: number) => void,
) => void = (
  cb: (entity: string, registered: number, inited: number) => void,
) => {
  initWatchers.add(cb)
}

export const fireInit = (itemType: string) => {
  if (isInit.has(itemType)) {
    return
  }

  isInit.add(itemType)

  initWatchers.forEach((cb) => cb(itemType, isRegistered.size, isInit.size))

  hooks.forEach((cb, key) => {
    const types = key.split(':')
    if (types.includes(itemType) && types.every((t) => isInit.has(t))) {
      hooks.get(key).forEach((cb) => cb())
    }
  })
}

export const beforeInit: (itemType: string) => void = (itemType) => {
  isRegistered.add(itemType)
}
