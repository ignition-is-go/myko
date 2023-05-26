const hooks = new Map<string, Set<() => void>>()

const isInit = new Set<string>()

export const onInit = (itemTypes: string[], cb: () => void) => {
  const key = itemTypes.sort().join(':')

  if (!hooks.has(key)) {
    hooks.set(key, new Set())
  }

  hooks.get(key).add(cb)
}

export const fireInit = (itemType: string) => {
  isInit.add(itemType)

  isInit.add(itemType)

  hooks.forEach((cb, key) => {
    const types = key.split(':')
    if (types.includes(itemType) && types.every((t) => isInit.has(t))) {
      hooks.get(key).forEach((cb) => cb())
    }
  })
}
