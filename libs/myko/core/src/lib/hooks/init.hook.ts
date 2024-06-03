const hooks: Map<string, Set<() => void>> = new Map()

const isRegistered: Set<string> = new Set()
const isInit: Set<string> = new Set()

const initWatchers: Set<
  (entity: string, egistered: number, inited: number) => void
> = new Set()

/**
 * Registers a callback to be called when all specified item types are initialized.
 * @param itemTypes - The item types to wait for.
 * @param cb - The callback to call when all item types are initialized.
 */
export const onInit = (itemTypes: string[], cb: () => void): void => {
  const key = itemTypes.sort().join(':')

  if (!hooks.has(key)) {
    hooks.set(key, new Set())
  }

  hooks.get(key).add(cb)
}

/**
 * Checks if all registered item types are initialized.
 * @returns An object containing information about the initialization status.
 */
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

/**
 * Returns the item types that are registered but not initialized.
 * @returns An array of item types that are registered but not initialized.
 */
export const leftToInit = (): string[] => {
  let registered = new Set(isRegistered)
  for (let i of isInit) {
    registered.delete(i)
  }
  return [...registered]
}

/**
 * Watches for initialization status changes.
 * @param cb - The callback to call when the initialization status changes.
 */
export const watchInit: (
  cb: (entity: string, registered: number, inited: number) => void,
) => void = (
  cb: (entity: string, registered: number, inited: number) => void,
) => {
  initWatchers.add(cb)
}

/**
 * Fires the initialization event for the specified item type.
 * @param itemType - The item type to initialize.
 */
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

/**
 * Registers an item type.
 * @param itemType - The item type to register.
 */
export const beforeInit: (itemType: string) => void = (itemType) => {
  isRegistered.add(itemType)
}
