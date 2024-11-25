const isRegistered: Set<string> = new Set()
const isInit: Set<string> = new Set()

const initWatchers: Set<
  (
    entity: string,
    egistered: string[],
    inited: string[],
    uninited: string[],
  ) => void
> = new Set()

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
  cb: (
    entity: string,
    registered: string[],
    inited: string[],
    uninited: string[],
  ) => void,
) => void = (
  cb: (
    entity: string,
    registered: string[],
    inited: string[],
    uninited: string[],
  ) => void,
) => {
  initWatchers.add(cb)
}

/**
 * Fires the initialization event for the specified item type.
 * @param itemType - The item type to initialize.
 */
export const fireInit = (itemType: string) => {
  if (isInit.has(itemType)) {
    console.log('Skipping duplicate init for', itemType)
    return
  }

  isInit.add(itemType)

  initWatchers.forEach((cb) =>
    cb(itemType, [...isRegistered], [...isInit], leftToInit()),
  )

  if (isAllInit().done) {
    allInitCallbacks.forEach((cb) => cb())
    allInitCallbacks.clear()
    initWatchers.clear()
  }
}

/**
 * Registers an item type.
 * @param itemType - The item type to register.
 */
export const beforeInit: (itemType: string) => void = (itemType) => {
  isRegistered.add(itemType)
}

const allInitCallbacks: Set<() => void> = new Set()

export const onAllInit = (cb: () => void): void => {
  allInitCallbacks.add(cb)
}
