import { inheritsRegistry } from './item.registry'

export type PropDocInfo = {
  entityType: string
  propName: string
  propType: string
  docString: string
  deprecated?: boolean
  type: 'prop'
}

export type ItemDocInfo = {
  entityType: string
  docString: string
  deprecated?: boolean
  extends?: string
  preventDocs?: boolean
  type: 'item'
}

export type BasicPropInfo = {
  propName: any
  propType: string
}

export type CommandDocInfo = {
  commandName: string
  commandId: string
  props: BasicPropInfo[]
  type: 'command'
  ctor: new (...args: any[]) => any
}

export type QueryDocInfo = {
  queryName: string
  queryId: string
  queryReturnType: string
  props: BasicPropInfo[]
  type: 'query'
  ctor: new (...args: any[]) => any
}

export type DocTypeEntry =
  | PropDocInfo
  | ItemDocInfo
  | CommandDocInfo
  | QueryDocInfo

export const docRegistry: DocTypeEntry[] = []

/**
 * Adds a prop doc to the registry.
 * @param item - The prop doc to add.
 * @returns void
 */
export const addPropDoc: (item: Omit<PropDocInfo, 'type'>) => void = (item) => {
  docRegistry.push({ ...item, type: 'prop' })
}

/**
 * Adds an item doc to the registry.
 * @param info - The item doc to add.
 * @returns void
 */

export const addItemDoc: (info: Omit<ItemDocInfo, 'type'>) => void = (info) => {
  docRegistry.push({ ...info, type: 'item' })
  if (info.extends) {
    inheritsRegistry.set(info.entityType, info.extends)
  }
}

/**
 * Adds a command doc to the registry.
 * @param info - The command doc to add.
 * @param paramTypes - The types of the command parameters.
 * @returns void
 */

export const addCommandDoc: (
  info: Omit<CommandDocInfo, 'type' | 'props'>,
  paramTypes: ('String' | 'Number' | 'Boolean' | 'Array' | unknown)[],
) => void = (info, paramTypes) => {
  const inherited = ['String', 'String']

  const props = makeProps(inherited, paramTypes, info.ctor)

  docRegistry.push({ ...info, props: props, type: 'command' })
}

/**
 * Adds a query doc to the registry.
 * @param item - The query doc to add.
 * @param paramTypes - The types of the query parameters.
 * @returns void
 */

export const addQueryDoc: (
  item: Omit<QueryDocInfo, 'type' | 'props'>,
  paramTypes: unknown[],
) => void = (item, paramTypes) => {
  const props = makeProps(['String'], paramTypes, item.ctor)

  docRegistry.push({ ...item, props, type: 'query' })
}

/**
 * Creates a list of prop info objects.
 * @param inherited - The inherited prop types.
 * @param paramTypes - The types of the parameters.
 * @param ctor - The constructor function.
 * @returns An array of prop info objects.
 */

function makeProps(
  inherited: string[],
  paramTypes: unknown[],
  ctor: new (...args: any[]) => any,
): BasicPropInfo[] {
  const allParaams = [...inherited, ...paramTypes]

  const fakeArgs = allParaams.map((x) => {
    switch (x) {
      case 'String':
        return ''
      case 'Number':
        return 0
      case 'Boolean':
        return false
      case 'Array':
        return []
      default:
        return 'not here'
    }
  })

  const cmd = new ctor(...fakeArgs)

  const keys = Object.keys(cmd)

  const props = allParaams.map((propType, index) => {
    return {
      propType,
      propName: keys[index],
    } as BasicPropInfo
  })
  return props
}
