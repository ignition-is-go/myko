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

export const addPropDoc = (item: Omit<PropDocInfo, 'type'>) => {
  docRegistry.push({ ...item, type: 'prop' })
}

export const addItemDoc = (info: Omit<ItemDocInfo, 'type'>) => {
  docRegistry.push({ ...info, type: 'item' })
  if (info.extends) {
    inheritsRegistry.set(info.entityType, info.extends)
  }
}

export const addCommandDoc = (
  info: Omit<CommandDocInfo, 'type' | 'props'>,
  paramTypes: ('String' | 'Number' | 'Boolean' | 'Array' | unknown)[],
) => {
  const inherited = ['String', 'String']

  const props = makeProps(inherited, paramTypes, info.ctor)

  docRegistry.push({ ...info, props: props, type: 'command' })
}

export const addQueryDoc = (
  item: Omit<QueryDocInfo, 'type' | 'props'>,
  paramTypes: unknown[],
) => {
  const props = makeProps(['String'], paramTypes, item.ctor)

  docRegistry.push({ ...item, props, type: 'query' })
}

function makeProps(
  inherited: string[],
  paramTypes: unknown[],
  ctor: new (...args: any[]) => any,
) {
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
