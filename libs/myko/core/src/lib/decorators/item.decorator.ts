import 'reflect-metadata'
import { MItem } from '../types'
import {
  MYKO_ITEM_BELONGS_TO_KEY,
  MYKO_ITEM_DEFAULT_VALUE_KEY,
  MYKO_ITEM_ENSURE_KEY,
  MYKO_ITEM_OWNS_MANY_KEY,
  MYKO_ITEM_TYPE,
} from '../constants'
import { propertyDefaults, relationRegistry } from '../registry'

export const MykoItem =
  (itemType: string): ClassDecorator =>
  (target) => {
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_ITEM_TYPE, itemType, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_ITEM_TYPE, itemType, withType)

    const metaKeys = Reflect.getMetadataKeys(original)

    metaKeys.forEach((key) => {
      if (key.startsWith(MYKO_ITEM_BELONGS_TO_KEY)) {
        // belongs to
        const propertyKey = decodeDepkey(key)
        const depType = Reflect.getMetadata(key, original)

        relationRegistry.add({
          type: 'belongs-to',
          foreignKey: 'id',
          foreignType: depType,
          localType: itemType,
          localKey: propertyKey,
        })
      }

      if (key.startsWith(MYKO_ITEM_OWNS_MANY_KEY)) {
        // owns many
        const propertyKey = decodeOwnsKey(key)
        const depType = Reflect.getMetadata(key, original)

        relationRegistry.add({
          type: 'owns-many',
          foreignKey: 'id',
          foreignType: depType,
          localKey: propertyKey,
          localType: itemType,
        })
      }

      if (key.startsWith(MYKO_ITEM_DEFAULT_VALUE_KEY)) {
        const propertyKey = decodeDefaultValueKey(key)

        if (!propertyDefaults.has(itemType)) {
          propertyDefaults.set(itemType, new Map())
        }

        propertyDefaults
          .get(itemType)
          .set(propertyKey, Reflect.getMetadata(key, original))
      }
    })

    const ensureKeys = metaKeys.filter((key) =>
      key.startsWith(MYKO_ITEM_ENSURE_KEY),
    )

    if (ensureKeys.length > 0) {
      relationRegistry.add({
        type: 'ensure-for',
        dependencies: ensureKeys.map((key) => {
          const propertyKey = decodeEnsureKey(key)
          const depType = Reflect.getMetadata(key, original)
          return {
            foreignType: depType,
            foreignKey: 'id',
            localKey: propertyKey,
          }
        }),
        makeDefault: withType,
        localType: itemType,
      })
    }

    return withType
  }

export const ownsMany = (
  depType: new (...args: any[]) => MItem,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, depType)

    Reflect.defineMetadata(
      makeOwnsKey(propertyKey.toString()),
      itemType,
      target.constructor,
    )
  }
}

export const belongsTo = (
  depType: new (...args: any[]) => MItem,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, depType)

    Reflect.defineMetadata(
      makeDepkey(propertyKey.toString()),
      itemType,
      target.constructor,
    )
  }
}

export const defaultValue = (value: any): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    Reflect.defineMetadata(
      makeDefaultValueKey(propertyKey.toString()),
      value,
      target.constructor,
    )
  }
}

export const ensureFor = (
  depType: new (...args: any[]) => MItem,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, depType)

    Reflect.defineMetadata(
      makeEnsureKey(propertyKey.toString()),
      itemType,
      target.constructor,
    )
  }
}

const makeOwnsKey = (propertyKey: string) =>
  `${MYKO_ITEM_OWNS_MANY_KEY}:${propertyKey}`

const decodeOwnsKey = (ownsKey: string): string => {
  const [_, propertyKey] = ownsKey.split(':')
  return propertyKey
}

const makeDepkey = (propertyKey: string) =>
  `${MYKO_ITEM_BELONGS_TO_KEY}:${propertyKey}`

const decodeDepkey = (depKey: string): string => {
  const [_, propertyKey] = depKey.split(':')
  return propertyKey
}

const makeEnsureKey = (propertyKey: string) =>
  `${MYKO_ITEM_ENSURE_KEY}:${propertyKey}`

const decodeEnsureKey = (EnsureKey: string): string => {
  const [_, propertyKey] = EnsureKey.split(':')
  return propertyKey
}

const makeDefaultValueKey = (propertyKey: string) =>
  `${MYKO_ITEM_DEFAULT_VALUE_KEY}:${propertyKey}`

const decodeDefaultValueKey = (DefaultValueKey: string): string => {
  const [_, propertyKey] = DefaultValueKey.split(':')
  return propertyKey
}
