/**
 * This file contains the implementation of the `MykoItem` decorator and related decorators.
 * The `MykoItem` decorator is a class decorator that adds metadata to a class and its properties.
 * It also registers relationships between classes and sets default values for properties.
 * The `ownsMany`, `belongsTo`, `defaultValue`, and `ensureFor` decorators are property decorators
 * that add specific metadata to the decorated properties.
 */
import {
  MYKO_CLIENT_ID_KEY,
  MYKO_ITEM_BELONGS_TO_KEY,
  MYKO_ITEM_DEFAULT_VALUE_KEY,
  MYKO_ITEM_ENSURE_KEY,
  MYKO_ITEM_OWNS_MANY_KEY,
  MYKO_ITEM_SEARCH_KEY,
  MYKO_ITEM_TYPE,
} from '../constants'
import {
  clientIdPropertyRegistry,
  initRepo,
  propertyDefaults,
  relationRegistry,
} from '../registry'
import type { MItem } from '../types'
import { doc, docEntity } from './doc.decorators'

/**
 * Decorator that elevates a class to a Myko Item.
 *
 * @param opts - An optional object containing decorator options.
 * @param opts.doc - The documentation string for the class.
 * @param opts.itemTypeOverride - The override value for the item type.
 * @param opts.deprecated - A flag indicating if the class is deprecated.
 * @param opts.preventDoc - A flag indicating if the documentation should be prevented.
 *
 * @returns A class decorator function.
 */
export const MykoItem =
  (opts?: {
    doc?: string
    itemTypeOverride?: string
    deprecated?: boolean
    preventDoc?: boolean
  }): ClassDecorator =>
  (target) => {
    const original: any = target
    const autoItemType =
      Object.getOwnPropertyDescriptors(original)['name'].value

    const itemType = opts?.itemTypeOverride ?? autoItemType

    if (!itemType) {
      throw new Error('itemType is undefined')
    }

    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_ITEM_TYPE, itemType, typed)
      return typed
    }

    Reflect.setPrototypeOf(withType, original)

    Reflect.defineMetadata(MYKO_ITEM_TYPE, itemType, withType)

    docEntity(opts?.doc, itemType, opts?.deprecated, opts?.preventDoc)(target)

    const metaKeys = Reflect.getMetadataKeys(original)

    metaKeys.forEach((key) => {
      if (key.startsWith(MYKO_ITEM_SEARCH_KEY)) {
        const propertyKey = decodeSearchKey(key)
        relationRegistry.add({
          type: 'searchable',
          localType: itemType,
          localKey: propertyKey,
        })
      }

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

      if (key.startsWith(MYKO_CLIENT_ID_KEY)) {
        const propertyKey = decodeClientIdKey(key)

        clientIdPropertyRegistry.set(itemType, propertyKey)
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

    initRepo(withType)

    return withType
  }

/**
 * Decorator that specifies that the target object owns many instances of the specified dependency type.
 *
 * @param depType The constructor function of the dependency type.
 * @returns The property decorator function.
 */
export const ownsMany = (
  depType: new (...args: any[]) => MItem,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const itemType = Reflect.getMetadata(MYKO_ITEM_TYPE, depType)
    doc(`Owns many ${itemType}`)(target, propertyKey)

    Reflect.defineMetadata(
      makeOwnsKey(propertyKey.toString()),
      itemType,
      target.constructor,
    )
  }
}

/**
 * Decorator that specifies that the target object belongs to the specified dependency type.
 * @param depType The constructor function of the dependency type.
 * @returns The property decorator function.
 *
 */
export const belongsTo = (
  depType: new (...args: any[]) => MItem,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const foreignItemType = Reflect.getMetadata(MYKO_ITEM_TYPE, depType)

    doc(`Belongs to ${foreignItemType}`)(target, propertyKey)

    Reflect.defineMetadata(
      makeDepkey(propertyKey.toString()),
      foreignItemType,
      target.constructor,
    )
  }
}

/**
 * Decorator that specifies a default value for the target property.
 * @param value The default value for the property.
 * @returns The property decorator function.
 */
export const defaultValue = (value: any): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    doc(`Defaults to ${value}`)(target, propertyKey)

    Reflect.defineMetadata(
      makeDefaultValueKey(propertyKey.toString()),
      value,
      target.constructor,
    )
  }
}

/**
 * Decorator that specifies that the target object ensures the existence of the specified dependency type.
 * @param depType The constructor function of the dependency type.
 * @returns The property decorator function.
 */
export const ensureFor = (
  depType: new (...args: any[]) => MItem,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const foreignItemType = Reflect.getMetadata(MYKO_ITEM_TYPE, depType)
    doc(`Ensured for ${foreignItemType}`)(target, propertyKey)

    Reflect.defineMetadata(
      makeEnsureKey(propertyKey.toString()),
      foreignItemType,
      target.constructor,
    )
  }
}

export const searchable = (): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    doc(`Searchable`)(target, propertyKey)

    Reflect.defineMetadata(
      makeSearchKey(propertyKey.toString()),
      true,
      target.constructor,
    )
  }
}

export const mykoClientId = (): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    doc(`Auto Client ID`)(target, propertyKey)

    Reflect.defineMetadata(
      makeClientIdKey(propertyKey.toString()),
      true,
      target.constructor,
    )
  }
}

const makeOwnsKey = (propertyKey: string): string =>
  `${MYKO_ITEM_OWNS_MANY_KEY}:${propertyKey}`

const decodeOwnsKey = (ownsKey: string): string => {
  const [_, propertyKey] = ownsKey.split(':')
  return propertyKey
}

const makeDepkey = (propertyKey: string): string =>
  `${MYKO_ITEM_BELONGS_TO_KEY}:${propertyKey}`

const decodeDepkey = (depKey: string): string => {
  const [_, propertyKey] = depKey.split(':')
  return propertyKey
}

const makeEnsureKey = (propertyKey: string): string =>
  `${MYKO_ITEM_ENSURE_KEY}:${propertyKey}`

const decodeEnsureKey = (EnsureKey: string): string => {
  const [_, propertyKey] = EnsureKey.split(':')
  return propertyKey
}

const makeDefaultValueKey = (propertyKey: string): string =>
  `${MYKO_ITEM_DEFAULT_VALUE_KEY}:${propertyKey}`

const decodeDefaultValueKey = (DefaultValueKey: string): string => {
  const [_, propertyKey] = DefaultValueKey.split(':')
  return propertyKey
}

const makeSearchKey = (propertyKey: string): string =>
  `${MYKO_ITEM_SEARCH_KEY}:${propertyKey}`

const decodeSearchKey = (SearchKey: string): string => {
  const [_, propertyKey] = SearchKey.split(':')
  return propertyKey
}

const makeClientIdKey = (propertyKey: string): string =>
  `${MYKO_CLIENT_ID_KEY}:${propertyKey}`

const decodeClientIdKey = (clientIdKey: string): string => {
  const [_, propertyKey] = clientIdKey.split(':')
  return propertyKey
}
