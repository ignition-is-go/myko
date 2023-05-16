import { MItem } from '../types'
import { MYKO_ITEM_TYPE } from '../types'
import 'reflect-metadata'

import { relationRegistry } from '../registry'

const MYKO_ITEM_BELONGS_TO_KEY = '__MYKO_ITEM_BELONGS_TO_KEY__'
const MYKO_ITEM_OWNS_MANY_KEY = '__MYKO_ITEM_OWNS_MANY_KEY__'

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

    Reflect.getOwnMetadataKeys(original).forEach((key) => {
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
    })

    return withType
  }

export const ownsMany = (
  depType: new (...args: any[]) => MItem,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const itemType = Reflect.getOwnMetadata(MYKO_ITEM_TYPE, depType)

    const propType = Reflect.getMetadata('design:type', target, propertyKey)

    if (propType !== Array) {
      console.warn('Cannot use ownsMany decorator on non-array property')
    }

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
    const itemType = Reflect.getOwnMetadata(MYKO_ITEM_TYPE, depType)

    const propType = Reflect.getMetadata('design:type', target, propertyKey)

    if (propType === Array || propType === Object) {
      console.log(propType)
      throw new Error('Cannot use belongsTo decorator on non-string property')
    }

    Reflect.defineMetadata(
      makeDepkey(propertyKey.toString()),
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
