import { MYKO_ITEM_TYPE } from '../constants'
import { addBallanceKey } from '../registry'
import type { MItem, MItemConstructor } from '../types'

/**
 * Decorator that registers a propery as  to a class property.
 * @returns A property decorator function.
 */
export const ballance = <T extends MItem>(otherType: MItemConstructor<T>): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const localType = Object.getOwnPropertyDescriptors(target.constructor).name.value

    if (!localType) {
      throw new Error('Cannot Ballance for undefined item type')
    }

    const foreignType = Reflect.getMetadata(MYKO_ITEM_TYPE, otherType)

    addBallanceKey(localType, {
      propName: propertyKey.toString(),
      foreignType,
    })
  }
}
