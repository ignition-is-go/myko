import { MYKO_SAGA_METADATA } from '../constants'

/**
 * Decorator for defining a Myko saga.
 * @returns {Function} - The decorator function.
 */
export const MykoSaga = (): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const properties =
      Reflect.getMetadata(MYKO_SAGA_METADATA, target.constructor) || []
    Reflect.defineMetadata(
      MYKO_SAGA_METADATA,
      [...properties, propertyKey],
      target.constructor,
    )
  }
}
