import { MYKO_SAGA_METADATA } from '../types'

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
