import { commandBus, eventBus } from '../busses'
import { onAllInit } from '../hooks'
import type { MSagaHandler } from '../types'

/**
 * Decorator for defining a Myko saga.
 * @returns {Function} - The decorator function.
 */
// export const MykoSaga = (): PropertyDecorator => {
//   return (target: object, propertyKey: string | symbol) => {
//     const properties =
//       Reflect.getMetadata(MYKO_SAGA_METADATA, target.constructor) || []
//     Reflect.defineMetadata(
//       MYKO_SAGA_METADATA,
//       [...properties, propertyKey],
//       target.constructor,
//     )
//   }
// }

export const MykoSaga = (): Function => {
  return (target: new () => MSagaHandler) => {
    const exec = new target()

    onAllInit(() => {
      exec.execute(eventBus.subject$).subscribe((c) => commandBus.execute(c))
    })
  }
}
