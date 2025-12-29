import { addItemDoc, addPropDoc } from '../registry'

/**
 * Decorator that adds documentation to a class.
 * @param docString - The documentation string for the class.
 * @param itemType - The type of the class.
 * @param deprecated - A flag indicating if the class is deprecated.
 * @param preventDocs - A flag indicating if the class should not be documented.
 * @returns A class decorator function.
 */
export const docEntity =
  (docString: string | undefined, itemType: string, deprecated?: boolean, preventDocs?: boolean): ClassDecorator =>
  (target: Function) => {
    const parentName = Object.getPrototypeOf(Object.getPrototypeOf(target)).name

    addItemDoc({
      docString: docString || '',
      entityType: itemType || '',
      deprecated,
      extends: parentName,
      preventDocs,
    })
  }

export const doc = (docString?: string, typeOverride?: string, deprecated?: boolean): PropertyDecorator => {
  return (target, propertyKey) => {
    if (
      !target ||
      Object.keys(propertyKey as any).includes('addInitializer' satisfies keyof ClassMethodDecoratorContext)
    ) {
      console.warn('New Decorator Detected: returning early')
      return
    }

    const propType = typeOverride ?? (Reflect.getMetadata('design:type', target, propertyKey)?.name.toLowerCase() || '')

    const autoItemType = Object.getOwnPropertyDescriptors(target.constructor)?.name?.value || ''

    if (!autoItemType) {
      throw new Error(`The class ${target.constructor.name} does not have a valid item type.`)
    }

    addPropDoc({
      docString: docString || '',
      entityType: autoItemType,
      propName: propertyKey.toString(),
      propType: propType || '',
      deprecated,
    })
  }
}
