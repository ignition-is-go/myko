import { addItemDoc, addPropDoc } from '../registry'

export const docEntity =
  (
    docString?: string,
    itemType?: string,
    deprecated?: boolean,
    preventDocs?: boolean,
  ): ClassDecorator =>
  (target: Function) => {
    const parentName = Object.getPrototypeOf(Object.getPrototypeOf(target)).name

    addItemDoc({
      docString,
      entityType: itemType,
      deprecated,
      extends: parentName,
      preventDocs,
    })
  }

export const doc = (
  docString?: string,
  typeOverride?: string,
  deprecated?: boolean,
): PropertyDecorator => {
  return (target: object, propertyKey: string | symbol) => {
    const propType =
      typeOverride ??
      Reflect.getMetadata(
        'design:type',
        target,
        propertyKey,
      )?.name.toLowerCase()

    const autoItemType = Object.getOwnPropertyDescriptors(target.constructor)
      .name.value

    addPropDoc({
      docString: docString,
      entityType: autoItemType,
      propName: propertyKey.toString(),
      propType: propType,
      deprecated,
    })
  }
}
