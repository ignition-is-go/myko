import 'reflect-metadata'

export const MYKO_ITEM_TYPE = '__MYKO_ITEM_TYPE__'

export const MykoItem =
  (itemType: string): ClassDecorator =>
  (target) => {
    const original: any = target
    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_ITEM_TYPE, itemType, typed)
      return typed
    }
    return withType
  }
