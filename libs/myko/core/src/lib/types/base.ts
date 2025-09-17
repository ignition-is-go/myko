import { DateTime } from 'luxon'
import { v4 as uuid } from 'uuid'

/**
 * Base types
 * @module Base
 *
 */

/**
 * A unique identifier.
 * @typedef {string} ID
 */

export type ID = string

/**
 * A generic item.
 * @typedef {object} MItem
 */
export interface Type<T = any> extends Function {
  new (...args: any[]): T
}

/**
 * omit a property from a type
 * @template T - The type to omit a property from.
 * @template K - The property to omit.
 */
export type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>

/**
 * make some properties of a type optional
 * @template T - The type to make properties optional.
 * @template K - The properties to make optional.
 */
export type PartialBy<T, K extends keyof T> = Omit<T, K> & Partial<Pick<T, K>>

/**
 * make some properties of a type required
 * @template T - The type to make properties required.
 * @template K - The properties to make required.
 */
export type DeepPartial<T> = T extends object
  ? {
      [P in keyof T]?: DeepPartial<T[P]>
    }
  : T

export type ContextPhantom = {
  $withContext: true
}

export class WithContext {
  /**
   * The transaction ID associated with the command.
   */
  readonly tx: string

  readonly commandClientId: string

  /**
   * The timestamp when the command was created.
   */
  readonly createdAt: string

  readonly lineage: string[]

  constructor() {
    this.tx = uuid()
    this.createdAt = DateTime.utc().toString()
  }

  withContext(ctx: IContext): this & ContextPhantom {
    Reflect.set(this, 'tx', ctx.tx)
    Reflect.set(this, 'commandClientId', ctx.commandClientId)

    const lineage = Reflect.get(ctx, 'lineage')

    const updatedLineage = [...(lineage ?? []), this.getTag()]

    Reflect.set(this, 'lineage', updatedLineage)

    return this as this & ContextPhantom
  }

  getTag(): string {
    throw new Error('Method not implemented.')
  }
}

export type IContext = Omit<
  WithContext,
  'withContext' | 'createdAt' | 'getTag' | 'lineage'
> &
  Partial<Pick<WithContext, 'lineage'>>
