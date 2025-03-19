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

  constructor() {
    this.tx = uuid()
    this.createdAt = DateTime.utc().toString()
  }

  /**
   * Sets the transaction ID for the command.
   * @param tx The transaction ID.
   * @returns The modified command instance.
   */
  withTransaction(tx: ID): this & ContextPhantom {
    Reflect.set(this, 'tx', tx)
    return this as this & ContextPhantom
  }

  /**
   * Sets the transaction ID for the command.
   * @param clientId the client ID.
   * @returns The modified command instance.
   */
  withClientId(clientId: ID): this & ContextPhantom {
    Reflect.set(this, 'commandClientId', clientId)
    return this as this & ContextPhantom
  }

  withContext(ctx: IWithContext): this & ContextPhantom {
    return this.withClientId(ctx.commandClientId).withTransaction(
      ctx.tx,
    ) as this & ContextPhantom
  }
}

export type IWithContext = Omit<
  WithContext,
  'withClientId' | 'withTransaction' | 'withContext' | 'createdAt'
>
