import { DateTime } from 'luxon'
import { filter, type MonoTypeOperatorFunction } from 'rxjs'
import { MYKO_ITEM_TYPE } from '../constants'
import { MykoItem, MykoQuery, doc } from '../decorators'
import type { ID } from './base'
import { MItem, type IMItem, type MItemConstructor } from './item'
import { MQuery } from './query'

/**
 * Enum representing the types of events.
 */
export enum MEventType {
  SET = 'SET',
  DEL = 'DEL',
}

/**
 * Interface representing an event.
 */
export type IMEvent = {
  readonly tx: ID
  readonly itemType: string
  readonly createdAt: string
  readonly sourceId?: ID
}

export type MEventOptions = {
  preventRelationshipUpdates?: boolean
}

/**
 * Type representing an event with a specific item and change type.
 */
export type MEvent<
  T extends MItem = MItem,
  C extends MEventType = MEventType,
> = IMEvent & {
  readonly item: T
  readonly changeType: C
  readonly options?: MEventOptions
}

/**
 * Creates a "SET" event for the given item and transaction ID.
 * @param item The item associated with the event.
 * @param tx The transaction ID.
 * @returns The created "SET" event.
 */
export const makeSet: <T extends MItem<IMItem>>(
  item: T,
  tx: ID,
  options?: MEventOptions,
) => MEvent<T, MEventType.SET> = <T extends MItem>(
  item: T,
  tx: ID,
  options?: MEventOptions,
) => makeMykoEvent(item, MEventType.SET, tx, undefined, undefined, options)

/**
 * Creates a "DEL" event for the given item and transaction ID.
 * @param item The item associated with the event.
 * @param tx The transaction ID.
 * @returns The created "DEL" event.
 */
export const makeDel: <T extends MItem<IMItem>>(
  item: T,
  tx: ID,
  options?: MEventOptions,
) => MEvent<T, MEventType.DEL> = <T extends MItem>(
  item: T,
  tx: ID,
  options?: MEventOptions,
) => makeMykoEvent(item, MEventType.DEL, tx, undefined, undefined, options)

/**
 * Creates a custom event for the given item, change type, and transaction ID.
 * @param item The item associated with the event.
 * @param changeType The change type of the event.
 * @param tx The transaction ID.
 * @param overrideType The override type for the event (optional).
 * @param sourceId The source ID for the event (optional).
 * @returns The created custom event.
 * @throws Error if the item is not decorated with a type.
 */
const makeMykoEvent = <T extends MItem, U extends MEventType>(
  item: T,
  changeType: U,
  tx: ID,
  overrideType?: string,
  sourceId?: ID,
  options?: MEventOptions,
): MEvent<T, U> => {
  const metadataType = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  const itemType = overrideType ?? metadataType

  if (!itemType) {
    throw new Error('Item not decorated with type')
  }
  return {
    changeType,
    item,
    itemType,
    createdAt: DateTime.utc().toString(),
    tx,
    sourceId,
    options,
  }
}

/**
 * Operator function that filters events based on the item types.
 * @param filterTypes The item types to filter.
 * @returns The filtered events.
 */
export const ofItems: <T extends MItem>(
  ...filterTypes: MItemConstructor<T>[]
) => MonoTypeOperatorFunction<MEvent<T, MEventType>> = <T extends MItem>(
  ...filterTypes: MItemConstructor<T>[]
) =>
  filter((event: MEvent<T, MEventType>) =>
    filterTypes.some(
      (filterType) =>
        Reflect.getMetadata(MYKO_ITEM_TYPE, filterType) === event.itemType,
    ),
  )

/**
 * Operator function that filters events based on the change type.
 * @param filterType The change type to filter.
 * @returns The filtered events.
 */
export const ofType: <T extends MItem, C extends MEventType>(
  filterType: C,
) => MonoTypeOperatorFunction<MEvent<T, C>> = <
  T extends MItem,
  C extends MEventType,
>(
  filterType: C,
) => filter((event: MEvent<T, C>) => filterType === event.changeType)

/**
 * Represents a container for events to allow them to be queried.
 */
@MykoItem({
  doc: 'A container for events to allow them to be queried',
  preventDoc: true,
})
export class EventContainer extends MItem<EventContainer> {
  /**
   * The event associated with the container.
   * Definite assignment assertion as value is set via Object.assign in parent constructor
   */
  @doc(undefined, 'MEvent')
  readonly event!: MEvent
}

/**
 * Represents a query to get event logs.
 */
@MykoQuery(EventContainer)
export class GetEventLog extends MQuery<EventContainer> {
  /**
   * The time parameter for the query.
   */
  constructor(readonly time: string) {
    super()
  }
}
