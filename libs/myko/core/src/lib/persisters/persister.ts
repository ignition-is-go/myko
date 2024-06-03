import { Observable } from 'rxjs'
import { MEvent, MItem } from '../types'

/**
 * Abstract class representing a persister in the Myko core library.
 * @template T - The type of item to persist.
 */
export abstract class Persister<T extends MItem> {
  public output: Observable<MEvent<T>>

  abstract persist(event: MEvent<T>): void
}

// /**
//  * Abstract class representing a controlled persister in the Myko core library.
//  * @template T - The type of item to persist.
//  */
// export abstract class ControledPersister<T extends MItem>
//   extends Persister<T>
//   implements Controlled
// {
//   abstract listenId(id: string): ID
//   abstract release(releaseId: string): void
// }

// /**
//  * Interface representing a controlled persister.
//  *
//  */
// export interface Controlled {
//   listenId(id: string): ID
//   release(releaseId: string): void
// }
