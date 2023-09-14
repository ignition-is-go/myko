import { Observable } from 'rxjs'
import { ID, MEvent, MItem } from '../types'

export abstract class Persister<T extends MItem> {
  public output: Observable<MEvent<T>>

  abstract persist(event: MEvent<T>): void
}

export abstract class ControledPersister<T extends MItem>
  extends Persister<T>
  implements Controlled
{
  abstract listenId(id: string): ID
  abstract release(releaseId: string): void
}

export interface Controlled {
  listenId(id: string): ID
  release(releaseId: string): void
}
