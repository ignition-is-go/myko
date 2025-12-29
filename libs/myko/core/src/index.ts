import 'reflect-metadata'
import { Observable } from 'rxjs'
export * from './lib'

export function subCount<T>(source: Observable<T>, description: string): Observable<T> {
  let counter = 0

  return new Observable<T>((subscriber) => {
    counter++
    console.log(`Subscribed to ${description} - ${counter}`)
    const subscription = source.subscribe(subscriber)

    return () => {
      counter--
      console.log(`Unsubscribed from ${description} - ${counter}`)
      subscription.unsubscribe()
    }
  })
}
