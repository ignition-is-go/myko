import { defer, tap, type Observable } from 'rxjs'

/* Executes the specified callback function for each emission until the number of emissions is equal to nEmissions*/
export const tapN =
  <T>(nEmissions: number, callback: (arg0: T) => void) =>
  (source$: Observable<T>): Observable<T> =>
    defer(() => {
      let counter = 0
      return source$.pipe(
        tap((item) => {
          if (counter < nEmissions) {
            callback(item)
            counter++
          }
        }),
      )
    })
