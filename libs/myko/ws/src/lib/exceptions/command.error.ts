import type { ID } from '@myko/core'
import { type WSMCommandError } from '../types'

export class MykoCommandError extends Error implements WSMCommandError {
  constructor(
    readonly tx: ID,
    error: string,
  ) {
    super(error)
    this.event = 'ws:m:command-error'
  }
  event: 'ws:m:command-error'
}
