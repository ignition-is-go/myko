import type { ID } from './base'

export class MykoCommandError {
  constructor(
    readonly tx: ID,
    readonly message: string,
  ) {}
}
