export class CommandUnwrapError extends Error {
  constructor() {
    super('Could not get command ID from Metadata')
  }
}
