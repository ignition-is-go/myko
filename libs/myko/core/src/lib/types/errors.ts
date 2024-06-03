/**
 * Represents an error that occurs when the command ID cannot be retrieved from metadata.
 */
export class CommandUnwrapError extends Error {
  constructor() {
    super('Could not get command ID from Metadata')
  }
}
