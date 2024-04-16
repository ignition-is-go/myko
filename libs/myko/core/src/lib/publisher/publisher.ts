export abstract class APublisher<MType> {
  constructor() {}

  abstract publish

  abstract publishCommand()
}
