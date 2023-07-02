export enum MykoProtocol {
  JSON = 'json',
  MSGPACK = 'msgpack',
}

export enum ProtocolMessages {
  SwitchToJSON = 'myko:switch-to-json',
  SwitchToMSGPACK = 'myko:switch-to-msgpack',
}
