export const makeSafeTopic = (topic: string) => {
  const safe = topic.replace(/[^a-zA-Z0-9\.\-_]/g, '_')
  return safe
}
