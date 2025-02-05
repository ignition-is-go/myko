export const autoballanceRegistry = new Map<string, BallanceInfo>()

export const addBallanceKey = (localType: string, info: BallanceInfo) => {
  if (autoballanceRegistry.has(localType)) {
    throw new Error(`Ballance key already exists: ${localType}`)
  }

  autoballanceRegistry.set(localType, info)
}

type BallanceInfo = {
  propName: string
  foreignType: string
}
