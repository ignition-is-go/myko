import * as jose from 'jose'

import type { MykoAuthService } from './types'

export class Auth0UserService implements MykoAuthService {
  constructor(
    private readonly peerSecret: string,
    private readonly auth0Domain: string,
  ) {
    if (!peerSecret) {
      throw new Error('peerSecret is required')
    }
    if (!auth0Domain) {
      throw new Error('auth0Domain is required')
    }
  }

  async canActivate(token: string): Promise<boolean> {
    if (token === this.peerSecret) {
      return true
    }

    const jwksUri = `https://${this.auth0Domain}/.well-known/jwks.json`

    const JWKS = jose.createRemoteJWKSet(new URL(jwksUri))

    const { payload } = await jose.jwtVerify(token, JWKS, {})

    const res = !!payload

    return res
  }

  async getUserId(token: string): Promise<string | undefined> {
    return jose.decodeJwt(token).sub
  }

  getPeerToken(): string {
    return this.peerSecret
  }
}
