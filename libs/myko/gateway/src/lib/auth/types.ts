import type { ID } from '@myko/core'

export interface MykoAuthService {
  canActivate(token: string): Promise<boolean>
  getUserId(token: string): Promise<ID | undefined>
  getPeerToken(): string
}
