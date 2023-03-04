import { ID } from '@myko/core'

export class MykoAuthService {
  canActivate(token: string): Promise<boolean> {
    throw new Error()
  }

  getUserId(token: string): Promise<ID> {
    throw new Error()
  }
}
