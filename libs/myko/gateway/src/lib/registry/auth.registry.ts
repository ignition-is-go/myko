import type { MykoAuthService } from '../auth/types'

let auth: MykoAuthService | null = null

export const getAuth = (): MykoAuthService | null => {
  return auth
}

export const setAuth = (authService: MykoAuthService): void => {
  if (auth) {
    throw new Error('Auth service already initialized')
  }

  auth = authService
}
