import type { CloudStorageProvider } from './types'

export type CloudStorageErrorCode = 'unauthorized'
  | 'forbidden'
  | 'not-found'
  | 'conflict'
  | 'rate-limited'
  | 'invalid-path'
  | 'network'
  | 'provider-error'

export class CloudStorageError extends Error {
  readonly code: CloudStorageErrorCode
  readonly provider?: CloudStorageProvider
  readonly status?: number
  readonly retryAfterSeconds?: number
  override readonly cause?: unknown

  constructor(
    message: string,
    options: {
      code: CloudStorageErrorCode
      provider?: CloudStorageProvider
      status?: number
      retryAfterSeconds?: number
      cause?: unknown
    }
  ) {
    super(message)
    this.name = 'CloudStorageError'
    this.code = options.code
    this.provider = options.provider
    this.status = options.status
    this.retryAfterSeconds = options.retryAfterSeconds
    this.cause = options.cause
  }
}
