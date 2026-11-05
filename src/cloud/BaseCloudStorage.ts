import { CloudStorageError, type CloudStorageErrorCode } from './CloudStorageError'
import type { CloudAccessTokenProvider, CloudFetch, CloudStorageProvider } from './types'

export abstract class BaseCloudStorage {
  abstract readonly provider: CloudStorageProvider

  protected readonly getAccessToken: CloudAccessTokenProvider
  private readonly cloudFetch: CloudFetch

  constructor(getAccessToken: CloudAccessTokenProvider, cloudFetch: CloudFetch = globalThis.fetch) {
    this.getAccessToken = getAccessToken
    // Browser fetch requires the Window receiver even when injected by a caller.
    this.cloudFetch = cloudFetch.bind(globalThis)
  }

  protected async request(url: string, init: RequestInit = {}): Promise<Response> {
    const headers = new Headers(init.headers)
    headers.set('Authorization', `Bearer ${await this.getAccessToken()}`)

    try {
      return await this.cloudFetch(url, { ...init, headers })
    } catch (cause) {
      throw new CloudStorageError(`Could not reach ${this.provider}.`, {
        code: 'network',
        provider: this.provider,
        cause
      })
    }
  }

  protected async assertOk(response: Response, fallbackMessage: string): Promise<void> {
    if (response.ok) return

    const body = await response.text().catch(() => '')
    const code = this.errorCode(response.status, body)
    const retryAfter = response.headers.get('Retry-After')
    throw new CloudStorageError(this.providerErrorMessage(body) || fallbackMessage, {
      code,
      provider: this.provider,
      status: response.status,
      retryAfterSeconds: retryAfter ? Number(retryAfter) || undefined : undefined
    })
  }

  protected errorCode(status: number, body: string): CloudStorageErrorCode {
    if (status === 401) return 'unauthorized'
    if (status === 403) return 'forbidden'
    if (status === 404 || body.includes('not_found')) return 'not-found'
    if (status === 409 || status === 412) return 'conflict'
    if (status === 429) return 'rate-limited'
    return 'provider-error'
  }

  private providerErrorMessage(body: string): string | undefined {
    if (!body) return undefined
    try {
      const value = JSON.parse(body) as {
        error_summary?: string
        error?: { message?: string } | string
      }
      if (value.error_summary) return value.error_summary
      if (typeof value.error === 'string') return value.error
      return value.error?.message
    } catch {
      return undefined
    }
  }
}
