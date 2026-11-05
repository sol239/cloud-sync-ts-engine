import { CloudStorageError, type CloudAccessTokenProvider, type CloudFetch } from '../cloud'
import type { SyncOperation } from '../types'
import type { ISyncBackend, RemoteSyncEntry, SyncCheckpoint } from './ISyncBackend'

/** RVN's authenticated, account-isolated SQLite operation store. */
export class RvnSyncBackend implements ISyncBackend {
  readonly provider = 'rvn'
  readonly requiresHistoryBeforeCheckpoint = true
  private readonly url: string

  constructor(url: string, private readonly space: string,
    private readonly getAccessToken: CloudAccessTokenProvider, private readonly fetcher: CloudFetch = globalThis.fetch) {
    const endpoint = new URL(url)
    if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash)
      throw new Error('A valid RVN HTTP endpoint is required.')
    if (!space || space.length > 256) throw new Error('A sync space is required.')
    const namespace = btoa(Array.from(new TextEncoder().encode(space), byte => String.fromCharCode(byte)).join('')).replaceAll('+', '-').replaceAll('/', '_').replaceAll('=', '')
    this.url = `${endpoint.href.replace(/\/$/, '')}/api/sync/spaces/${namespace}`
  }

  async list(): Promise<RemoteSyncEntry[]> {
    const entries: RemoteSyncEntry[] = []
    let after = 0
    while (true) {
      const page = await this.request(`?after=${after}`) as { entries: RemoteSyncEntry[], after: number }
      entries.push(...page.entries)
      if (!page.entries.length) return entries
      if (page.after <= after) throw new Error('Invalid RVN sync pagination.')
      after = page.after
    }
  }

  read(entry: RemoteSyncEntry): Promise<unknown> { return this.request(`/${encodeURIComponent(entry.id)}`) }
  async append(operation: SyncOperation): Promise<void> { await this.request('', 'POST', { id: operation.id, kind: 'operation', body: operation }) }
  async publishCheckpoint(checkpoint: SyncCheckpoint): Promise<RemoteSyncEntry> {
    const id = crypto.randomUUID()
    await this.request('', 'POST', { id, kind: 'checkpoint', body: checkpoint })
    return { id, kind: 'checkpoint', revision: id }
  }
  async deleteAll(): Promise<void> { await this.request('', 'DELETE') }

  private async request(path: string, method = 'GET', body?: unknown): Promise<unknown> {
    const token = await this.getAccessToken()
    let response: Response
    try {
      response = await this.fetcher(this.url + path, {
        method, credentials: 'include',
        headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })
    } catch (cause) { throw new CloudStorageError('Could not reach the RVN server.', { code: 'network', cause }) }
    if (!response.ok) throw new CloudStorageError(`RVN sync failed (${response.status}).`, {
      code: response.status === 401 ? 'unauthorized' : response.status === 403 ? 'forbidden' : response.status === 409 ? 'conflict' : response.status === 429 ? 'rate-limited' : 'provider-error', status: response.status,
    })
    return response.json()
  }
}
