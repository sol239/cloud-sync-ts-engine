import { CloudStorageError, type CloudAccessTokenProvider, type CloudFetch } from '../cloud'
import type { SyncOperation } from '../types'
import type { ISyncBackend, RemoteSyncEntry, SyncCheckpoint } from './ISyncBackend'

/** Append-only JSONB envelopes, scoped by Supabase Auth and a remote space. */
export class SupabaseSyncBackend implements ISyncBackend {
  readonly provider = 'supabase' as const
  readonly requiresHistoryBeforeCheckpoint = true
  private readonly url: string
  private readonly fetcher: CloudFetch

  constructor(url: string, private readonly publishableKey: string, private readonly space: string,
    private readonly getAccessToken: CloudAccessTokenProvider, fetcher: CloudFetch = globalThis.fetch) {
    const endpoint = new URL(url)
    if (!['http:', 'https:'].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
      throw new Error('A valid Supabase HTTP endpoint is required.')
    }
    this.url = endpoint.href.replace(/\/$/, '')
    if (!publishableKey || !space || space.length > 256) throw new Error('Supabase publishable key and sync space are required.')
    this.fetcher = fetcher.bind(globalThis)
  }

  async list(): Promise<RemoteSyncEntry[]> {
    const result: RemoteSyncEntry[] = []
    // Paginate by stable insertion seq rather than UUID (which has no natural order for v4).
    // Rescanning from seq=0 each run ensures late/concurrent inserts are never missed.
    let after = 0
    while (true) {
      const rows = await this.rpc('lyra_sync_list', { p_space: this.space, p_after: after }) as Array<{ id: string, kind: RemoteSyncEntry['kind'], seq: number }>
      for (const row of rows) result.push({ id: row.id, kind: row.kind, revision: row.id, operationId: row.kind === 'operation' ? row.id : undefined })
      if (!rows.length) return result
      after = rows.at(-1)!.seq
    }
  }

  async deleteAll(): Promise<void> {
    await this.rpc('lyra_sync_delete_all', { p_space: this.space })
  }

  async read(entry: RemoteSyncEntry): Promise<unknown> {
    return await this.rpc('lyra_sync_read', { p_space: this.space, p_id: entry.id })
  }

  async append(operation: SyncOperation): Promise<void> {
    await this.rpc('lyra_sync_append', { p_space: this.space, p_id: operation.id, p_kind: 'operation', p_body: operation })
  }

  async publishCheckpoint(checkpoint: SyncCheckpoint): Promise<RemoteSyncEntry> {
    const id = crypto.randomUUID()
    await this.rpc('lyra_sync_append', { p_space: this.space, p_id: id, p_kind: 'checkpoint', p_body: checkpoint })
    return { id, revision: id, kind: 'checkpoint' }
  }

  private async rpc(name: string, body: unknown): Promise<unknown> {
    let response: Response
    try {
      response = await this.fetcher(`${this.url}/rest/v1/rpc/${name}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'apikey': this.publishableKey, 'Authorization': `Bearer ${await this.getAccessToken()}` },
        body: JSON.stringify(body)
      })
    } catch (cause) {
      throw new CloudStorageError('Could not reach Supabase.', { code: 'network', cause })
    }
    if (!response.ok) {
      throw new CloudStorageError(`Supabase sync failed (${response.status}).`, {
        code: response.status === 401 ? 'unauthorized' : response.status === 403 ? 'forbidden' : response.status === 409 ? 'conflict' : response.status === 429 ? 'rate-limited' : 'provider-error',
        status: response.status
      })
    }
    return await response.json()
  }
}
