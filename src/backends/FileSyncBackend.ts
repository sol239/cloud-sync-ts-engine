import { CloudStorageError, type ICloudStorage } from '../cloud'
import type { SyncOperation } from '../types'
import type { ISyncBackend, RemoteSyncEntry, SyncCheckpoint } from './ISyncBackend'

/** Dropbox retains its legacy paths; Drive uses immutable checkpoints. */
export class FileSyncBackend implements ISyncBackend {
  readonly uploadConcurrency = 4
  get provider() { return this.storage.provider }

  constructor(private readonly storage: ICloudStorage, private readonly directory = 'sync/operations') {}

  async list(): Promise<RemoteSyncEntry[]> {
    try {
      const files = await this.storage.listFiles(this.directory, { recursive: true })
      return files.filter(file => file.name.endsWith('.json')).map(file => ({
        id: file.id ?? file.path,
        storageId: file.id,
        path: file.path,
        revision: file.revision,
        modifiedAt: file.modifiedAt,
        kind: file.path === `${this.directory}/full-sync.json` || file.path.startsWith(`${this.directory}/checkpoints/`)
          ? 'checkpoint'
          : 'operation',
        operationId: /^\d{12,}-([0-9a-f-]{36})\.json$/i.exec(file.name)?.[1]
      }))
    } catch (error) {
      if (error instanceof CloudStorageError && error.code === 'not-found') return []
      throw error
    }
  }

  /** Remove this account's sync directory, including checkpoints and invalid files. */
  async deleteAll(): Promise<void> {
    try {
      if (this.storage.deleteDirectory) await this.storage.deleteDirectory(this.directory)
      else await this.storage.deleteFile(this.directory)
    } catch (error) {
      if (!(error instanceof CloudStorageError && error.code === 'not-found')) throw error
    }
  }

  async read(entry: RemoteSyncEntry): Promise<unknown> {
    const file = entry.storageId && this.storage.downloadFileById
      ? await this.storage.downloadFileById(entry.storageId, entry.path!)
      : await this.storage.downloadFile(entry.path ?? entry.id)
    // A mutable checkpoint can change after listing. Acknowledge the downloaded revision.
    entry.revision = file.metadata?.revision || entry.revision
    return JSON.parse(new TextDecoder().decode(file.data))
  }

  async append(operation: SyncOperation): Promise<void> {
    const sequence = String(operation.sequence).padStart(12, '0')
    const path = `${this.directory}/${operation.deviceId}/${sequence}-${operation.id}.json`
    await this.writeImmutable(path, JSON.stringify(operation))
  }

  async publishCheckpoint(checkpoint: SyncCheckpoint, previous?: RemoteSyncEntry): Promise<RemoteSyncEntry> {
    const mutable = this.storage.capabilities?.conditionalWrites !== false
    const path = mutable ? `${this.directory}/full-sync.json` : `${this.directory}/checkpoints/${crypto.randomUUID()}.json`
    const body = JSON.stringify(checkpoint)
    const metadata = mutable
      ? await this.storage.uploadFile(path, body, {
          contentType: 'application/json',
          ...(previous ? { expectedRevision: previous.revision } : { createOnly: true })
        })
      : await this.writeImmutable(path, body)
    return { id: metadata.id ?? path, path, storageId: metadata.id, kind: 'checkpoint', revision: metadata.revision }
  }

  private async writeImmutable(path: string, body: string) {
    try {
      return await this.storage.uploadFile(path, body, { contentType: 'application/json', createOnly: true })
    } catch (error) {
      if (!(error instanceof CloudStorageError && error.code === 'conflict')) throw error
      const existing = await this.storage.downloadFile(path)
      if (new TextDecoder().decode(existing.data) !== body) throw error
      return existing.metadata ?? { revision: 'unknown' }
    }
  }
}
