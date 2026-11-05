import type { ISyncBackend, RemoteSyncEntry } from './backends/ISyncBackend'
import type { SyncOperation } from './types'

export interface InspectedCloudEntry {
  entry: RemoteSyncEntry
  body?: unknown
  error?: string
}
export interface RemoteEntityVersion {
  source: string
  operation: SyncOperation
}
export interface InspectedCloudEntity {
  type: string
  id: string
  label: string
  versions: RemoteEntityVersion[]
}
export interface CloudInspection {
  fetchedAt: string
  entries: InspectedCloudEntry[]
  entities: InspectedCloudEntity[]
}

/** Read remote envelopes only. Never constructs a sync engine or accesses a local store. */
export async function inspectCloud(backend: ISyncBackend, options: {
  signal?: AbortSignal
  progress?: (loaded: number, total: number) => void
} = {}): Promise<CloudInspection> {
  options.signal?.throwIfAborted()
  const listed = await backend.list()
  options.signal?.throwIfAborted()
  const entries: InspectedCloudEntry[] = new Array(listed.length)
  let next = 0
  let loaded = 0
  options.progress?.(0, listed.length)
  await Promise.all(Array.from({ length: Math.min(4, listed.length) }, async () => {
    while (next < listed.length) {
      options.signal?.throwIfAborted()
      const index = next++
      const entry = listed[index]!
      try {
        entries[index] = { entry, body: await backend.read(entry) }
      } catch (error) {
        options.signal?.throwIfAborted()
        entries[index] = { entry, error: error instanceof Error ? error.message : String(error) }
      }
      options.signal?.throwIfAborted()
      options.progress?.(++loaded, listed.length)
    }
  }))
  const entities = new Map<string, InspectedCloudEntity>()
  for (const item of entries) {
    const body = item.body
    const candidates = object(body) && body.format === 'full-sync-v1' && Array.isArray(body.operations)
      ? body.operations
      : [body]
    for (const value of candidates) {
      if (!object(value) || ![1, 2].includes(Number(value.schemaVersion))
        || typeof value.entityType !== 'string' || typeof value.entityId !== 'string'
        || typeof value.id !== 'string' || typeof value.createdAt !== 'string'
        || !['upsert', 'delete'].includes(String(value.action))) continue
      const operation = value as unknown as SyncOperation
      const key = JSON.stringify([operation.entityType, operation.entityId])
      const entity = entities.get(key) ?? { type: operation.entityType, id: operation.entityId, label: operation.entityId, versions: [] }
      entity.versions.push({ source: item.entry.id, operation })
      entities.set(key, entity)
    }
  }
  for (const entity of entities.values()) {
    entity.versions.sort((a, b) => b.operation.createdAt.localeCompare(a.operation.createdAt) || b.operation.id.localeCompare(a.operation.id))
    const payload = entity.versions.find(version => object(version.operation.payload))?.operation.payload
    const name = payload?.title ?? payload?.name ?? payload?.key
    if (typeof name === 'string' && name) entity.label = name
  }
  return { fetchedAt: new Date().toISOString(), entries, entities: [...entities.values()].sort((a, b) => a.type.localeCompare(b.type) || a.label.localeCompare(b.label)) }
}

function object(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === 'object' && !Array.isArray(value)
}
