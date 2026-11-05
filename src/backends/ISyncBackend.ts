import type { SyncOperation } from '../types'

export interface SyncCheckpoint {
  format: 'full-sync-v1'
  createdAt: string
  operations: SyncOperation[]
  coveredOperationIds: string[]
  /** Checkpoint revisions already merged into this snapshot. */
  coveredCheckpoints?: { id: string, revision: string }[]
}

export interface RemoteSyncEntry {
  id: string
  revision: string
  modifiedAt?: string
  kind: 'operation' | 'checkpoint'
  operationId?: string
  storageId?: string
  path?: string
}

/** Remote operation protocol. Paths, provider pagination and SQL stay in adapters. */
export interface ISyncBackend {
  readonly provider: string
  /** Whether checkpoint publication requires uploading immutable history first. */
  readonly requiresHistoryBeforeCheckpoint?: boolean
  /** Independent immutable appends may be transferred concurrently. Defaults to one. */
  readonly uploadConcurrency?: number
  deleteAll(): Promise<void>
  close?(): Promise<void>
  list(): Promise<RemoteSyncEntry[]>
  read(entry: RemoteSyncEntry): Promise<unknown>
  append(operation: SyncOperation): Promise<void>
  publishCheckpoint(checkpoint: SyncCheckpoint, previous?: RemoteSyncEntry): Promise<RemoteSyncEntry>
}
