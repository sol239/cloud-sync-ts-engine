import type { EntitySyncAdapter } from './EntitySyncRegistry'
import type { ISyncBackend } from './backends/ISyncBackend'
import type { SyncAction, SyncOperation, SyncPayload, SyncRecord, SyncStatus } from './types'

/** Structural contract: app entities do not need to import or implement this explicitly. */
export interface ISyncEntity {
  readonly id: string
  toJSON(): object
}

export interface ISyncEntityClass<T extends ISyncEntity = ISyncEntity> {
  readonly entityType: string
  fromJSON(payload: SyncPayload): T
}

export interface IEntityPersistence {
  list(type: string): Promise<ISyncEntity[]>
  read(type: string, id: string): Promise<ISyncEntity | null>
  save(type: string, entity: ISyncEntity): Promise<void>
  delete(type: string, id: string): Promise<void>
}

/** Durable journal and connection receipts. Acknowledgements follow successful writes. */
export interface ISyncStore {
  /** Called under the host write lock; commit related journal changes together. */
  batch?<T>(action: () => Promise<T>): Promise<T>
  initialize(): Promise<void>
  resetConnection(): Promise<void>
  getDeviceId(): Promise<string>
  status(): Promise<SyncStatus>
  enqueue(type: string, id: string, action: SyncAction, payload: SyncPayload | null,
    metadata?: Pick<SyncOperation, 'changedFields' | 'recordChanges' | 'fieldStamps'>): Promise<SyncOperation>
  localOperations(): Promise<SyncOperation[]>
  pendingOperations(): Promise<SyncOperation[]>
  markUploaded(id: string): Promise<void>
  markUploadedMany(ids: string[]): Promise<void>
  appliedOperationIds(): Promise<Set<string>>
  markApplied(operation: SyncOperation): Promise<void>
  markAppliedMany?(operations: SyncOperation[]): Promise<void>
  acknowledgeCovered(ids: string[]): Promise<void>
  snapshots(): Promise<Array<{ entity_type: string, entity_id: string, snapshot: string }>>
  snapshot(type: string, id: string): Promise<SyncPayload | null>
  setSnapshot(type: string, id: string, payload: SyncPayload): Promise<void>
  deleteSnapshot(type: string, id: string): Promise<void>
  entityClocks(type: string, id: string): Promise<Record<string, string>>
  deletionClocks(): Promise<Array<{ entity_type: string, entity_id: string, stamp: string }>>
  fieldIsNewer(operation: SyncOperation, field: string): Promise<boolean>
  setFieldClock(operation: SyncOperation, field: string): Promise<void>
  /** Persist an entity's clocks together instead of rewriting storage per field. */
  setFieldClocks?(operation: SyncOperation, fields: string[]): Promise<void>
  setting(key: string): Promise<string | undefined>
  setSetting(key: string, value: string): Promise<void>
  rejectedFile(id: string): Promise<string | undefined>
  rejectFile(id: string, revision: string): Promise<void>
  setLastSyncedAt(value: string): Promise<void>
}

/** Optional domain behavior; defaults synchronize ordinary JSON fields. */
export interface ISyncPolicy {
  /** Optional field synchronized as individually keyed records rather than one JSON value. */
  recordField?: string
  records(value: unknown): SyncRecord[]
  recordKey(record: SyncRecord): string
  isValidRecord(value: unknown): boolean
  normalize?(payload: SyncPayload): SyncPayload
  expandChangedFields?(type: string, fields: string[], payload: SyncPayload): string[]
  deferRecords?(type: string, records: SyncRecord[]): boolean
  mergeField?(type: string, field: string, current: unknown, incoming: unknown): { value: unknown } | undefined
  afterMerge?(operation: SyncOperation, current: SyncPayload | null, merged: SyncPayload, clocks: string[]): void
  afterPull?(): Promise<number>
}

export const defaultSyncPolicy: ISyncPolicy = {
  records: () => [],
  recordKey: () => { throw new Error('Register a record policy before synchronizing nested records.') },
  isValidRecord: () => false
}

export interface ISyncEngineOptions {
  backend: ISyncBackend
  store: ISyncStore
  entities: EntitySyncAdapter[]
  /** Serialize with ordinary app writes; never hold this lock across HTTP requests. */
  withWrite<T>(action: () => Promise<T>): Promise<T>
  /** Called under withWrite, around an apply batch and its journal persistence. */
  withApplyBatch?<T>(operations: SyncOperation[], action: () => Promise<T>): Promise<T>
  /** Operations per apply transaction (default 100, maximum 1000). */
  applyBatchSize?: number
  /** Atomically persist one entity, including its related rows. */
  writeEntity(action: () => Promise<void>): Promise<void>
  policy?: ISyncPolicy
  report?: (message: string) => void
  bootstrapConnection?: boolean
  createId?: () => string
  checkpointOperationThreshold?: number
  automaticCheckpoints?: boolean
}
