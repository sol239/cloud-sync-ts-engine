export type SyncEntityType = string
export type SyncAction = 'upsert' | 'delete'
export type SyncPayload = Record<string, unknown>

export type SyncRecord = Record<string, unknown>

export interface SyncRecordChanges {
  upserts: SyncRecord[]
  deletedKeys: string[]
}

export interface SyncOperation {
  schemaVersion: 1 | 2
  id: string
  deviceId: string
  sequence: number
  entityType: SyncEntityType
  entityId: string
  action: SyncAction
  payload: SyncPayload | null
  createdAt: string
  changedFields?: string[]
  recordChanges?: SyncRecordChanges
  fieldStamps?: Record<string, string>
}

export interface SyncResult {
  pushed: number
  pulled: number
  applied: number
  finishedAt: string
}

export interface SyncStatus {
  deviceId: string
  pendingOperations: number
  lastSyncedAt: string | null
}
