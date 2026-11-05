import type { ISyncStore } from '../contracts'
import type { SyncAction, SyncOperation, SyncPayload, SyncRecord, SyncStatus } from '../types'
/** localStorage satisfies this interface; asynchronous durable stores work too. */
export interface ISyncStateStorage {
  getItem(key: string): string | null | Promise<string | null>
  setItem(key: string, value: string): void | Promise<void>
}
interface Connection {
  uploaded: string[]
  applied: string[]
  settings: Record<string, string>
}
interface State {
  version: 1
  deviceId: string
  sequence: number
  operations: SyncOperation[]
  snapshots: Record<string, SyncPayload>
  clocks: Record<string, Record<string, string>>
  connections: Record<string, Connection>
}
/** Durable JSON bookkeeping for apps that do not need a custom database journal. */
export class JsonSyncStore implements ISyncStore {
  private batchState?: State
  private state?: State
  private ready?: Promise<void>
  private writes: Promise<void> = Promise.resolve()
  private readonly connectionId: string
  private readonly createId: () => string
  constructor(private readonly options: {
    storage: ISyncStateStorage
    /** Unique per app and local account; shared by its remote connections. */
    key: string
    connectionId?: string
    createId?: () => string
    /** Supply the same key function as a custom keyed-record policy. */
    recordKey?: (record: SyncRecord) => string
  }) {
    if (!options.key)
      throw new Error('A sync state storage key is required.')
    this.connectionId = options.connectionId ?? 'default'
    this.createId = options.createId ?? (() => globalThis.crypto.randomUUID())
  }

  async initialize(): Promise<void> {
    if (!this.ready) {
      this.ready = this.load().catch((error) => {
        this.ready = undefined
        throw error
      })
    }
    await this.ready
  }

  private async load(): Promise<void> {
    const raw = await this.options.storage.getItem(this.options.key)
    if (raw !== null) {
      const state = JSON.parse(raw) as State
      if (!state || state.version !== 1 || typeof state.deviceId !== 'string'
        || !Number.isSafeInteger(state.sequence) || !Array.isArray(state.operations)
        || !state.snapshots || !state.clocks || !state.connections)
        throw new Error('Invalid or unsupported sync state.')
      this.state = state
    } else {
      const state: State = { version: 1, deviceId: this.createId(), sequence: 0,
        operations: [], snapshots: {}, clocks: {}, connections: {} }
      await this.options.storage.setItem(this.options.key, JSON.stringify(state))
      this.state = state
    }
  }

  private connection(state: State): Connection {
    // Encode keys so names such as "__proto__" are ordinary own properties.
    const key = JSON.stringify(this.connectionId)
    return state.connections[key] ??= { uploaded: [], applied: [], settings: {} }
  }

  private key(type: string, id: string): string { return JSON.stringify([type, id]) }
  private stamp(op: SyncOperation, field: string): string {
    return op.fieldStamps?.[field] ?? `${op.createdAt}|${op.id}`
  }

  private async read<T>(action: (state: State) => T): Promise<T> {
    await this.initialize()
    await this.writes
    return structuredClone(action(this.batchState ?? this.state!))
  }

  private async change<T>(action: (state: State) => T): Promise<T> {
    await this.initialize()
    if (this.batchState) return structuredClone(action(this.batchState))
    const write = this.writes.then(async () => {
      const next = structuredClone(this.state!)
      const result = action(next)
      await this.options.storage.setItem(this.options.key, JSON.stringify(next))
      this.state = next
      return structuredClone(result)
    })
    this.writes = write.then(() => { }, () => { })
    return write
  }

  /** The engine holds the host write lock for this entire local transaction. */
  async batch<T>(action: () => Promise<T>): Promise<T> {
    await this.initialize()
    await this.writes
    if (this.batchState) return action()
    const draft = structuredClone(this.state!)
    this.batchState = draft
    try {
      const result = await action()
      await this.options.storage.setItem(this.options.key, JSON.stringify(draft))
      this.state = draft
      return result
    } finally { this.batchState = undefined }
  }

  async getDeviceId() { return this.read(state => state.deviceId) }
  async status(): Promise<SyncStatus> {
    return this.read((state) => {
      const connection = this.connection(state)
      const uploaded = new Set(connection.uploaded)
      return { deviceId: state.deviceId,
        pendingOperations: state.operations.filter(op => !uploaded.has(op.id)).length,
        lastSyncedAt: connection.settings.lastSyncedAt ?? null }
    })
  }

  async resetConnection(): Promise<void> {
    await this.change((state) => {
      state.connections[JSON.stringify(this.connectionId)] = { uploaded: [], applied: [], settings: {} }
    })
  }

  async enqueue(type: string, id: string, action: SyncAction, payload: SyncPayload | null, metadata: Pick<SyncOperation, 'changedFields' | 'recordChanges' | 'fieldStamps'> = {}): Promise<SyncOperation> {
    return this.change((state) => {
      const clocks = state.clocks[this.key(type, id)] ?? {}
      const latest = [state.operations.at(-1)?.createdAt, ...Object.values(clocks).map(stamp => stamp.split('|')[0])]
        .filter((value): value is string => !!value).sort().at(-1)
      const now = Date.now()
      const createdAt = new Date(Math.max(now, latest ? Date.parse(latest) + 1 : now)).toISOString()
      const op: SyncOperation = {
        schemaVersion: 2, id: this.createId(), deviceId: state.deviceId, sequence: ++state.sequence,
        entityType: type, entityId: id, action, payload: structuredClone(payload), createdAt,
        ...(action === 'upsert'
          ? { changedFields: Object.keys(payload ?? {}).filter(field => field !== 'id'),
              recordChanges: { upserts: [], deletedKeys: [] } }
          : {}), ...structuredClone(metadata)
      }
      state.operations.push(op)
      this.connection(state).applied.push(op.id)
      if (op.recordChanges?.upserts.length && !this.options.recordKey) {
        throw new Error('JsonSyncStore requires recordKey for keyed-record upserts.')
      }
      // Keyed record stamps are supplied by the engine/policy; their keys must match
      // the policy's recordKey implementation, so preserve explicit field stamps.
      const fields = action === 'delete' ? ['__deleted__'] : [...(op.changedFields ?? []), '__entity__']
      for (const field of [...fields, ...Object.keys(op.fieldStamps ?? {})])
        clocks[field] = [clocks[field] ?? '', this.stamp(op, field)].sort().at(-1)!
      for (const key of op.recordChanges?.deletedKeys ?? [])
        clocks[`record:${key}`] = this.stamp(op, `record:${key}`)
      for (const record of op.recordChanges?.upserts ?? []) {
        const field = `record:${this.options.recordKey!(record)}`
        clocks[field] = [clocks[field] ?? '', this.stamp(op, field)].sort().at(-1)!
      }
      state.clocks[this.key(type, id)] = clocks
      return op
    })
  }

  async localOperations() { return this.read(state => state.operations) }
  async pendingOperations() {
    return this.read((state) => {
      const uploaded = new Set(this.connection(state).uploaded)
      return state.operations.filter(op => !uploaded.has(op.id))
    })
  }
  async markUploaded(id: string) { await this.markUploadedMany([id]) }
  async markUploadedMany(ids: string[]) {
    await this.change((state) => {
      const connection = this.connection(state)
      connection.uploaded = [...new Set([...connection.uploaded, ...ids])]
    })
  }

  async appliedOperationIds() { return this.read(state => new Set(this.connection(state).applied)) }
  async markApplied(op: SyncOperation) { await this.markAppliedMany([op]) }
  async markAppliedMany(operations: SyncOperation[]) {
    await this.change((state) => {
      // Retain imported IDs and clocks for forwarding to other destinations.
      const known = new Set(state.operations.map(operation => operation.id))
      for (const operation of operations) if (!known.has(operation.id)) {
        state.operations.push(structuredClone(operation))
        known.add(operation.id)
      }
      const connection = this.connection(state)
      connection.applied = [...new Set([...connection.applied, ...operations.map(operation => operation.id)])]
    })
  }
  async acknowledgeCovered(ids: string[]) {
    await this.change((state) => {
      const connection = this.connection(state)
      connection.applied = [...new Set([...connection.applied, ...ids])]
    })
  }

  async snapshots() {
    return this.read(state => Object.entries(state.snapshots).map(([key, payload]) => {
      const [entity_type, entity_id] = JSON.parse(key) as [
        string,
        string
      ]
      return { entity_type, entity_id, snapshot: JSON.stringify(payload) }
    }))
  }

  async snapshot(type: string, id: string) { return this.read(state => state.snapshots[this.key(type, id)] ?? null) }
  async setSnapshot(type: string, id: string, payload: SyncPayload) {
    await this.change((state) => {
      state.snapshots[this.key(type, id)] = structuredClone(payload)
    })
  }

  async deleteSnapshot(type: string, id: string) {
    await this.change((state) => {
      Reflect.deleteProperty(state.snapshots, this.key(type, id))
    })
  }

  async entityClocks(type: string, id: string) { return this.read(state => state.clocks[this.key(type, id)] ?? {}) }
  async deletionClocks() {
    return this.read(state => Object.entries(state.clocks).flatMap(([key, clocks]) => {
      if (!clocks.__deleted__)
        return []
      const [entity_type, entity_id] = JSON.parse(key) as [
        string,
        string
      ]
      return [{ entity_type, entity_id, stamp: clocks.__deleted__ }]
    }))
  }

  async fieldIsNewer(op: SyncOperation, field: string) {
    return this.read(state => this.stamp(op, field) > (state.clocks[this.key(op.entityType, op.entityId)]?.[field] ?? ''))
  }

  async setFieldClock(op: SyncOperation, field: string) {
    await this.setFieldClocks(op, [field])
  }

  async setFieldClocks(op: SyncOperation, fields: string[]) {
    await this.change((state) => {
      const clocks = state.clocks[this.key(op.entityType, op.entityId)] ??= {}
      for (const field of fields) {
        const stamp = this.stamp(op, field)
        if (stamp > (clocks[field] ?? ''))
          clocks[field] = stamp
      }
    })
  }

  async setting(key: string) { return this.read(state => this.connection(state).settings[JSON.stringify(key)]) }
  async setSetting(key: string, value: string) {
    await this.change((state) => {
      this.connection(state).settings[JSON.stringify(key)] = value
    })
  }

  async rejectedFile(id: string) { return this.setting(`rejected:${id}`) }
  async rejectFile(id: string, revision: string) { await this.setSetting(`rejected:${id}`, revision) }
  async setLastSyncedAt(value: string) {
    await this.change((state) => {
      this.connection(state).settings.lastSyncedAt = value
    })
  }
}
