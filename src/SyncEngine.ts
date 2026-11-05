import { EntitySyncRegistry } from './EntitySyncRegistry'
import { defaultSyncPolicy } from './contracts'
import type { ISyncEngineOptions, ISyncPolicy, ISyncStore } from './contracts'
import type { ISyncBackend, RemoteSyncEntry, SyncCheckpoint } from './backends/ISyncBackend'
import type { SyncEntityType, SyncOperation, SyncPayload, SyncRecord, SyncResult } from './types'

interface EntitySnapshot {
  entityType: SyncEntityType
  entityId: string
  payload: SyncPayload
}

export class SyncEngine {
  private readonly registry: EntitySyncRegistry
  private coveredIds = new Set<string>()
  private coveredCheckpoints = new Map<string, string>()
  private checkpointCreatedAt?: string
  private readonly checkpointOperationThreshold: number
  private readonly automaticCheckpoints: boolean
  private readonly backend: ISyncBackend
  private readonly store: ISyncStore
  private readonly policy: ISyncPolicy
  private readonly recordField: string
  private readonly report: (message: string) => void
  private readonly withWrite: ISyncEngineOptions['withWrite']
  private readonly withApplyBatch: ISyncEngineOptions['withApplyBatch']
  private readonly applyBatchSize: number
  private readonly writeEntity: ISyncEngineOptions['writeEntity']
  private readonly connection: boolean
  private readonly createId: () => string
  private lastYield = Date.now()

  private async yieldToBrowser(): Promise<void> {
    // Resolved database promises only yield to microtasks, starving rendering
    // and input during large browser imports. Give the event loop a turn.
    if (typeof window === 'undefined' || typeof setTimeout === 'undefined' || Date.now() - this.lastYield < 8) return
    await new Promise<void>(resolve => setTimeout(resolve, 0))
    this.lastYield = Date.now()
  }

  constructor(options: ISyncEngineOptions) {
    const threshold = options.checkpointOperationThreshold ?? 100
    if (!Number.isSafeInteger(threshold) || threshold < 1) throw new Error('Checkpoint operation threshold must be a positive whole number.')
    this.checkpointOperationThreshold = threshold
    this.automaticCheckpoints = options.automaticCheckpoints ?? true
    this.registry = new EntitySyncRegistry(options.entities)
    this.backend = options.backend
    this.store = options.store
    this.policy = options.policy ?? defaultSyncPolicy
    this.recordField = this.policy.recordField ?? '__sync_records__'
    this.report = options.report ?? (() => {})
    this.applyBatchSize = options.applyBatchSize ?? 100
    if (!Number.isSafeInteger(this.applyBatchSize) || this.applyBatchSize < 1 || this.applyBatchSize > 1000) throw new Error('Apply batch size must be between 1 and 1000.')
    this.withApplyBatch = options.withApplyBatch
    this.withWrite = options.withWrite
    this.writeEntity = options.writeEntity
    this.connection = options.bootstrapConnection ?? false
    this.createId = options.createId ?? (() => globalThis.crypto.randomUUID())
  }

  private withJournalWrite<T>(action: () => Promise<T>, operations?: SyncOperation[]): Promise<T> {
    return this.withWrite(() => {
      const persist = () => this.store.batch ? this.store.batch(action) : action()
      return operations && this.withApplyBatch ? this.withApplyBatch(operations, persist) : persist()
    })
  }

  async sync(options: { full?: boolean } = {}): Promise<SyncResult> {
    const started = Date.now()
    this.lastYield = started
    this.report('Initializing local sync database…')
    await this.store.initialize()
    const status = await this.store.status()
    let bootstrap = await this.store.setting('bootstrap')
    if (!bootstrap && !status.lastSyncedAt && !(await this.store.snapshots()).length
      && !(await this.store.localOperations()).length) {
      bootstrap = await this.withJournalWrite(async () => {
        const original = await this.collectSnapshots()
        const value = JSON.stringify(original)
        await this.store.setSetting('bootstrap', value)
        return value
      })
    }
    if (bootstrap && bootstrap !== 'done') {
      // Baseline existing local fields before importing shared cloud entities.
      // Real offline measurements are queued independently, so seeds cannot
      // overwrite cloud fields while locally recorded data remains transferable.
      await this.withJournalWrite(async () => {
        for (const original of JSON.parse(bootstrap!) as EntitySnapshot[]) {
          if (await this.store.snapshot(original.entityType, original.entityId)) continue
          const recordChanges = this.recordDiff([], original.payload[this.recordField])
          if (recordChanges.upserts.length) {
            await this.store.enqueue(original.entityType, original.entityId, 'upsert', original.payload, {
              changedFields: [], recordChanges
            })
          }
          await this.store.setSnapshot(original.entityType, original.entityId, original.payload)
        }
      })
    } else {
      this.report('Checking local changes…')
      await this.captureLocalChanges()
    }
    this.report(`Listing ${this.backend.provider} operations…`)
    const files = await this.listOperationFiles()
    this.report(`Found ${files.length} remote files (${((Date.now() - started) / 1000).toFixed(1)}s elapsed).`)
    const snapshotResult = await this.pullSnapshot(files, options.full === true)
    const changes = await this.pull(files.filter(file => file.kind === 'operation'), options.full === true)
    const pulled = snapshotResult.pulled + changes.pulled
    const applied = snapshotResult.applied + changes.applied
    const restored = await this.withWrite(() => this.policy.afterPull?.() ?? Promise.resolve(0))
    if (restored) this.report(`Restored legacy measured attributes for ${restored} activities.`)
    if (bootstrap && bootstrap !== 'done') {
      await this.withJournalWrite(async () => {
        for (const original of JSON.parse(bootstrap!) as EntitySnapshot[]) {
          if (!await this.store.setting(`remote:${this.key(original.entityType, original.entityId)}`)) {
            await this.store.deleteSnapshot(original.entityType, original.entityId)
          }
        }
        await this.captureLocalChangesUnlocked()
        await this.store.setSetting('bootstrap', 'done')
      })
    }
    // Capture edits made while files were downloading, including unrelated entities.
    await this.captureLocalChanges()
    // A new target needs current data and tombstones, including entities originally
    // imported from another target. Preserve clocks rather than invent newer edits.
    const baseline = await this.store.setting('auto-checkpoint-baseline-at') ?? status.lastSyncedAt ?? new Date(started).toISOString()
    const automaticCheckpoint = this.automaticCheckpoints && !options.full && !(this.connection && !status.lastSyncedAt)
      && await this.shouldPublishAutomaticCheckpoint(files, baseline)
    if (automaticCheckpoint) this.report(`Creating automatic checkpoint (${this.checkpointOperationThreshold} uncovered operations or 24 hours with changes)…`)
    let pushed: number
    if (options.full || (this.connection && !status.lastSyncedAt)) {
      pushed = await this.publishSnapshot(files)
    } else {
      // Complete normal delivery first. A failed upload must not publish a checkpoint.
      pushed = await this.push(files)
      if (automaticCheckpoint) {
        const refreshed = await this.listOperationFiles()
        pushed += await this.publishSnapshot(refreshed)
      }
    }
    const finishedAt = new Date().toISOString()
    if (!await this.store.setting('auto-checkpoint-baseline-at')) await this.store.setSetting('auto-checkpoint-baseline-at', finishedAt)
    await this.store.setLastSyncedAt(finishedAt)
    this.report(`Sync complete in ${((Date.now() - started) / 1000).toFixed(1)}s: sent ${pushed}, received ${pulled}, applied ${applied}.`)
    return { pushed, pulled, applied, finishedAt }
  }

  async deleteCloudData(): Promise<void> {
    await this.store.initialize()
    await this.backend.deleteAll()
    await this.withWrite(() => this.store.resetConnection())
  }

  async getStatus() {
    await this.store.initialize()
    return await this.store.status()
  }

  private async shouldPublishAutomaticCheckpoint(files: RemoteSyncEntry[], baseline: string): Promise<boolean> {
    const applied = await this.store.appliedOperationIds()
    const uncovered = new Set((await this.store.localOperations()).map(operation => operation.id)
      .concat(files.flatMap(file => file.kind === 'operation' && file.operationId ? [file.operationId] : []))
      .filter(id => applied.has(id) && !this.coveredIds.has(id)))
    if (!uncovered.size) return false
    return uncovered.size >= this.checkpointOperationThreshold || Date.now() - Date.parse(this.checkpointCreatedAt ?? baseline) >= 24 * 60 * 60 * 1000
  }

  private rememberCheckpointTime(value: unknown): void {
    if (typeof value !== 'string') return
    const time = Date.parse(value)
    if (Number.isFinite(time) && time <= Date.now()
      && (!this.checkpointCreatedAt || time > Date.parse(this.checkpointCreatedAt))) this.checkpointCreatedAt = value
  }

  private async pullSnapshot(files: RemoteSyncEntry[], replay: boolean): Promise<{ pulled: number, applied: number }> {
    this.coveredIds = new Set()
    this.coveredCheckpoints = new Map()
    this.checkpointCreatedAt = undefined
    let pulled = 0
    let applied = 0
    // Start with the newest snapshot, skipping only revisions it explicitly
    // covers. Independent concurrent branches still need to be imported.
    const checkpoints = files.filter(file => file.kind === 'checkpoint')
      .sort((a, b) => (b.modifiedAt ?? '').localeCompare(a.modifiedAt ?? ''))
    for (const file of checkpoints) {
      if (this.coveredCheckpoints.get(file.id) === file.revision) continue
      const key = `checkpoint:${file.id}`
      const remembered = await this.store.setting(key)
      if (!replay && remembered) {
        const cached = JSON.parse(remembered) as { revision: string, covered: string[], checkpoints?: { id: string, revision: string }[], createdAt?: string }
        if (cached.revision === file.revision) {
          for (const id of cached.covered) this.coveredIds.add(id)
          for (const checkpoint of cached.checkpoints ?? []) this.coveredCheckpoints.set(checkpoint.id, checkpoint.revision)
          this.coveredCheckpoints.set(file.id, file.revision)
          this.rememberCheckpointTime(cached.createdAt)
          continue
        }
      }
      this.report(`Downloading checkpoint ${pulled + 1}: ${file.path ?? file.id}…`)
      const value = await this.backend.read(file) as SyncCheckpoint
      if (!value || value.format !== 'full-sync-v1' || !Array.isArray(value.operations)
        || !Array.isArray(value.coveredOperationIds) || value.coveredOperationIds.some(id => typeof id !== 'string')
        || (value.coveredCheckpoints !== undefined && (!Array.isArray(value.coveredCheckpoints)
          || value.coveredCheckpoints.some(checkpoint => !checkpoint || typeof checkpoint.id !== 'string' || typeof checkpoint.revision !== 'string')))
        || !value.operations.every(operation => this.isValidOperation(operation))) {
        throw new Error('Invalid or unsupported full sync snapshot.')
      }
      this.report(`Checkpoint downloaded: ${value.operations.length} entity changes; covers ${value.coveredOperationIds.length} historical operations (${value.createdAt}).`)
      applied += await this.applyOperations(value.operations)
      // Snapshot operations are already present in this remote checkpoint.
      // Retain them for forwarding to other destinations, but do not re-upload
      // or count them as uncovered changes on the source connection.
      const covered = [...new Set([...value.coveredOperationIds, ...value.operations.map(operation => operation.id)])]
      await this.withJournalWrite(async () => {
        await this.store.acknowledgeCovered(covered)
        await this.store.setSetting(key, JSON.stringify({ revision: file.revision, covered, checkpoints: value.coveredCheckpoints, createdAt: value.createdAt }))
      })
      for (const id of covered) this.coveredIds.add(id)
      for (const checkpoint of value.coveredCheckpoints ?? []) this.coveredCheckpoints.set(checkpoint.id, checkpoint.revision)
      this.coveredCheckpoints.set(file.id, file.revision)
      this.rememberCheckpointTime(value.createdAt)
      pulled++
    }
    return { pulled, applied }
  }

  private async publishSnapshot(files: RemoteSyncEntry[]): Promise<number> {
    // Postgres can verify checkpoint coverage only after immutable history exists.
    const uploadedOperations = this.backend.requiresHistoryBeforeCheckpoint ? await this.push(files) : 0
    const { snapshot, local } = await this.withJournalWrite(async () => {
      // Include writes made while downloads were running in this same checkpoint.
      await this.captureLocalChangesUnlocked()
      const current = await this.collectSnapshots()
      const currentKeys = new Set(current.map(item => this.key(item.entityType, item.entityId)))
      const deviceId = await this.store.getDeviceId()
      const createdAt = new Date().toISOString()
      const operations: SyncOperation[] = []
      for (const entity of current) {
        const clocks = await this.store.entityClocks(entity.entityType, entity.entityId)
        const payload = { ...entity.payload }
        // Keep null fields and per-record tombstones after compaction.
        for (const field of Object.keys(clocks)) {
          if (!field.startsWith('__') && !field.startsWith('record:') && !(field in payload)) payload[field] = null
        }
        const records = this.records(payload[this.recordField])
        const recordKeys = new Set(records.map(record => this.recordKey(record)))
        const changedFields = Object.keys(payload).filter(field => field !== 'id' && field !== this.recordField)
        const deletedKeys = Object.keys(clocks).filter(field => field.startsWith('record:') && !recordKeys.has(field.slice(7))).map(field => field.slice(7))
        const fields = [...changedFields, '__entity__', ...records.map(record => `record:${this.recordKey(record)}`), ...deletedKeys.map(key => `record:${key}`)]
        operations.push({
          schemaVersion: 2, id: this.createId(), deviceId, sequence: operations.length + 1,
          entityType: entity.entityType, entityId: entity.entityId, action: 'upsert', payload, createdAt,
          changedFields, recordChanges: { upserts: records, deletedKeys },
          fieldStamps: Object.fromEntries(fields.map(field => [field, clocks[field] ?? '1970-01-01T00:00:00.000Z|baseline']))
        })
      }
      for (const clock of await this.store.deletionClocks()) {
        if (currentKeys.has(this.key(clock.entity_type, clock.entity_id))) continue
        operations.push({
          schemaVersion: 2, id: this.createId(), deviceId, sequence: operations.length + 1,
          entityType: clock.entity_type, entityId: clock.entity_id, action: 'delete', payload: null,
          createdAt, fieldStamps: { __deleted__: clock.stamp }
        })
      }
      const local = await this.store.localOperations()
      const applied = await this.store.appliedOperationIds()
      const history = new Set([...(this.backend.requiresHistoryBeforeCheckpoint ? [] : this.coveredIds), ...local.map(operation => operation.id),
        ...files.flatMap(file => file.operationId ? [file.operationId] : [])])
      const coveredOperationIds = [...history].filter(id => applied.has(id))
      const coveredCheckpoints = [...this.coveredCheckpoints].map(([id, revision]) => ({ id, revision }))
      return { snapshot: { format: 'full-sync-v1', createdAt, operations, coveredOperationIds, coveredCheckpoints } satisfies SyncCheckpoint, local }
    })
    this.report('Publishing storage checkpoint…')
    const existing = files.find(file => file.kind === 'checkpoint')
    const uploaded = await this.backend.publishCheckpoint(snapshot, existing)
    this.coveredIds = new Set(snapshot.coveredOperationIds)
    await this.withJournalWrite(async () => {
      await this.store.markUploadedMany(local.map(operation => operation.id))
      await this.store.setSetting(`checkpoint:${uploaded.id}`, JSON.stringify({ revision: uploaded.revision, covered: snapshot.coveredOperationIds, checkpoints: snapshot.coveredCheckpoints, createdAt: snapshot.createdAt }))
    })
    return uploadedOperations + 1
  }

  private async captureLocalChanges(force = false): Promise<void> {
    await this.withWrite(() => this.captureLocalChangesUnlocked(force))
  }

  private async captureLocalChangesUnlocked(force = false): Promise<void> {
    const capture = () => this.captureLocalChangesBatch(force)
    if (this.store.batch) await this.store.batch(capture)
    else await capture()
  }

  private async captureLocalChangesBatch(force = false): Promise<void> {
    const current = await this.collectSnapshots()
    const stored = await this.store.snapshots()
    const storedByKey = new Map(stored.map(row => [this.key(row.entity_type, row.entity_id), JSON.parse(row.snapshot) as SyncPayload]))
    const currentKeys = new Set<string>()
    this.report(`Comparing ${current.length} local entities with ${stored.length} saved snapshots${force ? ' (full sync)' : ''}.`)
    let compared = 0
    for (const snapshot of current) {
      if (++compared % 100 === 0) this.report(`Comparing local entities: ${compared}/${current.length}…`)
      const key = this.key(snapshot.entityType, snapshot.entityId)
      currentKeys.add(key)
      await this.captureSnapshot(snapshot, storedByKey.get(key) ?? null, force)
      await this.yieldToBrowser()
    }
    for (const previous of stored) {
      if (currentKeys.has(this.key(previous.entity_type, previous.entity_id))) continue
      await this.store.enqueue(previous.entity_type, previous.entity_id, 'delete', null)
      await this.store.deleteSnapshot(previous.entity_type, previous.entity_id)
    }
  }

  private async captureSnapshot(snapshot: EntitySnapshot, previous: SyncPayload | null, force = false): Promise<void> {
    const { entityType, entityId, payload } = snapshot
    for (const field of Object.keys(previous ?? {})) {
      if (field !== this.recordField && !Object.hasOwn(payload, field)) payload[field] = null
    }
    const fields = [...new Set([...Object.keys(previous ?? {}), ...Object.keys(payload)])]
      .filter(field => field !== this.recordField && field !== 'id')
    let changedFields = fields.filter(field => this.stableStringify(previous?.[field] ?? null) !== this.stableStringify(payload[field] ?? null))
    changedFields = this.policy.expandChangedFields?.(entityType, changedFields, payload) ?? changedFields
    const recordChanges = this.recordDiff(previous?.[this.recordField], payload[this.recordField])
    if (previous && !changedFields.length && !recordChanges.upserts.length && !recordChanges.deletedKeys.length && !force) return
    const fieldStamps: Record<string, string> = {}
    if (force) {
      const clocks = await this.store.entityClocks(entityType, entityId)
      for (const field of fields) {
        if (!changedFields.includes(field)) fieldStamps[field] = clocks[field] ?? '1970-01-01T00:00:00.000Z|baseline'
      }
      if (!changedFields.length && !recordChanges.upserts.length && !recordChanges.deletedKeys.length) {
        fieldStamps.__entity__ = clocks.__entity__ ?? Object.values(clocks).sort().at(-1) ?? '1970-01-01T00:00:00.000Z|baseline'
      }
      const changedKeys = new Set(recordChanges.upserts.map(record => this.recordKey(record)))
      for (const record of this.records(payload[this.recordField])) {
        const key = `record:${this.recordKey(record)}`
        if (!changedKeys.has(this.recordKey(record))) fieldStamps[key] = clocks[key] ?? '1970-01-01T00:00:00.000Z|baseline'
      }
      changedFields = fields
      recordChanges.upserts = this.records(payload[this.recordField])
    }
    await this.store.enqueue(entityType, entityId, 'upsert', payload, { changedFields, recordChanges, fieldStamps })
    await this.store.setSnapshot(entityType, entityId, payload)
  }

  private async push(files: RemoteSyncEntry[]): Promise<number> {
    const present = new Set(files.filter(file => file.kind === 'operation').map(file => file.operationId))
    const operations = await this.store.localOperations()
    const pending = new Set((await this.store.pendingOperations()).map(operation => operation.id))
    const covered: string[] = []
    const uploads: SyncOperation[] = []
    for (const operation of operations) {
      await this.yieldToBrowser()
      if (this.coveredIds.has(operation.id) || (present.has(operation.id) && !pending.has(operation.id))) {
        if (pending.has(operation.id)) covered.push(operation.id)
      } else uploads.push(operation)
    }
    if (covered.length) await this.store.markUploadedMany(covered)
    const concurrency = Math.max(1, Math.min(4, Math.floor(this.backend.uploadConcurrency ?? 1) || 1))
    let pushed = 0
    for (let offset = 0; offset < uploads.length; offset += concurrency) {
      await this.yieldToBrowser()
      const batch = uploads.slice(offset, offset + concurrency)
      const results = await Promise.allSettled(batch.map(async (operation) => {
        this.report(`Uploading ${operation.entityType} ${operation.entityId}…`)
        await this.backend.append(operation)
        return operation
      }))
      const successful = results.flatMap(result => result.status === 'fulfilled' ? [result.value] : [])
      const acknowledgements = successful.filter(operation => pending.has(operation.id)).map(operation => operation.id)
      if (acknowledgements.length) await this.store.markUploadedMany(acknowledgements)
      pushed += successful.filter(operation => !present.has(operation.id)).length
      const failed = results.find(result => result.status === 'rejected')
      if (failed?.status === 'rejected') throw failed.reason
    }
    return pushed
  }

  private async listOperationFiles(): Promise<RemoteSyncEntry[]> {
    return await this.backend.list()
  }

  private async pull(files: RemoteSyncEntry[], replay = false): Promise<{ pulled: number, applied: number }> {
    const operations: SyncOperation[] = []
    const appliedIds = await this.store.appliedOperationIds()
    this.report('Checking remote history for new operations…')
    let skipped = 0
    const candidates: RemoteSyncEntry[] = []
    for (const file of files) {
      await this.yieldToBrowser()
      const operationId = file.operationId
      if ((operationId && this.coveredIds.has(operationId)) || (!replay && ((operationId && appliedIds.has(operationId))
        || await this.store.rejectedFile(file.id) === (file.revision ?? 'unknown')))) {
        skipped++
        continue
      }
      candidates.push(file)
    }
    // Limit concurrent requests; failed network transfers remain retryable.
    let next = 0
    let completed = 0
    let active = 0
    const concurrency = Math.min(4, candidates.length)
    const progress = (detail: string) => this.report(`Downloading changes: ${completed}/${candidates.length} files complete; ${active} active (up to ${concurrency} parallel); ${skipped} skipped. ${detail}`)
    progress(candidates.length ? 'Fetching changes outside checkpoint coverage…' : 'No new change files to download.')
    const downloads = await Promise.allSettled(Array.from({ length: concurrency }, async () => {
      while (next < candidates.length) {
        const file = candidates[next++]!
        active++
        progress(`Fetching ${file.path ?? file.id}…`)
        let value: unknown
        try {
          value = await this.backend.read(file)
        } catch (error) {
          if ((error as { name?: string } | null)?.name !== 'SyntaxError') {
            this.report(`Download failed: ${file.path ?? file.id}.`)
            throw error
          }
          await this.store.rejectFile(file.id, file.revision ?? 'unknown')
          this.report(`Ignored invalid JSON: ${file.id}.`)
          continue
        } finally {
          active--
          completed++
          progress(`Finished request for ${file.path ?? file.id}.`)
        }
        if (value && typeof value === 'object' && 'schemaVersion' in value
          && typeof value.schemaVersion === 'number' && value.schemaVersion > 2) {
          this.report(`Skipped operation from a newer app version: ${file.id}.`)
          continue
        }
        if (!this.isValidOperation(value)) {
          await this.store.rejectFile(file.id, file.revision ?? 'unknown')
          this.report(`Ignored invalid or unsupported operation: ${file.id}.`)
          continue
        }
        if (!replay && appliedIds.has(value.id)) continue
        operations.push(value)
        appliedIds.add(value.id)
        progress(`Received ${value.action} ${value.entityType} ${value.entityId}.`)
      }
    }))
    const failed = downloads.find(result => result.status === 'rejected')
    if (failed?.status === 'rejected') throw failed.reason

    this.report(`Downloaded ${operations.length} new operations; skipped ${skipped} already processed files.`)
    const applied = await this.applyOperations(operations)
    return { pulled: operations.length, applied }
  }

  private async applyOperations(operations: SyncOperation[]): Promise<number> {
    const upserts = operations
      .filter(operation => operation.action === 'upsert')
      .sort((a, b) => this.operationOrder(a, b, false))
    const deletes = operations
      .filter(operation => operation.action === 'delete')
      .sort((a, b) => this.operationOrder(a, b, true))

    let applied = 0
    const deferredRecords: SyncOperation[] = []
    const applyBatches = async (items: SyncOperation[], defer: boolean) => {
      for (let offset = 0; offset < items.length; offset += this.applyBatchSize) {
        const batch = items.slice(offset, offset + this.applyBatchSize)
        // Preserve dependency order, but serialize the journal only once per batch.
        // Commit successful records even if a later host write fails, so retrying
        // does not mistake those imported records for new local edits.
        let failure: { error: unknown } | undefined
        const acknowledged: SyncOperation[] = []
        await this.withJournalWrite(async () => {
          for (const operation of batch) {
            try {
              await this.yieldToBrowser()
              const records = operation.recordChanges?.upserts ?? this.records(operation.payload?.[this.recordField])
              if (defer && this.policy.deferRecords?.(operation.entityType, records)) {
                await this.applyInWrite({ ...operation, recordChanges: { upserts: [], deletedKeys: [] } })
                deferredRecords.push(operation)
                continue
              }
              await this.applyInWrite(operation)
              if (this.store.markAppliedMany) acknowledged.push(operation)
              else await this.store.markApplied(operation)
              await this.store.setSetting(`remote:${this.key(operation.entityType, operation.entityId)}`, '1')
              applied++
            } catch (error) {
              failure = { error }
              break
            }
          }
          if (acknowledged.length) await this.store.markAppliedMany!(acknowledged)
        }, batch)
        if (failure) throw failure.error
        this.report(`Applying changes: ${applied}/${operations.length}…`)
      }
    }
    await applyBatches(upserts, true)
    await applyBatches([...deferredRecords, ...deletes], false)
    return applied
  }

  /** Called with the host write lock and journal batch already held. */
  private async applyInWrite(operation: SyncOperation): Promise<void> {
    const current = await this.currentPayload(operation.entityType, operation.entityId)
    const previous = await this.store.snapshot(operation.entityType, operation.entityId)
    // Journal an edit or deletion made since the initial scan before merging.
    if (current) {
      await this.captureSnapshot(this.snapshot(operation.entityType, operation.entityId, current), previous)
    } else if (previous) {
      await this.store.enqueue(operation.entityType, operation.entityId, 'delete', null)
      await this.store.deleteSnapshot(operation.entityType, operation.entityId)
    }
    await this.applyUnlocked(operation, current)
  }

  private async applyUnlocked(operation: SyncOperation, current: SyncPayload | null): Promise<void> {
    if (operation.action === 'delete') {
      const clocks = await this.store.entityClocks(operation.entityType, operation.entityId)
      const stamp = operation.fieldStamps?.__deleted__ ?? `${operation.createdAt}|${operation.id}`
      if (Object.values(clocks).some(clock => clock >= stamp)) return
      await this.deleteEntity(operation.entityType, operation.entityId)
      await this.store.deleteSnapshot(operation.entityType, operation.entityId)
      await this.store.setFieldClock(operation, '__deleted__')
      return
    }
    if (!operation.payload || !await this.store.fieldIsNewer(operation, '__deleted__')) return
    const merged: SyncPayload = current ? { ...current } : {}
    const clocksToSave: string[] = []
    // A delta can arrive before the entity's original creation operation. Its
    // payload is a full snapshot: seed missing fields rather than saving defaults.
    // Seeded fields keep baseline clocks so real field edits still win later.
    const changedFields = operation.changedFields ?? Object.keys(operation.payload)
    const fields = current ? changedFields : Object.keys(operation.payload)
    const fieldOperation = current
      ? operation
      : {
          ...operation,
          fieldStamps: {
            ...Object.fromEntries(fields.filter(field => !changedFields.includes(field))
              .map(field => [field, '1970-01-01T00:00:00.000Z|baseline'])),
            ...operation.fieldStamps
          }
        }
    for (const field of fields) {
      if (field === this.recordField || field === 'id') continue
      const custom = this.policy.mergeField?.(operation.entityType, field, current?.[field], operation.payload[field])
      if (custom) {
        merged[field] = custom.value
        clocksToSave.push(field)
        continue
      }
      if (await this.store.fieldIsNewer(fieldOperation, field)) {
        merged[field] = operation.payload[field] ?? null
        clocksToSave.push(field)
      }
    }
    const records = new Map(this.records(merged[this.recordField]).map(record => [this.recordKey(record), record]))
    // Legacy operations have no deletion list; retain their additions while
    // respecting per-record clocks and tombstones produced by the new format.
    const changes = operation.recordChanges ?? { upserts: this.records(operation.payload[this.recordField]), deletedKeys: [] }
    for (const key of changes.deletedKeys) {
      await this.yieldToBrowser()
      if (!await this.store.fieldIsNewer(operation, `record:${key}`)) continue
      records.delete(key)
      clocksToSave.push(`record:${key}`)
    }
    for (const record of changes.upserts) {
      await this.yieldToBrowser()
      const key = this.recordKey(record)
      if (!await this.store.fieldIsNewer(operation, `record:${key}`)) continue
      records.set(key, this.records([record])[0]!)
      clocksToSave.push(`record:${key}`)
    }
    if (this.recordField in merged || changes.upserts.length || changes.deletedKeys.length) {
      merged[this.recordField] = [...records.values()]
    }
    this.policy.afterMerge?.(operation, current, merged, clocksToSave)
    if (!clocksToSave.length && current) return
    // Clocks are acknowledged only after the complete entity write and snapshot
    // succeed. A failed or interrupted save can safely replay the operation.
    await this.saveEntity(operation.entityType, operation.entityId, merged)
    const saved = await this.currentPayload(operation.entityType, operation.entityId)
    if (saved) await this.store.setSnapshot(operation.entityType, operation.entityId, saved)
    if (this.store.setFieldClocks) {
      await this.store.setFieldClocks(fieldOperation, clocksToSave)
    } else {
      for (const field of clocksToSave) {
        await this.store.setFieldClock(fieldOperation, field)
        await this.yieldToBrowser()
      }
    }
    await this.store.setFieldClock(operation, '__entity__')
  }

  private async collectSnapshots(): Promise<EntitySnapshot[]> {
    return (await Promise.all(this.registry.adapters.map(async adapter =>
      (await adapter.all()).map(entity => this.snapshot(adapter.type, entity.entityId, entity.payload))
    ))).flat()
  }

  private async currentPayload(entityType: SyncEntityType, entityId: string): Promise<SyncPayload | null> {
    return this.registry.get(entityType).read(entityId)
  }

  private async saveEntity(type: SyncEntityType, id: string, payload: SyncPayload): Promise<void> {
    await this.writeEntity(() => this.registry.get(type).save(id, payload))
  }

  private async deleteEntity(type: SyncEntityType, id: string): Promise<void> {
    await this.writeEntity(() => this.registry.get(type).delete(id))
  }

  private recordKey(record: SyncRecord): string {
    return this.policy.recordKey(record)
  }

  private recordDiff(previous: unknown, current: unknown) {
    const before = new Map(this.records(previous).map(record => [this.recordKey(record), record]))
    const after = new Map(this.records(current).map(record => [this.recordKey(record), record]))
    return {
      upserts: [...after.entries()].filter(([key, record]) => this.stableStringify(before.get(key)) !== this.stableStringify(record)).map(([, record]) => record),
      deletedKeys: [...before.keys()].filter(key => !after.has(key))
    }
  }

  private records(value: unknown): SyncRecord[] {
    return this.policy.records(value)
  }

  private snapshot(entityType: SyncEntityType, entityId: string, value: object): EntitySnapshot {
    return { entityType, entityId, payload: this.normalizePayload(value) }
  }

  private normalizePayload(value: object): SyncPayload {
    const payload = JSON.parse(JSON.stringify(value, (_key, item) => item === undefined ? null : item)) as SyncPayload
    return this.policy.normalize?.(payload) ?? payload
  }

  private isValidOperation(value: unknown): value is SyncOperation {
    if (!value || typeof value !== 'object') return false
    const operation = value as SyncOperation
    if ((operation.schemaVersion !== 1 && operation.schemaVersion !== 2)
      || typeof operation.id !== 'string' || !operation.id
      || typeof operation.deviceId !== 'string' || !operation.deviceId
      || typeof operation.entityId !== 'string' || !operation.entityId
      || !Number.isSafeInteger(operation.sequence) || operation.sequence < 1
      || !this.registry.adapters.some(adapter => adapter.type === operation.entityType)
      || (operation.action !== 'upsert' && operation.action !== 'delete')
      || typeof operation.createdAt !== 'string'
      || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(operation.createdAt)
      || !Number.isFinite(Date.parse(operation.createdAt))) return false
    if (operation.action === 'upsert' && (!operation.payload || typeof operation.payload !== 'object' || Array.isArray(operation.payload))) return false
    if (operation.action === 'delete' && operation.payload !== null) return false
    if (operation.changedFields !== undefined && (!Array.isArray(operation.changedFields)
      || operation.changedFields.some(field => typeof field !== 'string' || field.startsWith('__') || field === this.recordField)
      || operation.changedFields.some(field => !Object.hasOwn(operation.payload ?? {}, field)))) return false
    if (operation.recordChanges !== undefined) {
      const changes = operation.recordChanges
      if (!changes || !Array.isArray(changes.upserts) || !Array.isArray(changes.deletedKeys)
        || changes.deletedKeys.some(key => typeof key !== 'string')
        || changes.upserts.some(record => !this.policy.isValidRecord(record))) return false
    }
    if (operation.fieldStamps !== undefined && (!operation.fieldStamps || typeof operation.fieldStamps !== 'object'
      || Array.isArray(operation.fieldStamps) || Object.values(operation.fieldStamps).some(stamp => typeof stamp !== 'string'))) return false
    if (operation.schemaVersion === 2 && operation.action === 'upsert'
      && (!operation.changedFields || !operation.recordChanges)) return false
    return true
  }

  private operationOrder(a: SyncOperation, b: SyncOperation, reverseTypes: boolean): number {
    const aType = this.registry.adapters.findIndex(adapter => adapter.type === a.entityType)
    const bType = this.registry.adapters.findIndex(adapter => adapter.type === b.entityType)
    const typeOrder = reverseTypes ? bType - aType : aType - bType
    return typeOrder || a.createdAt.localeCompare(b.createdAt) || (a.deviceId === b.deviceId ? a.sequence - b.sequence : 0) || a.id.localeCompare(b.id)
  }

  private key(entityType: SyncEntityType, entityId: string): string {
    return `${entityType}:${entityId}`
  }

  private stableStringify(value: unknown): string {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return JSON.stringify(value)
    const record = value as Record<string, unknown>
    return JSON.stringify(Object.fromEntries(Object.keys(record).sort().map(key => [key, record[key]])))
  }
}
