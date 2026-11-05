import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import test from 'node:test'
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { JsonSyncStore, SyncEngine, registerEntities } from '../dist/index.js'

function storage() {
  const data = new Map()
  return { data, getItem: key => data.get(key) ?? null, setItem: (key, value) => {
    data.set(key, value)
  } }
}
const make = (storage, connectionId = 'one', extra = {}) => new JsonSyncStore({ storage, key: 'account', connectionId, ...extra })
test('example runs using the built package and ordinary entity registration', () => {
  const output = execFileSync(process.execPath, [new URL('../examples/notes.mjs', import.meta.url).pathname], { encoding: 'utf8' })
  assert.match(output, /Hello from another app/)
})
test('journal, device identity, snapshots, clocks and receipts survive restart', async () => {
  const disk = storage()
  const first = make(disk)
  const op = await first.enqueue('note', 'one', 'upsert', { title: 'Offline' })
  await first.setSnapshot('note', 'one', { title: 'Offline' })
  await first.markUploaded(op.id)
  await first.setSetting('bootstrap', 'done')
  await first.setLastSyncedAt(op.createdAt)
  const restarted = make(disk)
  assert.equal(await restarted.getDeviceId(), op.deviceId)
  assert.deepEqual(await restarted.snapshot('note', 'one'), { title: 'Offline' })
  assert.equal((await restarted.pendingOperations()).length, 0)
  assert.equal((await restarted.appliedOperationIds()).has(op.id), true)
  assert.equal(await restarted.setting('bootstrap'), 'done')
  assert.equal((await restarted.status()).lastSyncedAt, op.createdAt)
  assert.equal(await restarted.fieldIsNewer(op, 'title'), false)
  assert.equal((await restarted.enqueue('note', 'one', 'delete', null)).sequence, 2)
  assert.equal((await restarted.deletionClocks()).length, 1)
})
test('new connections share journal and tombstones while receipts reset independently', async () => {
  const disk = storage()
  const first = make(disk)
  const op = await first.enqueue('note', 'n', 'delete', null)
  await first.markUploaded(op.id)
  const second = make(disk, 'two')
  assert.equal((await second.pendingOperations()).length, 1)
  assert.equal((await second.deletionClocks()).length, 1)
  await second.markUploaded(op.id)
  await second.resetConnection()
  assert.equal((await second.pendingOperations()).length, 1)
  assert.equal((await make(disk).pendingOperations()).length, 0)
})
test('failed persistence does not acknowledge an operation and can retry', async () => {
  const disk = storage()
  const store = make(disk)
  await store.initialize()
  const set = disk.setItem
  disk.setItem = () => {
    throw new Error('Disk full')
  }
  await assert.rejects(store.enqueue('note', 'n', 'upsert', { title: 'One' }), /Disk full/)
  assert.equal((await store.localOperations()).length, 0)
  disk.setItem = set
  const op = await store.enqueue('note', 'n', 'upsert', { title: 'One' })
  assert.equal(op.sequence, 1)
  assert.equal((await make(disk).localOperations()).length, 1)
})
test('concurrent journal mutations serialize without losing updates; returned values are detached', async () => {
  const disk = storage()
  const store = make(disk)
  const operations = await Promise.all(Array.from({ length: 20 }, (_, n) => store.enqueue('note', `${n}`, 'upsert', { title: `${n}` })))
  assert.deepEqual(operations.map(op => op.sequence), Array.from({ length: 20 }, (_, n) => n + 1))
  operations[0].payload.title = 'Mutated'
  assert.equal((await store.localOperations())[0].payload.title, '0')
  assert.equal((await make(disk).localOperations()).length, 20)
})
test('keyed-record clocks use the supplied policy key and reject missing configuration', async () => {
  const disk = storage()
  const metadata = { changedFields: [], recordChanges: { upserts: [{ key: 'r1', value: 1 }], deletedKeys: [] } }
  await assert.rejects(make(disk).enqueue('note', 'n', 'upsert', {}, metadata), /requires recordKey/)
  const store = make(disk, 'one', { recordKey: record => record.key })
  const op = await store.enqueue('note', 'n', 'upsert', {}, metadata)
  assert.equal(await store.fieldIsNewer(op, 'record:r1'), false)
  assert.equal((await make(disk).entityClocks('note', 'n'))['record:r1'], `${op.createdAt}|${op.id}`)
})
test('fresh app syncs its own classes, checkpoints, deletions and later edits', async () => {
  class Item {
    static entityType = 'item'
    constructor(id, title) {
      this.id = id
      this.title = title
    }

    toJSON() { return { id: this.id, title: this.title } }
    static fromJSON(data) {
      if (typeof data.id !== 'string' || typeof data.title !== 'string')
        throw new Error('Invalid item')
      return new Item(data.id, data.title)
    }
  }
  const entries = new Map()
  const downloads = []
  const backend = {
    provider: 'test',
    async list() { return [...entries].map(([id, body], index) => ({ id, revision: id, modifiedAt: id === 'independent-branch' ? '' : String(index).padStart(12, '0'), kind: body.format ? 'checkpoint' : 'operation', operationId: body.format ? undefined : id })) },
    async read(entry) { downloads.push(entry.id); return structuredClone(entries.get(entry.id)) },
    async append(op) { entries.set(op.id, structuredClone(op)) },
    async publishCheckpoint(body) {
      const id = crypto.randomUUID()
      entries.set(id, structuredClone(body))
      return { id, revision: id, kind: 'checkpoint' }
    },
    async deleteAll() { entries.clear() }
  }
  const client = () => {
    const items = new Map()
    const store = make(storage())
    const engine = new SyncEngine({ backend, store, withWrite: action => action(), writeEntity: action => action(),
      entities: registerEntities([Item], {
        async list() { return [...items.values()] },
        async read(_type, id) { return items.get(id) ?? null },
        async save(_type, entity) { items.set(entity.id, entity) },
        async delete(_type, id) { items.delete(id) }
      }) })
    return { items, engine }
  }
  const a = client()
  a.items.set('n', new Item('n', 'Offline'))
  await a.engine.sync()
  const b = client()
  await b.engine.sync()
  assert.equal(b.items.get('n').title, 'Offline')
  b.items.get('n').title = 'Edited'
  await b.engine.sync()
  await a.engine.sync({ full: true })
  const c = client()
  // A slow synchronous storage task must not starve a pending browser event.
  const previousWindow = globalThis.window
  const read = backend.read
  let browserEventHandled = false
  globalThis.window = {}
  const browserEvent = setTimeout(() => { browserEventHandled = true }, 0)
  backend.read = async (entry) => {
    const deadline = Date.now() + 12
    while (Date.now() < deadline) { /* simulate synchronous storage work */ }
    return read(entry)
  }
  try {
    await c.engine.sync()
    assert.equal(browserEventHandled, true, 'Sync must yield to browser events before finishing')
  } finally {
    clearTimeout(browserEvent)
    backend.read = read
    if (previousWindow === undefined) delete globalThis.window
    else globalThis.window = previousWindow
  }
  assert.equal(c.items.get('n').title, 'Edited')
  await a.engine.sync({ full: true })
  const newestCheckpoint = [...entries].filter(([, body]) => body.format).at(-1)[0]
  assert.ok(entries.get(newestCheckpoint).coveredCheckpoints.length > 0)
  b.items.set('later', new Item('later', 'After checkpoint'))
  await b.engine.sync()
  const newerOperations = [...entries].filter(([, body]) => !body.format && body.entityId === 'later').map(([id]) => id)
  // An independent checkpoint branch must survive even if it is older.
  const branch = structuredClone(entries.get(newestCheckpoint))
  branch.coveredCheckpoints = []
  branch.coveredOperationIds = []
  branch.operations = [structuredClone(branch.operations[0])]
  Object.assign(branch.operations[0], { id: crypto.randomUUID(), entityId: 'branch', payload: { id: 'branch', title: 'Concurrent branch' } })
  entries.set('independent-branch', branch)
  downloads.length = 0
  const d = client()
  await d.engine.sync()
  assert.deepEqual(downloads, [newestCheckpoint, 'independent-branch', ...newerOperations])
  assert.equal(d.items.get('n').title, 'Edited')
  assert.equal(d.items.get('later').title, 'After checkpoint')
  assert.equal(d.items.get('branch').title, 'Concurrent branch')
  downloads.length = 0
  await d.engine.sync()
  assert.deepEqual(downloads, [])
  b.items.delete('n')
  await b.engine.sync()
  await c.engine.sync()
  assert.equal(c.items.has('n'), false)
  assert.equal((await c.engine.sync()).pushed, 0)
})
// Resolve declarations as a separate NodeNext app, without the host's tsconfig.
test('compiled exports typecheck in an independent TypeScript consumer', () => {
  const directory = mkdtempSync(join(tmpdir(), 'sync-types-'))
  try {
    const entry = fileURLToPath(new URL('../dist/index.js', import.meta.url))
    writeFileSync(join(directory, 'consumer.mts'), `
      import { JsonSyncStore, registerEntities } from ${JSON.stringify(entry)}
      import type { ISyncStore, IEntityPersistence } from ${JSON.stringify(entry)}
      const store: ISyncStore = new JsonSyncStore({ key: 'account', storage: {
        getItem: () => null, setItem: () => {}
      } })
      class Note {
        static entityType = 'note'
        constructor(readonly id: string) {}
        toJSON() { return { id: this.id } }
        static fromJSON(data: Record<string, unknown>) { return new Note(String(data.id)) }
      }
      declare const persistence: IEntityPersistence
      registerEntities([Note], persistence)
      void store
    `)
    writeFileSync(join(directory, 'tsconfig.json'), JSON.stringify({ compilerOptions: {
      target: 'ES2022', module: 'NodeNext', moduleResolution: 'NodeNext', strict: true, noEmit: true, types: []
    }, files: ['consumer.mts'] }))
    execFileSync(process.execPath, [fileURLToPath(import.meta.resolve('typescript/bin/tsc')), '-p', join(directory, 'tsconfig.json')], { stdio: 'pipe' })
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
})

test('large local comparison persists the journal a constant number of times', async () => {
  const entries = new Map()
  const backend = { provider: 'test', async list() { return [] }, async read() { throw new Error('Unexpected read') },
    async append(operation) { entries.set(operation.id, operation) },
    async publishCheckpoint(snapshot) { const id = crypto.randomUUID(); return { id, revision: id, kind: 'checkpoint' } }, async deleteAll() {} }
  let writes = 0
  const disk = storage()
  const store = make({ getItem: disk.getItem, setItem(key, value) { writes++; disk.setItem(key, value) } })
  await store.initialize()
  await store.setSetting('bootstrap', 'done')
  const entities = Array.from({ length: 12291 }, (_, i) => ({ entityId: String(i), payload: { id: String(i), title: `Block ${i}` } }))
  const engine = new SyncEngine({ backend, store, bootstrapConnection: true,
    entities: [{ type: 'block', all: async () => entities, read: async id => entities[Number(id)]?.payload ?? null,
      save: async () => {}, delete: async () => {} }],
    withWrite: action => action(), writeEntity: action => action() })
  writes = 0
  const started = Date.now()
  await engine.sync()
  assert.equal((await store.localOperations()).length, 12291)
  assert.equal((await store.snapshots()).length, 12291)
  assert.ok(writes < 15, `Expected batched journal saves, got ${writes}`)
  console.log(`Compared 12291 entities in ${Date.now() - started}ms with ${writes} journal saves`)
})

test('failed journal batch persistence rolls back mutations and can retry', async () => {
  const disk = storage()
  let fail = false
  const store = make({ getItem: disk.getItem, setItem(key, value) { if (fail) throw new Error('Disk full'); disk.setItem(key, value) } })
  await store.initialize()
  fail = true
  await assert.rejects(store.batch(async () => {
    await store.enqueue('note', 'one', 'upsert', { title: 'Keep me' })
    await store.setSnapshot('note', 'one', { title: 'Keep me' })
  }), /Disk full/)
  assert.equal((await store.localOperations()).length, 0)
  assert.equal(await store.snapshot('note', 'one'), null)
  fail = false
  await store.batch(async () => { await store.enqueue('note', 'one', 'upsert', { title: 'Keep me' }) })
  assert.equal((await store.localOperations()).length, 1)
})

test('remote apply batches journal saves and preserves successful work on retry', async () => {
  let writes = 0
  const disk = storage()
  const store = make({ getItem: disk.getItem, setItem(key, value) { writes++; disk.setItem(key, value) } })
  await store.initialize()
  const rows = new Map()
  let failAt = '125'
  const engine = new SyncEngine({ store,
    backend: { provider: 'test', async list() { return [] }, async read() {}, async append() {}, async deleteAll() {} },
    entities: [{ type: 'note', all: async () => [...rows].map(([entityId, payload]) => ({ entityId, payload })),
      read: async id => rows.get(id) ?? null,
      save: async (id, payload) => { if (id === failAt) throw new Error('Host write failed'); rows.set(id, payload) },
      delete: async id => { rows.delete(id) } }],
    withWrite: action => action(), writeEntity: action => action() })
  const operations = Array.from({ length: 1200 }, (_, i) => ({ id: `remote-${i}`, deviceId: 'remote', sequence: i + 1,
    createdAt: '2026-10-25T00:00:00.000Z', entityType: 'note', entityId: String(i), action: 'upsert',
    payload: { id: String(i), title: `Note ${i}` } }))
  writes = 0
  await assert.rejects(engine.applyOperations(operations), /Host write failed/)
  assert.equal(rows.size, 125)
  const restarted = make(disk)
  assert.equal((await restarted.appliedOperationIds()).size, 125)
  assert.equal((await restarted.snapshots()).length, 125)
  assert.equal((await restarted.localOperations()).filter(op => op.deviceId !== 'remote').length, 0)
  failAt = undefined
  const started = Date.now()
  await engine.applyOperations(operations)
  assert.equal(rows.size, 1200)
  assert.equal((await store.appliedOperationIds()).size, 1200)
  assert.equal((await store.localOperations()).filter(op => op.deviceId !== 'remote').length, 0)
  assert.ok(writes <= 14, `Expected batch saves, got ${writes}`)
  console.log(`Applied 1200 records in ${Date.now() - started}ms with ${writes} journal saves including failure/retry`)
})

test('downloads four change files in parallel and reports counts, contents and skipped history', async () => {
  const store = make(storage())
  await store.initialize()
  const operations = Array.from({ length: 8 }, (_, i) => ({ schemaVersion: 2, id: `remote-${i}`, deviceId: 'remote', sequence: i + 1,
    createdAt: '2026-10-25T00:00:00.000Z', entityType: 'note', entityId: String(i), action: 'upsert', changedFields: ['title'], recordChanges: { upserts: [], deletedKeys: [] }, payload: { title: `Note ${i}` } }))
  await store.markApplied(operations[0])
  let active = 0, peak = 0
  const reads = [], messages = [], rows = new Map()
  const backend = { provider: 'test', async list() { return [] }, async append() {}, async deleteAll() {},
    async read(file) {
      if (file.kind === 'checkpoint') return { format: 'full-sync-v1', createdAt: '2026-10-25T00:00:00.000Z', operations: [], coveredOperationIds: ['old-a', 'old-b'] }
      reads.push(file.operationId); peak = Math.max(peak, ++active)
      await new Promise(resolve => setTimeout(resolve, 5))
      active--
      return operations.find(op => op.id === file.operationId)
    } }
  const engine = new SyncEngine({ store, backend, report: message => messages.push(message),
    entities: [{ type: 'note', all: async () => [], read: async id => rows.get(id) ?? null,
      save: async (id, payload) => { rows.set(id, payload) }, delete: async id => { rows.delete(id) } }],
    withWrite: action => action(), writeEntity: action => action() })
  const result = await engine.pull(operations.map(op => ({ id: op.id, operationId: op.id, kind: 'operation' })))
  assert.equal(peak, 4)
  assert.equal(reads.length, 7)
  assert.ok(!reads.includes('remote-0'))
  assert.equal(result.applied, 7)
  assert.ok(messages.some(message => message.includes('7/7 files complete; 0 active (up to 4 parallel); 1 skipped.')))
  assert.ok(messages.some(message => message.includes('Received upsert note')))
  await engine.pullSnapshot([{ id: 'snapshot', path: 'full-sync.json', kind: 'checkpoint', revision: 'one' }], false)
  assert.ok(messages.some(message => message.includes('Downloading checkpoint 1: full-sync.json')))
  assert.ok(messages.some(message => message.includes('covers 2 historical operations')))
})
