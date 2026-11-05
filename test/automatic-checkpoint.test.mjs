import assert from 'node:assert/strict'
import test from 'node:test'
import { JsonSyncStore, SyncEngine, registerEntities } from '../dist/index.js'

class Note {
  static entityType = 'note'
  constructor(id, title) { this.id = id; this.title = title }
  toJSON() { return { id: this.id, title: this.title } }
  static fromJSON(data) { return new Note(data.id, data.title) }
}
function cloud() {
  const files = new Map()
  const downloads = []
  return {
    files, downloads, provider: 'test',
    async list() { return [...files].map(([id, body], index) => ({
      id, revision: id, modifiedAt: String(index).padStart(12, '0'),
      kind: body.format ? 'checkpoint' : 'operation', operationId: body.format ? undefined : id
    })) },
    async read(entry) { downloads.push(entry.id); return structuredClone(files.get(entry.id)) },
    async append(operation) { files.set(operation.id, structuredClone(operation)) },
    async publishCheckpoint(snapshot) {
      const id = crypto.randomUUID()
      files.set(id, structuredClone(snapshot))
      return { id, revision: id, kind: 'checkpoint' }
    },
    async deleteAll() { files.clear() }
  }
}
function client(backend, disk = new Map(), notes = new Map(), checkpointOperationThreshold, automaticCheckpoints) {
  const store = new JsonSyncStore({ key: 'sync', storage: {
    getItem: key => disk.get(key) ?? null, setItem: (key, value) => disk.set(key, value)
  } })
  const engine = new SyncEngine({ backend, store, checkpointOperationThreshold, automaticCheckpoints, withWrite: run => run(), writeEntity: run => run(),
    entities: registerEntities([Note], {
      async list() { return [...notes.values()] },
      async read(_type, id) { return notes.get(id) ?? null },
      async save(_type, note) { notes.set(note.id, note) },
      async delete(_type, id) { notes.delete(id) }
    })
  })
  return { engine, store, disk, notes }
}
const checkpoints = backend => [...backend.files].filter(([, body]) => body.format)

test('500 uncovered operations trigger a checkpoint and a fresh device skips covered history', async () => {
  const backend = cloud()
  const a = client(backend, new Map(), new Map(), 500)
  for (let index = 0; index < 499; index++) a.notes.set(String(index), new Note(String(index), `Note ${index}`))
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 0)
  assert.equal(backend.files.size, 499)
  a.notes.set('500', new Note('500', 'Threshold reached'))
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 1)
  const [checkpointId, snapshot] = checkpoints(backend)[0]
  assert.equal(snapshot.coveredOperationIds.length, 500)
  backend.downloads.length = 0
  const b = client(backend)
  await b.engine.sync()
  assert.deepEqual(backend.downloads, [checkpointId])
  assert.equal(b.notes.size, 500)
  assert.equal(checkpoints(backend).length, 1)
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 1)
})

test('daily checkpoints require changes, survive restart, and use the shared checkpoint age', async (t) => {
  const start = Date.parse('2026-10-01T08:00:00Z')
  t.mock.timers.enable({ apis: ['Date'], now: start })
  const backend = cloud()
  let a = client(backend)
  a.notes.set('one', new Note('one', 'Initial'))
  await a.engine.sync()
  t.mock.timers.setTime(start + 24 * 60 * 60 * 1000 - 1)
  a.notes.get('one').title = 'Changed before the daily threshold'
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 0)
  a = client(backend, a.disk, a.notes)
  t.mock.timers.setTime(start + 24 * 60 * 60 * 1000)
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 1)
  const b = client(backend)
  await b.store.setSetting('auto-checkpoint-baseline-at', '2020-01-01T00:00:00Z')
  b.notes.set('two', new Note('two', 'New device edit'))
  await b.engine.sync()
  assert.equal(checkpoints(backend).length, 1, 'A recent shared checkpoint resets the daily interval for other devices')
  await a.engine.sync({ full: true })
  t.mock.timers.setTime(start + 48 * 60 * 60 * 1000)
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 2, 'No checkpoint when everything is already covered')
  a.notes.get('one').title = 'Next day edit'
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 3)
})

test('failed automatic checkpoint publication leaves changes pending and retries on the next sync', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-03T08:00:00Z') })
  const backend = cloud()
  const a = client(backend)
  await a.store.setSetting('auto-checkpoint-baseline-at', '2026-10-01T08:00:00Z')
  a.notes.set('one', new Note('one', 'Keep this change'))
  const publish = backend.publishCheckpoint
  backend.publishCheckpoint = async () => { throw new Error('Offline during checkpoint upload') }
  await assert.rejects(() => a.engine.sync(), /Offline during checkpoint upload/)
  assert.equal(checkpoints(backend).length, 0)
  assert.equal((await a.store.pendingOperations()).length, 0, 'Normal sync finishes before checkpoint upload')
  backend.publishCheckpoint = publish
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 1)
  assert.equal((await a.store.pendingOperations()).length, 0)
})

test('history-requiring backends upload operations before automatic checkpoint publication', async (t) => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-03T08:00:00Z') })
  const backend = cloud()
  backend.requiresHistoryBeforeCheckpoint = true
  const publish = backend.publishCheckpoint
  backend.publishCheckpoint = async (snapshot) => {
    assert.equal(snapshot.coveredOperationIds.length, 1)
    for (const id of snapshot.coveredOperationIds) assert.ok(backend.files.has(id))
    return publish(snapshot)
  }
  const a = client(backend)
  await a.store.setSetting('auto-checkpoint-baseline-at', '2026-10-01T08:00:00Z')
  a.notes.set('one', new Note('one', 'Persist history first'))
  const result = await a.engine.sync()
  assert.equal(result.pushed, 2)
  assert.equal(checkpoints(backend).length, 1)
})

test('bounded parallel uploads acknowledge successes and retry failed and unattempted changes', async () => {
  const backend = cloud()
  backend.uploadConcurrency = 4
  const append = backend.append
  let active = 0
  let maximum = 0
  let fail = true
  backend.append = async (operation) => {
    active++
    maximum = Math.max(maximum, active)
    try {
      await new Promise(resolve => setTimeout(resolve, 0))
      if (operation.entityId === '1' && fail) throw new Error('Upload interrupted')
      await append(operation)
    } finally { active-- }
  }
  const a = client(backend)
  for (let index = 0; index < 9; index++) a.notes.set(String(index), new Note(String(index), 'Today plan'))
  await assert.rejects(() => a.engine.sync(), /Upload interrupted/)
  assert.equal(maximum, 4)
  assert.equal(backend.files.size, 3)
  assert.equal((await a.store.pendingOperations()).length, 6)
  fail = false
  await a.engine.sync()
  assert.equal(backend.files.size, 9)
  assert.equal((await a.store.pendingOperations()).length, 0)
  assert.equal(maximum, 4)
})

 test('custom operation threshold publishes at the configured count', async () => {
  const backend = cloud()
  const a = client(backend, new Map(), new Map(), 2)
  a.notes.set('one', new Note('one', 'First'))
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 0)
  a.notes.set('two', new Note('two', 'Second'))
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 1)
  backend.downloads.length = 0
  const b = client(backend)
  await b.engine.sync()
  assert.deepEqual(backend.downloads, [checkpoints(backend)[0][0]])
  assert.equal(b.notes.size, 2)
  assert.throws(() => client(backend, new Map(), new Map(), 0), /positive whole number/)
})

test('automatic checkpoints can be disabled and enabled, with sync completed first', async () => {
  const backend = cloud()
  let a = client(backend, new Map(), new Map(), 1, false)
  a.notes.set('one', new Note('one', 'Local data'))
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 0)
  assert.equal(backend.files.size, 1)
  a = client(backend, a.disk, a.notes, 1, true)
  const publish = backend.publishCheckpoint
  backend.publishCheckpoint = async snapshot => {
    assert.equal((await a.store.pendingOperations()).length, 0)
    for (const id of snapshot.coveredOperationIds) assert.ok(backend.files.has(id))
    return publish(snapshot)
  }
  await a.engine.sync()
  assert.equal(checkpoints(backend).length, 1)
})

test('default checkpoints cover 400 operations and a fresh device downloads only the newest snapshot and five later operations', async () => {
  const backend = cloud()
  const source = client(backend)
  for (let batch = 0; batch < 4; batch++) {
    for (let index = batch * 100; index < (batch + 1) * 100; index++) {
      source.notes.set(String(index), new Note(String(index), `Note ${index}`))
    }
    await source.engine.sync()
    assert.equal(checkpoints(backend).length, batch + 1)
  }
  const latest = checkpoints(backend).at(-1)[0]
  for (let index = 400; index < 405; index++) source.notes.set(String(index), new Note(String(index), `Note ${index}`))
  await source.engine.sync()
  assert.equal(checkpoints(backend).length, 4)
  backend.downloads.length = 0
  const target = client(backend)
  await target.engine.sync()
  assert.equal(target.notes.size, 405)
  assert.equal(backend.downloads.length, 6)
  assert.equal(backend.downloads[0], latest)
  for (const id of backend.downloads.slice(1)) assert.equal(backend.files.get(id).format, undefined)
})
