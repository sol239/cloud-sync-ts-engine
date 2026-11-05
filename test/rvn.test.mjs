import assert from 'node:assert/strict'
import test from 'node:test'
import { JsonSyncStore, SyncEngine, SyncBackendFactory } from '../dist/index.js'

test('RVN provider paginates immutable envelopes, reads, checkpoints and errors', async () => {
  const entries = new Map()
  let requests = 0
  const backend = SyncBackendFactory.create({ provider: 'rvn', url: 'http://localhost:8765', space: 'workspace/main' }, {
    getAccessToken: async () => '',
    fetch: async (input, init) => {
      requests++
      const url = new URL(input)
      assert.match(url.pathname, /^\/api\/sync\/spaces\/d29ya3NwYWNlL21haW4/)
      assert.equal(init.credentials, 'include')
      if (init.method === 'POST') {
        const entry = JSON.parse(init.body)
        entries.set(entry.id, entry.body)
        return Response.json({ ok: true })
      }
      if (init.method === 'DELETE') { entries.clear(); return Response.json({ ok: true }) }
      const id = url.pathname.split('/')[5]
      if (id) return entries.has(id) ? Response.json(entries.get(id)) : new Response('', { status: 404 })
      const after = Number(url.searchParams.get('after'))
      const page = [...entries].slice(after, after + 1)
      return Response.json({ entries: page.map(([id, body]) => ({ id, revision: id, kind: body.format ? 'checkpoint' : 'operation' })), after: after + page.length })
    },
  })
  const op = { schemaVersion: 1, id: 'one', deviceId: 'device', sequence: 1, entityType: 'note', entityId: 'n', action: 'upsert', payload: { title: 'Note' }, createdAt: new Date().toISOString() }
  await backend.append(op)
  const checkpoint = await backend.publishCheckpoint({ format: 'full-sync-v1', createdAt: op.createdAt, operations: [op], coveredOperationIds: ['one'] })
  assert.equal((await backend.list()).length, 2)
  assert.deepEqual(await backend.read({ id: 'one' }), op)
  assert.equal((await backend.read(checkpoint)).format, 'full-sync-v1')
  await assert.rejects(backend.read({ id: 'missing' }), error => error.status === 404)
  await backend.deleteAll()
  assert.equal((await backend.list()).length, 0)
  assert.ok(requests >= 9)
})

test('two connected destinations forward imported edits and deletions with original clocks', async () => {
  const remote = () => {
    const entries = new Map()
    return {
      provider: 'test', entries, requiresHistoryBeforeCheckpoint: true,
      async list() { return [...entries].map(([id, body]) => ({ id, revision: id, kind: body.format ? 'checkpoint' : 'operation', operationId: body.format ? undefined : id })) },
      async read(entry) { return structuredClone(entries.get(entry.id)) },
      async append(op) { entries.set(op.id, structuredClone(op)) },
      async publishCheckpoint(body) { for (const id of body.coveredOperationIds) assert.ok(entries.has(id), `Missing immutable history: ${id}`); const id = crypto.randomUUID(); entries.set(id, structuredClone(body)); return { id, revision: id, kind: 'checkpoint' } },
      async deleteAll() { entries.clear() },
    }
  }
  const a = remote(), b = remote()
  const disk = new Map(), items = new Map()
  const run = async (connectionId, backend, full = false) => {
    const store = new JsonSyncStore({ key: 'shared', connectionId, storage: { getItem: key => disk.get(key) ?? null, setItem: (key, value) => { disk.set(key, value) } } })
    const engine = new SyncEngine({ backend, store, bootstrapConnection: true,
      entities: [{ type: 'note', all: async () => [...items].map(([entityId, payload]) => ({ entityId, payload })), read: async id => items.get(id) ?? null,
        save: async (id, payload) => { items.set(id, payload) }, delete: async id => { items.delete(id) } }],
      withWrite: action => action(), writeEntity: action => action(),
    })
    return engine.sync({ full })
  }
  await run('a', a); await run('b', b)
  const op = { schemaVersion: 1, id: 'external-create', deviceId: 'external', sequence: 1, entityType: 'note', entityId: 'n', action: 'upsert', payload: { title: 'Remote' }, createdAt: new Date().toISOString() }
  await a.append(op)
  await run('a', a)
  assert.equal(items.get('n').title, 'Remote')
  await run('b', b)
  assert.deepEqual(b.entries.get(op.id), op)
  await run('a', a, true); await run('b', b, true)
  const deletion = { ...op, id: 'external-delete', sequence: 2, action: 'delete', payload: null, createdAt: new Date(Date.now() + 1000).toISOString() }
  await a.append(deletion)
  await run('a', a); await run('b', b)
  assert.equal(items.has('n'), false)
  assert.deepEqual(b.entries.get(deletion.id), deletion)
  const snapshotOperation = { ...op, id: 'snapshot-only-create', entityId: 'from-checkpoint',
    payload: { title: 'Checkpoint-only data' }, createdAt: new Date(Date.now() + 2000).toISOString() }
  const snapshotId = crypto.randomUUID()
  a.entries.set(snapshotId, { format: 'full-sync-v1', createdAt: new Date().toISOString(),
    operations: [snapshotOperation], coveredOperationIds: [] })
  await run('a', a)
  assert.equal(a.entries.has(snapshotOperation.id), false, 'Snapshot contents must not be uploaded back as new history')
  await run('b', b)
  assert.deepEqual(b.entries.get(snapshotOperation.id), snapshotOperation, 'Checkpoint-only data must forward with original clocks')
})
