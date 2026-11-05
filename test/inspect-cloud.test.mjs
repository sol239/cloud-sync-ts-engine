import assert from 'node:assert/strict'
import test from 'node:test'
import { inspectCloud } from '../dist/index.js'

const operation = (entityType, entityId, action = 'upsert') => ({ schemaVersion: 2,
  id: crypto.randomUUID(), entityType, entityId, action, createdAt: '2026-10-24T10:00:00.000Z',
  payload: action === 'upsert' ? { name: `${entityType} in cloud`, nested: { values: [1, 2] } } : null })

test('inspection indexes every remote entity type, checkpoint branch and tombstone while preserving raw entries', async () => {
  const activity = operation('activity', 'a')
  const bodies = [activity, operation('routine', 'r'), operation('activity', 'a', 'delete'),
    { format: 'full-sync-v1', operations: [activity, operation('diary', 'd'), operation('metadata', 'm'), operation('custom-type', 'c')], coveredOperationIds: [activity.id] },
    { schemaVersion: 999, other: 'future-format' }]
  let reads = 0
  const progress = []
  const backend = {
    async list() { return bodies.map((body, index) => ({ id: `${index}`, kind: body.format ? 'checkpoint' : 'operation', revision: 'r1' })) },
    async read(entry) { reads++; return bodies[Number(entry.id)] },
    append() { throw new Error('Unexpected write') }, publishCheckpoint() { throw new Error('Unexpected checkpoint') }, deleteAll() { throw new Error('Unexpected deletion') }
  }
  const data = await inspectCloud(backend, { progress: (done, total) => progress.push([done, total]) })
  assert.equal(reads, bodies.length)
  assert.equal(data.entries.length, bodies.length)
  assert.equal(data.entities.length, 5)
  assert.equal(data.entities.find(entity => entity.type === 'activity').versions.length, 3)
  assert.ok(data.entities.find(entity => entity.type === 'activity').versions.some(version => version.operation.action === 'delete'))
  assert.deepEqual(data.entries.at(-1).body, bodies.at(-1))
  assert.deepEqual(progress.at(-1), [bodies.length, bodies.length])
})

test('unreadable entries remain visible and do not hide other remote data', async () => {
  const data = await inspectCloud({
    async list() { return [{ id: 'bad' }, { id: 'good' }] },
    async read(entry) { if (entry.id === 'bad') throw new Error('Invalid JSON'); return operation('goal', 'g') }
  })
  assert.equal(data.entries[0].error, 'Invalid JSON')
  assert.equal(data.entities[0].type, 'goal')
})

test('cancellation stops inspection instead of returning partial results', async () => {
  const controller = new AbortController()
  await assert.rejects(inspectCloud({
    async list() { return [{ id: 'one' }] },
    async read() { controller.abort(); return operation('routine', 'r') }
  }, { signal: controller.signal }), /abort/i)
})
