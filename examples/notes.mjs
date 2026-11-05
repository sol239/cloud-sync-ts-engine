import assert from 'node:assert/strict'
import { SyncEngine, JsonSyncStore, registerEntities } from '../dist/index.js'
// An ordinary app model with no dependency on the sync package.
class Note {
  static entityType = 'note'
  constructor(id, title) {
    this.id = id
    this.title = title
  }

  toJSON() { return { id: this.id, title: this.title } }
  static fromJSON(data) {
    if (typeof data.id !== 'string' || typeof data.title !== 'string')
      throw new Error('Invalid note')
    return new Note(data.id, data.title)
  }
}
// Fake remote for an offline, credential-free example. In an app use
// SyncBackendFactory.create({ provider: 'dropbox', directory: 'notes/sync' }, { getAccessToken }).
const remote = new Map()
const backend = {
  provider: 'example',
  async list() { return [...remote.keys()].map(id => ({ id, revision: id, kind: 'operation', operationId: id })) },
  async read(entry) { return structuredClone(remote.get(entry.id)) },
  async append(op) { remote.set(op.id, structuredClone(op)) },
  async deleteAll() { remote.clear() },
  async publishCheckpoint() { throw new Error('This minimal example uses ordinary sync.') }
}
function registerApp(name) {
  const notes = new Map()
  const syncState = new Map() // Browser: pass localStorage. Node: pass a durable file/DB implementation.
  const persistence = {
    async list() { return [...notes.values()] },
    async read(_type, id) { return notes.get(id) ?? null },
    async save(_type, note) { notes.set(note.id, note) },
    async delete(_type, id) { notes.delete(id) }
  }
  let writes = Promise.resolve()
  const withWrite = (action) => {
    const next = writes.then(action)
    writes = next.then(() => { }, () => { })
    return next
  }
  const sync = new SyncEngine({
    backend,
    entities: registerEntities([Note], persistence),
    store: new JsonSyncStore({ key: name, storage: {
      getItem: key => syncState.get(key) ?? null,
      setItem: (key, value) => { syncState.set(key, value) }
    } }),
    withWrite,
    // A Map assignment is atomic. Database apps supply their transaction wrapper.
    writeEntity: action => action()
  })
  return { notes, sync, withWrite }
}
const laptop = registerApp('laptop')
const phone = registerApp('phone')
await laptop.withWrite(async () => {
  laptop.notes.set('n1', new Note('n1', 'Hello from another app'))
})
await laptop.sync.sync()
await phone.sync.sync()
assert.equal(phone.notes.get('n1').title, 'Hello from another app')
console.log('Synced note:', phone.notes.get('n1').toJSON())
