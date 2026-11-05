# cloud-sync-ts-engine

A standalone sync engine with Dropbox, Google Drive, Supabase, and RVN server backends. **Copy this entire directory into another repository**: it includes the engine, providers, local sync bookkeeping, SQL migrations, build tools, tests, and a working example. It has no runtime dependencies or imports from its parent repository.

Your app keeps its own entities and database. Registration connects them to the engine; entities do not need to import this package.

## Copy and run

Node.js 22 or newer and npm are required for building and running the tests. The built package also runs in modern browsers with `fetch`, Web Crypto, and `structuredClone`.

```sh
# After copying this directory to your repository as vendor/sync:
cd vendor/sync
npm install
npm test
npm run example
```

`npm install` installs the package's own TypeScript development dependency and builds `dist/`. `npm test` rebuilds and runs the tests inside this directory. The example synchronizes a note between two independent app instances using a simulated remote; no account or credentials are required.

Install the copied directory from your app's root:

```sh
npm install ./vendor/sync
```

Then import from `cloud-sync-ts-engine`. JavaScript and TypeScript consumers use the same compiled ESM entry point; TypeScript declarations are included. Run `npm run build` inside the copied directory after changing its source. Bundler projects may also embed `src/` directly. No Nuxt, Vue, Tauri, or parent workspace setup is needed.

## Register your app

This browser example uses the built-in journal with `localStorage`. `noteRepository` and `database` below represent your app's existing persistence APIs; connect these four methods to your own repositories. For a runnable integration without placeholders, see [`examples/notes.mjs`](examples/notes.mjs).

```ts
import {
  JsonSyncStore,
  SyncEngine,
  SyncBackendFactory,
  registerEntities,
} from 'cloud-sync-ts-engine'
import type { IEntityPersistence } from 'cloud-sync-ts-engine'

// An ordinary app class. No sync import or `implements` is required.
class Note {
  static readonly entityType = 'note'
  constructor(readonly id: string, public title: string) {}
  toJSON() { return { id: this.id, title: this.title } }
  static fromJSON(data: Record<string, unknown>) {
    if (typeof data.id !== 'string' || typeof data.title !== 'string') {
      throw new Error('Invalid note')
    }
    return new Note(data.id, data.title)
  }
}

const persistence: IEntityPersistence = {
  list: type => noteRepository.list(),
  read: (type, id) => noteRepository.find(id),
  save: (type, note) => noteRepository.save(note),
  delete: (type, id) => noteRepository.delete(id),
}

const sync = new SyncEngine({
  entities: registerEntities([Note], persistence),
  store: new JsonSyncStore({
    storage: localStorage,
    key: `notes:sync:${localAccountId}`,
    connectionId: `dropbox:${remoteAccountId}`,
  }),
  backend: SyncBackendFactory.create(
    { provider: 'dropbox', directory: 'notes/sync/operations' },
    { getAccessToken },
  ),
  withWrite: action => database.withWriteLock(action),
  writeEntity: action => database.transaction(action),
})

await sync.sync()
await sync.sync({ full: true }) // Import history and publish a checkpoint.
```

For several entity types, dispatch the persistence methods to the corresponding repository using `type`. Register classes in creation dependency order (for example, folders before notes); deletes apply in reverse order.

`ISyncEntity` describes `id` and `toJSON()`. `ISyncEntityClass` describes static `entityType` and `fromJSON()`. TypeScript checks their structure, while runtime checks reject duplicate types, malformed registrations, invalid instances, and identity changes. `fromJSON()` is responsible for validating untrusted entity data before persistence. Existing classes with a different factory API can instead supply `EntitySyncAdapter` objects directly.

## Local bookkeeping

`JsonSyncStore` provides the journal, device sequence, snapshots, clocks, tombstones, rejected-file revisions, and per-connection receipts. It accepts any `ISyncStateStorage` with `getItem(key)` and `setItem(key, value)`, synchronous or asynchronous. Browser `localStorage` works directly. Node and other hosts can supply file or database storage; `setItem` must replace one value atomically and durably. Use memory storage only for tests or ephemeral sessions.

Use a unique storage key per app/local account, and a stable connection ID including provider, endpoint/space, and authenticated remote identity. Keep the same key when switching connections to retain the journal and tombstones. Use a single writer/store instance per key at a time, including across browser tabs; the built-in queue serializes writes within one instance. It does not coordinate multiple tabs or processes. Close/finish the old connection before opening the next one.

The JSON store rewrites its full retained state on each mutation, so it suits smaller datasets. Larger apps can implement `ISyncStore` using SQLite or another durable database. No application-specific journal implementation is required to get started. This store uses its own versioned format and does not automatically migrate journals from other integrations.

`withWrite` must serialize engine capture and merge work with ordinary app writes. Network requests happen outside that lock. `writeEntity` must commit the complete entity and its related rows atomically, using the same transaction in the registered persistence methods. The engine acknowledges snapshots/clocks after entity persistence; custom stores and repositories must allow idempotent replay after a crash between those steps.

## Providers and credentials

All providers are included. The host app handles login and supplies a callback returning a valid access token; credentials are never stored in operation payloads. An optional `fetch` implementation supports host-specific networking and abort signals.

```ts
const backend = SyncBackendFactory.create(
  { provider: 'google-drive', directory: 'notes/sync/operations' },
  { getAccessToken },
)

const backend = SyncBackendFactory.create(
  { provider: 'supabase', url, publishableKey, space: 'notes' },
  { getAccessToken },
)
```

Dropbox requires app access to its configured directory. Google Drive uses `appDataFolder`; configure OAuth for Drive app-data access. Cloud permissions and OAuth clients must be configured for the new app. Remote directories/spaces must be isolated between unrelated apps and identities.

For Supabase, apply every file in [`supabase/migrations`](supabase/migrations) in filename order to your project, enable Supabase Auth, and supply a user session access token. The client does not deploy SQL. Use a publishable key, never a service-role key. The last migration allows arbitrary registered entity names. The `lyra_sync_` table/RPC prefix is retained for wire compatibility and does not depend on the original app. Owner isolation, immutable operations, pagination ordering, and checkpoint validation are enforced server-side.

Use `SyncService.run(connectionId, full, signal => engine.sync({ full }))` to serialize calls and retry transient failures. Its signal must be wired to the supplied fetch and persistence guards if cancellation is required; the service cannot cancel arbitrary injected code. Automatic scheduling and UI remain the host's responsibility.

## Merge behavior and extensions

The default policy merges JSON fields independently with last-write-wins clocks, retains entity deletion tombstones, and treats ordinary arrays as whole values. Device timestamps participate in conflict ordering. The wire format accepts v1/v2 operations and `full-sync-v1` checkpoints. Historical operations are retained; checkpoints do not prune history.

`ISyncPolicy` supplies optional normalization, grouped field changes, custom field merging, keyed nested records, deferred record dependencies, and post-import repair. Default reserved names include `id`, fields beginning with `__`, and the internal `record:` clock prefix.

For individually keyed nested records, set `recordField`, `records`, `recordKey`, and `isValidRecord` on the policy, and pass the same `recordKey` to `JsonSyncStore`. The default internal record slot is `__sync_records__`. Supabase's included checkpoint SQL retains the existing measurement record-key shape (`attributeId`, `datetimeUTC`, `routineId`, `routineActivityId`); other keyed-record shapes need corresponding server validation changes. Ordinary JSON fields and arrays work with every included backend.

## Read-only cloud inspection

`inspectCloud(backend, { signal, progress })` lists and reads remote envelopes without a local store or engine. It returns `entries` containing raw bodies and per-entry read errors, plus `entities` grouped by type and ID with all supported operation/checkpoint versions. It never appends, publishes checkpoints, deletes, or acknowledges data. Unknown formats remain available in raw entries. This is a remote history viewer, not a calculation of the current merged state. Hosts must guard account changes and supply an abort-aware fetch when needed.

## Commands

Run these from this directory, including after copying it elsewhere:

```sh
npm install         # Install build tooling and compile.
npm run build       # Emit JavaScript and declarations into dist/.
npm run typecheck   # Check the package independently.
npm test            # Build and run local tests.
npm run example     # Build and run the two-client registration example.
```

Tests cover registration, initial sync, edits, checkpoints, deletion, durable restart, connection receipt isolation, concurrent journal writes, failed-write recovery, and custom record clocks. They use simulated remotes; testing actual provider projects requires credentials and network access.


## RVN server / SQLite provider

```ts
const backend = SyncBackendFactory.create(
  { provider: 'rvn', url: 'http://localhost:8765', space: 'rvn/workspace' },
  { getAccessToken: async () => '' },
)
```

The RVN API stores immutable envelopes in the signed-in account's SQLite workspace,
with stable insertion pagination, idempotent append, conflict detection, and checkpoint
support. The host supplies authenticated HTTP transport; the RVN app uses its existing
session, including a native cookie jar in Tauri. Space names are encoded as UTF-8
base64url path segments, so names containing slashes and Unicode remain distinct.
The package itself remains independent of RVN, Vue and Tauri.

`JsonSyncStore` retains successfully imported operations with their original clocks.
Other connections sharing that journal forward both edits and deletion tombstones.
Set `bootstrapConnection: true` when constructing each connection's engine to publish
current data and tombstones to a newly added destination. Construct stores sequentially
from the shared durable journal; never keep multiple cached store instances writing
the same key concurrently.

### Automatic checkpoints and file transfer updates

`ISyncEngineOptions` accepts `automaticCheckpoints` (default `true`) and
`checkpointOperationThreshold` (default `100`, positive integer). A successful
ordinary sync publishes after that many uncovered operations, or after 24 hours
with changes. Ordinary uploads complete first. Full sync remains explicit.
Checkpoints contain exact operation IDs and covered checkpoint revisions, allowing
new devices to skip covered history while preserving independent branches.
File backends transfer up to four immutable operations concurrently. Stores can
implement `setFieldClocks` to persist entity clocks together. RVN retains imported
history for forwarding between connected destinations, including checkpoint-only
data with its original clocks, without counting it as uncovered on the source.

Local entity capture now uses the optional `ISyncStore.batch` API under the host
write lock. `JsonSyncStore` clones and durably writes its journal once per capture
transaction, rather than once per entity mutation. Failed persistence discards
the draft and can retry without losing queued history. Large comparisons report
progress every 100 entities and continue yielding to the browser event loop.
Uploaded-receipt queries use a Set instead of repeated linear array searches.

Remote application groups journal updates and delivery receipts into batches of
100 operations under the host write lock. Entity writes retain dependency order;
progress updates report batch completion. If an entity write fails, successful
preceding operations retain their receipts and snapshots before the error is
reported, so retrying does not turn imported records into local edits.

Hosts can provide `withApplyBatch(operations, action)` under the existing write
lock and choose `applyBatchSize` (1–1000, default 100). Raven uses 500 and stages
entity writes until its journal storage commits both in a single SQLite
transaction. `markAppliedMany` avoids rebuilding receipt sets per entity.
