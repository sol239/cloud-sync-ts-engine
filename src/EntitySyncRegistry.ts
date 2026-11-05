import type { SyncEntityType, SyncPayload } from './types'

export interface EntitySyncAdapter {
  type: SyncEntityType
  all(): Promise<Array<{ entityId: string, payload: SyncPayload }>>
  read(id: string): Promise<SyncPayload | null>
  save(id: string, payload: SyncPayload): Promise<void>
  delete(id: string): Promise<void>
}

/** Registration order is the dependency order for creation; deletion reverses it. */
export class EntitySyncRegistry {
  constructor(readonly adapters: EntitySyncAdapter[]) {
    for (const adapter of adapters) {
      if (!adapter || typeof adapter.type !== 'string' || !adapter.type.trim()
        || ['all', 'read', 'save', 'delete'].some(method => typeof adapter[method as keyof EntitySyncAdapter] !== 'function')) {
        throw new Error('Invalid sync entity registration.')
      }
    }
    if (new Set(adapters.map(adapter => adapter.type)).size !== adapters.length) throw new Error('Duplicate sync entity registration.')
  }

  get(type: SyncEntityType): EntitySyncAdapter {
    const adapter = this.adapters.find(adapter => adapter.type === type)
    if (!adapter) throw new Error(`Unsupported sync entity: ${type}`)
    return adapter
  }
}

/** Register entity classes without adding a sync dependency to those classes. */
export function registerEntities(
  classes: import('./contracts').ISyncEntityClass[],
  persistence: import('./contracts').IEntityPersistence
): EntitySyncAdapter[] {
  const adapters = classes.map((entity) => {
    if (!entity || typeof entity.entityType !== 'string' || !entity.entityType.trim()
      || typeof entity.fromJSON !== 'function') throw new Error('Invalid sync entity class registration.')
    const serialize = (value: import('./contracts').ISyncEntity): SyncPayload => {
      if (!value || typeof value.id !== 'string' || !value.id || typeof value.toJSON !== 'function') {
        throw new Error(`Invalid ${entity.entityType} sync entity.`)
      }
      const payload = value.toJSON()
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('Entity JSON must be an object.')
      return { ...payload, id: value.id }
    }
    return {
      type: entity.entityType,
      all: async () => (await persistence.list(entity.entityType)).map(value => ({ entityId: value.id, payload: serialize(value) })),
      read: async (id: string) => {
        const value = await persistence.read(entity.entityType, id)
        return value ? serialize(value) : null
      },
      save: async (id: string, payload: SyncPayload) => {
        const value = entity.fromJSON({ ...payload, id })
        serialize(value)
        if (value.id !== id) throw new Error('Entity identity changed during deserialization.')
        await persistence.save(entity.entityType, value)
      },
      delete: (id: string) => persistence.delete(entity.entityType, id)
    }
  })
  return new EntitySyncRegistry(adapters).adapters
}
