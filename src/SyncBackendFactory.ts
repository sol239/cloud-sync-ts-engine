import { DropboxCloudStorage, GoogleDriveCloudStorage, type CloudAccessTokenProvider, type CloudFetch } from './cloud'
import { RvnSyncBackend } from './backends/RvnSyncBackend'
import { FileSyncBackend } from './backends/FileSyncBackend'
import { SupabaseSyncBackend } from './backends/SupabaseSyncBackend'
import type { ISyncBackend } from './backends/ISyncBackend'

export type SyncProvider = 'dropbox' | 'google-drive' | 'supabase' | 'rvn'
export type SyncBackendConfig
  = | { provider: 'dropbox' | 'google-drive', directory: string }
    | { provider: 'rvn', url: string, space: string }
    | { provider: 'supabase', url: string, publishableKey: string, space: string }

export const SyncBackendFactory = {
  create(config: SyncBackendConfig, dependencies: { getAccessToken: CloudAccessTokenProvider, fetch?: CloudFetch }): ISyncBackend {
    const { getAccessToken, fetch } = dependencies
    if ((config.provider === 'dropbox' || config.provider === 'google-drive') && (!config.directory || config.directory.split('/').some(part => !part || part === '.' || part === '..'))) {
      throw new Error('A valid relative sync directory is required.')
    }
    switch (config.provider) {
      case 'dropbox': return new FileSyncBackend(new DropboxCloudStorage(getAccessToken, fetch), config.directory)
      case 'google-drive': return new FileSyncBackend(new GoogleDriveCloudStorage(getAccessToken, fetch), config.directory)
      case 'rvn': return new RvnSyncBackend(config.url, config.space, getAccessToken, fetch)
      case 'supabase': return new SupabaseSyncBackend(config.url, config.publishableKey, config.space, getAccessToken, fetch)
      default: throw new Error('Unsupported sync provider.')
    }
  }
}
