import { BaseCloudStorage } from '../BaseCloudStorage'
import { CloudStorageError } from '../CloudStorageError'
import type { ICloudStorage } from '../ICloudStorage'
import { contentToBlob, joinCloudPath, normalizeCloudPath, splitCloudPath } from '../path'
import type {
  CloudFile,
  CloudFileContent,
  CloudFileMetadata,
  CloudListOptions,
  CloudStorageCapabilities,
  CloudWriteOptions
} from '../types'

const RPC_BASE = 'https://api.dropboxapi.com/2'
const CONTENT_BASE = 'https://content.dropboxapi.com/2'

interface DropboxFileMetadata {
  '.tag': 'file'
  'name': string
  'path_display'?: string
  'size': number
  'rev': string
  'server_modified'?: string
}

interface DropboxFolderMetadata {
  '.tag': 'folder'
  'name': string
  'path_display'?: string
}

type DropboxMetadata = DropboxFileMetadata | DropboxFolderMetadata | { '.tag': 'deleted' }

interface DropboxListResult {
  entries: DropboxMetadata[]
  cursor: string
  has_more: boolean
}

export class DropboxCloudStorage extends BaseCloudStorage implements ICloudStorage {
  private readonly ensuredDirectories = new Set<string>()
  private readonly pendingDirectories = new Map<string, Promise<void>>()

  readonly provider = 'dropbox' as const
  readonly capabilities: CloudStorageCapabilities = { conditionalWrites: true }

  async uploadFile(
    filePath: string,
    fileContent: CloudFileContent,
    options: CloudWriteOptions = {}
  ): Promise<CloudFileMetadata> {
    const path = normalizeCloudPath(filePath)
    const { directory } = splitCloudPath(path)
    await this.ensureDirectory(directory)

    const mode = options.expectedRevision
      ? { '.tag': 'update', 'update': options.expectedRevision }
      : options.createOnly
        ? 'add'
        : 'overwrite'
    const response = await this.request(`${CONTENT_BASE}/files/upload`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/octet-stream',
        'Dropbox-API-Arg': JSON.stringify({
          path: this.apiPath(path),
          mode,
          autorename: false,
          mute: true,
          strict_conflict: options.createOnly === true
        })
      },
      body: contentToBlob(fileContent, options.contentType)
    })
    await this.assertOk(response, `Dropbox could not upload ${path}.`)
    return this.toMetadata(await response.json() as DropboxFileMetadata, path)
  }

  async downloadFile(filePath: string): Promise<CloudFile> {
    const path = normalizeCloudPath(filePath)
    const response = await this.request(`${CONTENT_BASE}/files/download`, {
      method: 'POST',
      headers: { 'Dropbox-API-Arg': JSON.stringify({ path: this.apiPath(path) }) }
    })
    await this.assertOk(response, `Dropbox could not download ${path}.`)

    const rawMetadata = response.headers.get('Dropbox-API-Result')
    if (!rawMetadata) {
      throw new CloudStorageError('Dropbox did not return file metadata.', {
        code: 'provider-error',
        provider: this.provider
      })
    }
    const metadata = this.toMetadata(JSON.parse(rawMetadata) as DropboxFileMetadata, path)
    const contentType = response.headers.get('Content-Type')
    if (contentType) metadata.contentType = contentType
    return { metadata, data: new Uint8Array(await response.arrayBuffer()) }
  }

  async deleteFile(
    filePath: string,
    options: Pick<CloudWriteOptions, 'expectedRevision'> = {}
  ): Promise<void> {
    const path = normalizeCloudPath(filePath)
    await this.rpc('/files/delete_v2', {
      path: this.apiPath(path),
      ...(options.expectedRevision ? { parent_rev: options.expectedRevision } : {})
    })
  }

  async listFiles(
    directoryPath = '',
    options: CloudListOptions = {}
  ): Promise<CloudFileMetadata[]> {
    const directory = normalizeCloudPath(directoryPath, true)
    let result = await this.rpc<DropboxListResult>('/files/list_folder', {
      path: this.apiPath(directory),
      recursive: options.recursive ?? false,
      include_deleted: false
    })
    const entries = [...result.entries]

    while (result.has_more) {
      result = await this.rpc<DropboxListResult>('/files/list_folder/continue', {
        cursor: result.cursor
      })
      entries.push(...result.entries)
    }

    return entries
      .filter((entry): entry is DropboxFileMetadata => entry['.tag'] === 'file')
      .map((entry) => {
        const path = entry.path_display
          ? normalizeCloudPath(entry.path_display)
          : joinCloudPath(directory, entry.name)
        return this.toMetadata(entry, path)
      })
  }

  async statFile(filePath: string): Promise<CloudFileMetadata | null> {
    const path = normalizeCloudPath(filePath)
    try {
      const metadata = await this.rpc<DropboxMetadata>('/files/get_metadata', {
        path: this.apiPath(path),
        include_deleted: false
      })
      return metadata['.tag'] === 'file' ? this.toMetadata(metadata, path) : null
    } catch (error) {
      if (error instanceof CloudStorageError && error.code === 'not-found') return null
      throw error
    }
  }

  private async ensureDirectory(directoryPath: string): Promise<void> {
    if (!directoryPath) return
    const segments = directoryPath.split('/')
    let current = ''
    for (const segment of segments) {
      current = joinCloudPath(current, segment)
      const key = current.toLowerCase()
      if (this.ensuredDirectories.has(key)) continue
      let pending = this.pendingDirectories.get(key)
      if (!pending) {
        pending = this.ensureFolder(current)
        this.pendingDirectories.set(key, pending)
      }
      try {
        await pending
        this.ensuredDirectories.add(key)
      } finally {
        if (this.pendingDirectories.get(key) === pending) this.pendingDirectories.delete(key)
      }
    }
  }

  private async ensureFolder(path: string): Promise<void> {
    const lookup = async () => {
      const metadata = await this.rpc<DropboxMetadata>('/files/get_metadata', {
        path: this.apiPath(path), include_deleted: false
      })
      if (metadata['.tag'] !== 'folder') {
        throw new CloudStorageError(`Dropbox path ${path} is not a folder.`, {
          code: 'conflict', provider: this.provider
        })
      }
    }
    try {
      await lookup()
      return
    } catch (error) {
      if (!(error instanceof CloudStorageError && error.code === 'not-found')) throw error
    }
    try {
      await this.rpc('/files/create_folder_v2', { path: this.apiPath(path), autorename: false })
    } catch (error) {
      if (!(error instanceof CloudStorageError && error.code === 'conflict')) throw error
      // Another client may have created it since the lookup. A file conflict must still fail.
      await lookup()
    }
  }

  private async rpc<T = unknown>(route: string, body: unknown): Promise<T> {
    const response = await this.request(`${RPC_BASE}${route}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    })
    await this.assertOk(response, `Dropbox request ${route} failed.`)
    return await response.json() as T
  }

  private apiPath(path: string): string {
    return path ? `/${path}` : ''
  }

  private toMetadata(metadata: DropboxFileMetadata, fallbackPath: string): CloudFileMetadata {
    return {
      path: metadata.path_display ? normalizeCloudPath(metadata.path_display) : fallbackPath,
      name: metadata.name,
      size: metadata.size,
      revision: metadata.rev,
      modifiedAt: metadata.server_modified
    }
  }
}
