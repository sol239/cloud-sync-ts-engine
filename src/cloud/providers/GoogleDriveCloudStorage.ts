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

const API_BASE = 'https://www.googleapis.com/drive/v3'
const UPLOAD_BASE = 'https://www.googleapis.com/upload/drive/v3'
const APP_DATA_ROOT = 'appDataFolder'
const FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'
const FILE_FIELDS = 'id,name,mimeType,size,modifiedTime,version,parents'

interface GoogleDriveFile {
  id: string
  name: string
  mimeType?: string
  size?: string
  modifiedTime?: string
  version?: string
  parents?: string[]
}

interface GoogleDriveFileList {
  files?: GoogleDriveFile[]
  nextPageToken?: string
}

export class GoogleDriveCloudStorage extends BaseCloudStorage implements ICloudStorage {
  readonly provider = 'google-drive' as const
  readonly capabilities: CloudStorageCapabilities = { conditionalWrites: false }
  private readonly directoryIds = new Map<string, string>([['', APP_DATA_ROOT]])
  private readonly directoryCreations = new Map<string, Promise<GoogleDriveFile>>()

  async uploadFile(
    filePath: string,
    fileContent: CloudFileContent,
    options: CloudWriteOptions = {}
  ): Promise<CloudFileMetadata> {
    const path = normalizeCloudPath(filePath)
    const { directory, name } = splitCloudPath(path)
    const parentId = await this.resolveDirectory(directory, true)
    const existing = await this.findChild(parentId, name, false)

    if (options.createOnly && existing) {
      throw this.conflict(`Google Drive file already exists: ${path}`)
    }
    if (options.expectedRevision && existing?.version !== options.expectedRevision) {
      throw this.conflict(`Google Drive file changed before it could be written: ${path}`)
    }
    if (options.expectedRevision && !existing) {
      throw this.conflict(`Google Drive file no longer exists: ${path}`)
    }

    const contentType = options.contentType || 'application/octet-stream'
    const content = contentToBlob(fileContent, contentType)
    let response: Response
    if (existing) {
      const params = new URLSearchParams({ uploadType: 'media', fields: FILE_FIELDS })
      response = await this.request(`${UPLOAD_BASE}/files/${encodeURIComponent(existing.id)}?${params}`, {
        method: 'PATCH',
        headers: { 'Content-Type': contentType },
        body: content
      })
    } else {
      const boundary = `cloud-storage-${globalThis.crypto.randomUUID()}`
      const multipart = new Blob([
        `--${boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n`,
        JSON.stringify({ name, parents: [parentId] }),
        `\r\n--${boundary}\r\nContent-Type: ${contentType}\r\n\r\n`,
        content,
        `\r\n--${boundary}--`
      ])
      const params = new URLSearchParams({ uploadType: 'multipart', fields: FILE_FIELDS })
      response = await this.request(`${UPLOAD_BASE}/files?${params}`, {
        method: 'POST',
        headers: { 'Content-Type': `multipart/related; boundary=${boundary}` },
        body: multipart
      })
    }

    await this.assertOk(response, `Google Drive could not upload ${path}.`)
    return this.toMetadata(await response.json() as GoogleDriveFile, path)
  }

  async downloadFile(filePath: string): Promise<CloudFile> {
    const path = normalizeCloudPath(filePath)
    const file = await this.resolveFile(path)
    if (!file) throw this.notFound(path)

    const downloaded = await this.downloadFileById(file.id, path)
    return { ...downloaded, metadata: { ...this.toMetadata(file, path), contentType: downloaded.metadata.contentType } }
  }

  async downloadFileById(id: string, path: string): Promise<CloudFile> {
    const response = await this.request(`${API_BASE}/files/${encodeURIComponent(id)}?alt=media`, { method: 'GET' })
    await this.assertOk(response, `Google Drive could not download ${path}.`)
    // Listed files are immutable in sync. Keep the listed revision at the backend.
    return {
      metadata: { id, path, name: splitCloudPath(path).name, size: 0, revision: '', contentType: response.headers.get('Content-Type') ?? undefined },
      data: new Uint8Array(await response.arrayBuffer())
    }
  }

  async deleteDirectory(directoryPath: string): Promise<void> {
    const directory = normalizeCloudPath(directoryPath)
    for (const id of await this.resolveDirectories(directory)) {
      const response = await this.request(`${API_BASE}/files/${encodeURIComponent(id)}`, { method: 'DELETE' })
      if (response.status !== 404) await this.assertOk(response, `Google Drive could not delete ${directory}.`)
    }
    this.directoryIds.clear()
    this.directoryIds.set('', APP_DATA_ROOT)
  }

  async deleteFile(
    filePath: string,
    options: Pick<CloudWriteOptions, 'expectedRevision'> = {}
  ): Promise<void> {
    const path = normalizeCloudPath(filePath)
    const file = await this.resolveFile(path)
    if (!file) throw this.notFound(path)
    if (options.expectedRevision && file.version !== options.expectedRevision) {
      throw this.conflict(`Google Drive file changed before it could be deleted: ${path}`)
    }

    const response = await this.request(`${API_BASE}/files/${encodeURIComponent(file.id)}`, {
      method: 'DELETE'
    })
    await this.assertOk(response, `Google Drive could not delete ${path}.`)
  }

  async listFiles(
    directoryPath = '',
    options: CloudListOptions = {}
  ): Promise<CloudFileMetadata[]> {
    const directory = normalizeCloudPath(directoryPath, true)
    const rootIds = await this.resolveDirectories(directory)
    if (!rootIds.length) return []

    const files: CloudFileMetadata[] = []
    const pending = rootIds.map(id => ({ id, path: directory }))
    const visited = new Set<string>()
    while (pending.length > 0) {
      const current = pending.shift()!
      if (visited.has(current.id)) continue
      visited.add(current.id)
      for (const child of await this.listChildren(current.id)) {
        const childPath = joinCloudPath(current.path, child.name)
        if (child.mimeType === FOLDER_MIME_TYPE) {
          if (options.recursive) pending.push({ id: child.id, path: childPath })
        } else {
          files.push(this.toMetadata(child, childPath))
        }
      }
    }
    return files
  }

  async statFile(filePath: string): Promise<CloudFileMetadata | null> {
    const path = normalizeCloudPath(filePath)
    const file = await this.resolveFile(path)
    return file ? this.toMetadata(file, path) : null
  }

  /** Concurrent Drive creates may produce multiple folders with the same name. */
  private async resolveDirectories(path: string): Promise<string[]> {
    let parents = [APP_DATA_ROOT]
    for (const name of path.split('/').filter(Boolean)) {
      const branches = await Promise.all(parents.map(parent => this.queryFiles(
        `'${this.escapeQuery(parent)}' in parents and name = '${this.escapeQuery(name)}' and trashed = false and mimeType = '${FOLDER_MIME_TYPE}'`
      )))
      parents = branches.flat().map(folder => folder.id)
      if (!parents.length) break
    }
    return parents
  }

  private async resolveFile(path: string): Promise<GoogleDriveFile | null> {
    const { directory, name } = splitCloudPath(path)
    const parentId = await this.resolveDirectory(directory, false)
    return parentId ? await this.findChild(parentId, name, false) : null
  }

  private async resolveDirectory(path: string, create: true): Promise<string>
  private async resolveDirectory(path: string, create: false): Promise<string | null>
  private async resolveDirectory(path: string, create: boolean): Promise<string | null> {
    const cached = this.directoryIds.get(path)
    if (cached) return cached

    let currentPath = ''
    let parentId = APP_DATA_ROOT
    for (const segment of path.split('/').filter(Boolean)) {
      currentPath = joinCloudPath(currentPath, segment)
      const cachedId = this.directoryIds.get(currentPath)
      if (cachedId) {
        parentId = cachedId
        continue
      }

      let lookup = this.directoryCreations.get(currentPath)
      if (!lookup && create) {
        const parent = parentId
        lookup = (async () => await this.findChild(parent, segment, true) ?? await this.createDirectory(parent, segment))()
        this.directoryCreations.set(currentPath, lookup)
      }
      let folder: GoogleDriveFile | null
      try {
        folder = lookup ? await lookup : await this.findChild(parentId, segment, true)
      } finally {
        if (lookup) this.directoryCreations.delete(currentPath)
      }
      if (!folder) return null
      parentId = folder.id
      this.directoryIds.set(currentPath, parentId)
    }
    return parentId
  }

  private async createDirectory(parentId: string, name: string): Promise<GoogleDriveFile> {
    const params = new URLSearchParams({ fields: FILE_FIELDS })
    const response = await this.request(`${API_BASE}/files?${params}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, mimeType: FOLDER_MIME_TYPE, parents: [parentId] })
    })
    await this.assertOk(response, `Google Drive could not create directory ${name}.`)
    return await response.json() as GoogleDriveFile
  }

  private async findChild(
    parentId: string,
    name: string,
    directory: boolean
  ): Promise<GoogleDriveFile | null> {
    const mimeQuery = directory
      ? ` and mimeType = '${FOLDER_MIME_TYPE}'`
      : ` and mimeType != '${FOLDER_MIME_TYPE}'`
    const query = `'${this.escapeQuery(parentId)}' in parents and name = '${this.escapeQuery(name)}' and trashed = false${mimeQuery}`
    const files = await this.queryFiles(query, 2)
    return files[0] ?? null
  }

  private async listChildren(parentId: string): Promise<GoogleDriveFile[]> {
    return await this.queryFiles(`'${this.escapeQuery(parentId)}' in parents and trashed = false`)
  }

  private async queryFiles(query: string, pageSize = 1000): Promise<GoogleDriveFile[]> {
    const files: GoogleDriveFile[] = []
    let pageToken: string | undefined
    do {
      const params = new URLSearchParams({
        spaces: APP_DATA_ROOT,
        q: query,
        fields: `nextPageToken,files(${FILE_FIELDS})`,
        pageSize: String(pageSize),
        orderBy: 'modifiedTime desc'
      })
      if (pageToken) params.set('pageToken', pageToken)
      const response = await this.request(`${API_BASE}/files?${params}`, { method: 'GET' })
      await this.assertOk(response, 'Google Drive could not list files.')
      const result = await response.json() as GoogleDriveFileList
      files.push(...(result.files ?? []))
      pageToken = result.nextPageToken
    } while (pageToken)
    return files
  }

  private toMetadata(file: GoogleDriveFile, path: string): CloudFileMetadata {
    return {
      id: file.id,
      path,
      name: file.name,
      size: Number(file.size ?? 0),
      revision: file.version ?? file.modifiedTime ?? '',
      modifiedAt: file.modifiedTime,
      contentType: file.mimeType
    }
  }

  private escapeQuery(value: string): string {
    return value.replace(/\\/g, '\\\\').replace(/'/g, '\\\'')
  }

  private conflict(message: string): CloudStorageError {
    return new CloudStorageError(message, { code: 'conflict', provider: this.provider })
  }

  private notFound(path: string): CloudStorageError {
    return new CloudStorageError(`Google Drive file not found: ${path}`, {
      code: 'not-found',
      provider: this.provider
    })
  }
}
