export type CloudStorageProvider = 'dropbox' | 'google-drive'

export type CloudFileContent = Blob | ArrayBuffer | ArrayBufferView | string

export interface CloudStorageCapabilities {
  /** Whether expectedRevision is checked atomically by the provider. */
  conditionalWrites: boolean
}

export interface CloudWriteOptions {
  contentType?: string
  /** Fail when the path already exists. Intended for immutable operation files. */
  createOnly?: boolean
  /** Fail when the current remote revision differs. */
  expectedRevision?: string
}

export interface CloudListOptions {
  recursive?: boolean
}

export interface CloudFileMetadata {
  /** Provider identity distinguishes files with the same path on Drive. */
  id?: string
  path: string
  name: string
  size: number
  revision: string
  modifiedAt?: string
  contentType?: string
}

export interface CloudFile {
  metadata: CloudFileMetadata
  data: Uint8Array
}

export type CloudAccessTokenProvider = () => string | Promise<string>

export type CloudFetch = typeof globalThis.fetch
