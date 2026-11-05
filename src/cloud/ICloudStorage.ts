import type {
  CloudFile,
  CloudFileContent,
  CloudFileMetadata,
  CloudListOptions,
  CloudStorageCapabilities,
  CloudStorageProvider,
  CloudWriteOptions
} from './types'

/**
 * Provider-neutral storage used as the transport for cloud synchronization.
 *
 * Paths are always relative, POSIX-style paths (for example
 * `operations/device-1/000001.json`). Authentication is deliberately kept
 * outside this interface; providers receive an access-token callback.
 */
export interface ICloudStorage {
  readonly provider: CloudStorageProvider
  readonly capabilities: CloudStorageCapabilities

  uploadFile(
    filePath: string,
    fileContent: CloudFileContent,
    options?: CloudWriteOptions
  ): Promise<CloudFileMetadata>

  downloadFile(filePath: string): Promise<CloudFile>
  downloadFileById?(id: string, path: string): Promise<CloudFile>
  deleteDirectory?(directoryPath: string): Promise<void>
  deleteFile(filePath: string, options?: Pick<CloudWriteOptions, 'expectedRevision'>): Promise<void>
  listFiles(directoryPath?: string, options?: CloudListOptions): Promise<CloudFileMetadata[]>
  statFile(filePath: string): Promise<CloudFileMetadata | null>
}
