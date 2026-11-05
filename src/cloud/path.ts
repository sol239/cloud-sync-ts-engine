import { CloudStorageError } from './CloudStorageError'
import type { CloudFileContent } from './types'

export function normalizeCloudPath(path: string, allowRoot = false): string {
  const trimmed = path.trim()
  if (allowRoot && (trimmed === '' || trimmed === '/')) return ''

  const withoutEdges = trimmed.replace(/^\/+|\/+$/g, '')
  const parts = withoutEdges.split('/')
  if (
    withoutEdges === ''
    || path.includes('\\')
    || path.includes('\0')
    || parts.some(part => part === '' || part === '.' || part === '..')
  ) {
    throw new CloudStorageError(`Invalid cloud path: ${path}`, { code: 'invalid-path' })
  }

  return parts.join('/')
}

export function splitCloudPath(path: string): { directory: string, name: string } {
  const normalized = normalizeCloudPath(path)
  const parts = normalized.split('/')
  const name = parts.pop()
  if (!name) {
    throw new CloudStorageError(`Invalid cloud file path: ${path}`, { code: 'invalid-path' })
  }
  return { directory: parts.join('/'), name }
}

export function joinCloudPath(directory: string, name: string): string {
  return directory ? `${directory}/${name}` : name
}

export function contentToBlob(content: CloudFileContent, contentType?: string): Blob {
  if (content instanceof Blob) {
    return contentType && content.type !== contentType
      ? new Blob([content], { type: contentType })
      : content
  }
  if (typeof content === 'string') return new Blob([content], { type: contentType })
  if (content instanceof ArrayBuffer) return new Blob([content], { type: contentType })

  const bytes = new Uint8Array(content.buffer, content.byteOffset, content.byteLength)
  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  return new Blob([copy], { type: contentType })
}
