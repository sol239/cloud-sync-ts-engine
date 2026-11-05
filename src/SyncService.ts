import type { SyncResult } from './types'

/** Serialize runs across connections and cancel work before rebinding identity. */
export class SyncService {
  private active?: { key: string, controller: AbortController, promise: Promise<SyncResult> }

  constructor(private readonly retryDelays = [500, 1500]) {}

  cancel(): void {
    this.active?.controller.abort()
  }

  async run(key: string, full: boolean, action: (signal: AbortSignal) => Promise<SyncResult>): Promise<SyncResult> {
    if (this.active) {
      if (this.active.key === key && !full && !this.active.controller.signal.aborted) return this.active.promise
      if (this.active.key !== key) this.cancel()
      await this.active.promise.catch(() => undefined)
      return this.run(key, full, action)
    }
    const controller = new AbortController()
    const promise = this.execute(controller.signal, action)
    const active = { key, controller, promise }
    this.active = active
    try {
      return await promise
    } finally {
      if (this.active === active) this.active = undefined
    }
  }

  private async execute(signal: AbortSignal, action: (signal: AbortSignal) => Promise<SyncResult>): Promise<SyncResult> {
    for (let attempt = 0; ; attempt++) {
      signal.throwIfAborted()
      try {
        return await action(signal)
      } catch (error) {
        const failure = error as { code?: string, status?: number }
        const retryable = failure?.code === 'network' || failure?.code === 'rate-limited'
          || (failure?.status !== undefined && failure.status >= 500)
        const delay = this.retryDelays[attempt]
        if (signal.aborted || !retryable || delay === undefined) throw error
        await new Promise<void>((resolve, reject) => {
          const abort = () => {
            clearTimeout(timer)
            reject(signal.reason)
          }
          const timer = setTimeout(() => {
            signal.removeEventListener('abort', abort)
            resolve()
          }, delay)
          signal.addEventListener('abort', abort, { once: true })
          if (signal.aborted) abort()
        })
      }
    }
  }
}
