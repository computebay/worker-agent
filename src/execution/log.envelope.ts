/**
 * Worker-side log timestamping.
 *
 * Every log chunk leaving the worker carries `ts`: Unix epoch milliseconds
 * captured with `Date.now()` at emission time. Epoch millis are absolute —
 * they never pass through worker-local timezone formatting, so producers in
 * any timezone describe the same instant.
 *
 * Clock assumption: workers are expected to run NTP-synced clocks. A skewed
 * worker clock only shifts the displayed wall time; downstream never
 * re-sorts by `ts` (arrival order is preserved), so skew cannot reorder logs.
 */
export interface StampedLogChunk {
  /** Unix epoch milliseconds, captured at emission. */
  ts: number;
  /** Raw container output chunk (may contain multiple lines). */
  chunk: string;
}

export function stampLogChunk(chunk: string, now: number = Date.now()): StampedLogChunk {
  return { ts: now, chunk };
}
