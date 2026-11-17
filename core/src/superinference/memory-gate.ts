import { formatRelativeTime, memoryAgeDays } from '../memory';
import type { MemoryManager } from '../memory';
import { logError } from '../logger';
import type { CriticDecision } from './types';

export interface GatedMemoryEntry {
  query: string;
  result: string;
  criticScore: number;
  belief: number;
  step: number;
  timestamp: number;
}

/**
 * Freshness readout for the gate's memory consolidation (the `persistTo()` write
 * that promotes critic-approved entries into the MemoryManager). Mirrors the
 * state Claude Code surfaces from its consolidation lock + MemoryFileSelector
 * status row: whether consolidation has ever run, how long ago, and how many
 * approved entries are waiting for the next write. AMI consolidation is
 * synchronous, so there is no persistent `running` phase — the state is `never`
 * until the first write and `idle` afterwards.
 */
export interface ConsolidationStatus {
  state: 'never' | 'idle';
  /** Epoch ms of the last `persistTo()`, or null when consolidation never ran. */
  lastConsolidatedAt: number | null;
  /** Approved entries currently held in the gate, awaiting the next write. */
  pending: number;
  /** Human-readable "<n><unit> ago" of the last write, or null when never run. */
  relative: string | null;
  /**
   * Message of the most recent `persistTo()` persistence failure (I/O error
   * raised by `MemoryManager.saveMemory`), or null when the last write succeeded
   * or none has run. Surfaced for diagnostics — parity with Claude Code's
   * `logForDebugging(...Failed to…)` memory-persistence error path.
   */
  lastError: string | null;
}

// Per-entry cap: an individual stored result longer than `max` is cut and an
// explicit marker is appended so the model (and the persisted memory file) never
// read a mid-content cut as if it were the complete result. Distinct from
// capAggregate() below, which bounds the JOINED output on an entry boundary.
function truncResult(text: string, max: number): string {
  if (text.length <= max) return text;
  const omitted = text.length - max;
  return `${text.slice(0, max)}…[truncated ${omitted} chars]`;
}

// Default overall budgets on gate state and the content it emits for the model,
// so a long-running session cannot grow the entries array — or the memory /
// retriever context derived from it — without bound. Mirrors the explicit
// MAX_STATUS_CHARS-style cap used for model-facing content elsewhere.
//
// MAX_ENTRIES bounds the in-memory ring of approved entries (lowest-scored,
// oldest-on-tie dropped). MAX_*_CHARS / MAX_*_LINES cap the aggregate length of
// the joined content emitted by getApprovedContext()/persistTo(); when a cap
// fires the content is cut on an entry/line boundary where possible and a
// truncation sentinel is appended so the reader knows the tail was omitted
// rather than silently dropped. These are the DEFAULTS — each is overridable per
// instance via {@link MemoryGateOptions} so the magic numbers are configurable.
export const MAX_ENTRIES = 100;
export const MAX_CONTEXT_CHARS = 8000;
export const MAX_PERSIST_CHARS = 8000;
// Line caps mirror Claude Code's memdir MAX_ENTRYPOINT_LINES: a block that stays
// under the char budget can still be excessively many lines (many short entries),
// so a separate line boundary cap fires first on a line boundary.
export const MAX_CONTEXT_LINES = 200;
export const MAX_PERSIST_LINES = 200;
// Per-entry result truncation lengths (promoted from inline magic numbers).
export const PER_ENTRY_CONTEXT_CHARS = 500;
export const PER_ENTRY_PERSIST_CHARS = 200;
// Entries older than this (in days) get an inline staleness caveat when emitted —
// parity with Claude Code's memoryFreshnessText. Gate entries are session-scoped
// and usually fresh, so the default threshold rarely fires.
export const STALENESS_DAYS = 7;

/**
 * Per-instance configuration knobs for {@link MemoryGate}. Every field defaults
 * to the exported constant of the same concept, so `new MemoryGate()` keeps the
 * historical behavior while callers (and tests) can tune the caps. Threaded from
 * SuperInferenceConfig through {@link SuperInferenceEngine} so the magic numbers
 * are user-configurable rather than hardcoded.
 */
export interface MemoryGateOptions {
  /** Max retained approved entries (ring cap). @default MAX_ENTRIES */
  maxEntries?: number;
  /** Max entries emitted (top-N by criticScore) by the get methods. @default maxEntries */
  maxEmitEntries?: number;
  /** Aggregate char cap for getApprovedContext(). @default MAX_CONTEXT_CHARS */
  maxContextChars?: number;
  /** Aggregate char cap for persistTo(). @default MAX_PERSIST_CHARS */
  maxPersistChars?: number;
  /** Aggregate line cap for getApprovedContext(). @default MAX_CONTEXT_LINES */
  maxContextLines?: number;
  /** Aggregate line cap for persistTo(). @default MAX_PERSIST_LINES */
  maxPersistLines?: number;
  /** Per-entry result char cap in getApprovedContext(). @default PER_ENTRY_CONTEXT_CHARS */
  perEntryContextChars?: number;
  /** Per-entry result char cap in persistTo(). @default PER_ENTRY_PERSIST_CHARS */
  perEntryPersistChars?: number;
  /** Age threshold (days) past which an emitted entry carries a staleness caveat. @default STALENESS_DAYS */
  stalenessDays?: number;
}

// Cap `text` to `max` chars on an entry boundary (the `\n\n` separator used to
// join entries) when one exists within the retained prefix, else on a hard char
// boundary, appending a sentinel naming how many chars were dropped.
function capAggregate(text: string, max: number): string {
  if (text.length <= max) return text;
  const hardSlice = text.slice(0, max);
  const lastBoundary = hardSlice.lastIndexOf('\n\n');
  const kept = lastBoundary > 0 ? hardSlice.slice(0, lastBoundary) : hardSlice;
  const omitted = text.length - kept.length;
  return `${kept}\n\n… [truncated: ${omitted} more chars omitted]`;
}

// Cap `text` to `max` lines on a line boundary, appending a sentinel naming how
// many lines were dropped. Runs BEFORE capAggregate so the char cap operates on
// the already line-bounded text — mirroring Claude Code's line-then-byte order.
function capLines(text: string, max: number): string {
  const lines = text.split('\n');
  if (lines.length <= max) return text;
  const kept = lines.slice(0, max).join('\n');
  const omitted = lines.length - max;
  return `${kept}\n… [truncated: ${omitted} more lines omitted]`;
}

export class MemoryGate {
  private entries: GatedMemoryEntry[] = [];
  // Epoch ms of the last persistTo() consolidation, or null until the first one.
  // Surfaced through getConsolidationStatus()/getLastConsolidatedAt() so a readout
  // can show consolidation freshness (parity with Claude's readLastConsolidatedAt).
  private lastConsolidatedAt: number | null = null;
  // Message of the last persistTo() I/O failure, or null on success/never-run.
  private lastPersistError: string | null = null;

  private readonly maxEntries: number;
  private readonly maxEmitEntries: number;
  private readonly maxContextChars: number;
  private readonly maxPersistChars: number;
  private readonly maxContextLines: number;
  private readonly maxPersistLines: number;
  private readonly perEntryContextChars: number;
  private readonly perEntryPersistChars: number;
  private readonly stalenessDays: number;

  constructor(options: MemoryGateOptions = {}) {
    this.maxEntries = options.maxEntries ?? MAX_ENTRIES;
    this.maxEmitEntries = options.maxEmitEntries ?? this.maxEntries;
    this.maxContextChars = options.maxContextChars ?? MAX_CONTEXT_CHARS;
    this.maxPersistChars = options.maxPersistChars ?? MAX_PERSIST_CHARS;
    this.maxContextLines = options.maxContextLines ?? MAX_CONTEXT_LINES;
    this.maxPersistLines = options.maxPersistLines ?? MAX_PERSIST_LINES;
    this.perEntryContextChars = options.perEntryContextChars ?? PER_ENTRY_CONTEXT_CHARS;
    this.perEntryPersistChars = options.perEntryPersistChars ?? PER_ENTRY_PERSIST_CHARS;
    this.stalenessDays = options.stalenessDays ?? STALENESS_DAYS;
  }

  // Equation 5: M_{t+1} = M_t ∪ {(q_t, a_t, metadata)} if approved, M_t if rejected
  gate(
    query: string,
    result: string,
    decision: CriticDecision,
    belief: number,
    step: number,
  ): boolean {
    if (!decision.approved) return false; // Rejected, memory unchanged

    const entry: GatedMemoryEntry = {
      query,
      result,
      criticScore: decision.score,
      belief,
      step,
      timestamp: Date.now(),
    };

    // Deduplicate by query: a repeated query must not accumulate duplicate
    // entries (growth) — keep the higher-scored observation. A strictly higher
    // new score replaces the stored one; an equal/lower score leaves the better
    // entry in place. Either way the store does not grow.
    const existing = this.entries.findIndex((e) => e.query === query);
    if (existing >= 0) {
      if (decision.score > this.entries[existing].criticScore) {
        this.entries[existing] = entry;
      }
      return true; // Memory considered updated (deduped)
    }

    this.entries.push(entry);
    // Growth bound: evict the lowest-scored entry (oldest on a score tie) so a
    // long session keeps the most valuable observations rather than blindly the
    // most recent. Insertion order is otherwise preserved.
    while (this.entries.length > this.maxEntries) {
      this.evictLowestScored();
    }
    return true; // Memory updated
  }

  // Remove the single lowest-scored entry; ties broken by age (lowest index =
  // oldest), since the array is kept in insertion order.
  private evictLowestScored(): void {
    let minIdx = 0;
    for (let i = 1; i < this.entries.length; i++) {
      if (this.entries[i].criticScore < this.entries[minIdx].criticScore) {
        minIdx = i;
      }
    }
    this.entries.splice(minIdx, 1);
  }

  // Entries sorted by criticScore (descending) and sliced to the emit budget, so
  // the most relevant approved observations are surfaced first and a hard cap
  // bounds how many are emitted — parity with Claude Code's relevance-ranked,
  // top-N budgeted memory selection. Stable: ties keep insertion order.
  private rankedForEmit(): GatedMemoryEntry[] {
    return this.entries
      .map((e, i) => ({ e, i }))
      .sort((a, b) => b.e.criticScore - a.e.criticScore || a.i - b.i)
      .slice(0, this.maxEmitEntries)
      .map((x) => x.e);
  }

  // Inline staleness caveat for an entry older than the configured threshold,
  // computed against `now`. Reuses the canonical memoryAgeDays() clamp. Empty for
  // fresh entries so freshly gated session content carries no noise.
  private staleSuffix(timestamp: number, now: number): string {
    const ageDays = memoryAgeDays(timestamp, now);
    if (ageDays <= this.stalenessDays) return '';
    return ` [stale: ${ageDays}d old — point-in-time, verify before relying]`;
  }

  // Persist gated entries to the memory manager
  persistTo(memory: MemoryManager, taskDescription: string, now: number = Date.now()): void {
    if (this.entries.length === 0) return;

    const content = capAggregate(
      capLines(
        this.rankedForEmit()
          .map(
            (e) =>
              `Step ${e.step}: ${e.query}\n→ ${truncResult(e.result, this.perEntryPersistChars)} (score: ${e.criticScore.toFixed(2)})${this.staleSuffix(e.timestamp, now)}`,
          )
          .join('\n\n'),
        this.maxPersistLines,
      ),
      this.maxPersistChars,
    );

    // Guard the persistence I/O: a filesystem error (EACCES/ENOSPC/EROFS) raised
    // by saveMemory must not crash the POMDP loop. Record + log the failure and
    // return gracefully; the entries stay pending for a later retry and the
    // failure is surfaced via getConsolidationStatus().lastError.
    try {
      memory.saveMemory(
        `task-${now}`,
        `# Task: ${taskDescription}\n\n${content}`,
        'Critic-approved task results',
        'project',
      );
    } catch (err) {
      const code =
        err instanceof Error && 'code' in err && typeof (err as { code?: unknown }).code === 'string'
          ? (err as { code: string }).code
          : undefined;
      const message = err instanceof Error ? err.message : String(err);
      this.lastPersistError = code ? `${code}: ${message}` : message;
      logError('memory-gate', `persistTo failed: ${this.lastPersistError}`);
      return;
    }
    // Record the consolidation time so a readout can report freshness, and clear
    // any stale error from a previously failed write.
    this.lastConsolidatedAt = now;
    this.lastPersistError = null;
  }

  /**
   * Freshness readout for memory consolidation — parity with Claude's
   * consolidation-lock mtime read + MemoryFileSelector status row. Exposes the
   * last-consolidation timestamp (parity: readLastConsolidatedAt) alongside the
   * derived state/relative string and the last persistence error. `now` is
   * injectable for deterministic tests.
   */
  getConsolidationStatus(now: number = Date.now()): ConsolidationStatus {
    const last = this.lastConsolidatedAt;
    return {
      state: last === null ? 'never' : 'idle',
      lastConsolidatedAt: last,
      pending: this.entries.length,
      relative: last === null
        ? null
        : formatRelativeTime(new Date(last), { now: new Date(now) }),
      lastError: this.lastPersistError,
    };
  }

  getEntries(): GatedMemoryEntry[] { return [...this.entries]; }

  // Get context from approved results (for retriever)
  getApprovedContext(now: number = Date.now()): string {
    return capAggregate(
      capLines(
        this.rankedForEmit()
          .map((e) => `Q: ${e.query}\nA: ${truncResult(e.result, this.perEntryContextChars)}${this.staleSuffix(e.timestamp, now)}`)
          .join('\n\n'),
        this.maxContextLines,
      ),
      this.maxContextChars,
    );
  }

  reset(): void {
    this.entries = [];
    this.lastConsolidatedAt = null;
    this.lastPersistError = null;
  }
}
