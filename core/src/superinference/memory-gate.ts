import { formatRelativeTime } from '../memory';
import type { MemoryManager } from '../memory';
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

// Overall budgets on gate state and the content it emits for the model, so a
// long-running session cannot grow the entries array — or the memory /
// retriever context derived from it — without bound. Mirrors the explicit
// MAX_STATUS_CHARS-style cap used for model-facing content elsewhere.
//
// MAX_ENTRIES bounds the in-memory ring of approved entries (oldest dropped).
// MAX_*_CHARS cap the aggregate character length of the joined content emitted
// by getApprovedContext()/persistTo(); when a cap fires, the content is cut on
// an entry boundary where possible and a truncation sentinel is appended so the
// reader knows the tail was omitted rather than silently dropped.
export const MAX_ENTRIES = 100;
export const MAX_CONTEXT_CHARS = 8000;
export const MAX_PERSIST_CHARS = 8000;

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

export class MemoryGate {
  private entries: GatedMemoryEntry[] = [];
  // Epoch ms of the last persistTo() consolidation, or null until the first one.
  // Surfaced through getConsolidationStatus()/getLastConsolidatedAt() so a readout
  // can show consolidation freshness (parity with Claude's readLastConsolidatedAt).
  private lastConsolidatedAt: number | null = null;

  // Equation 5: M_{t+1} = M_t ∪ {(q_t, a_t, metadata)} if approved, M_t if rejected
  gate(
    query: string,
    result: string,
    decision: CriticDecision,
    belief: number,
    step: number,
  ): boolean {
    if (decision.approved) {
      this.entries.push({
        query,
        result,
        criticScore: decision.score,
        belief,
        step,
        timestamp: Date.now(),
      });
      // Bound the ring of retained entries: drop the oldest so a long session
      // cannot grow the array (and the context derived from it) without limit.
      if (this.entries.length > MAX_ENTRIES) {
        this.entries.splice(0, this.entries.length - MAX_ENTRIES);
      }
      return true; // Memory updated
    }
    return false; // Rejected, memory unchanged
  }

  // Persist gated entries to the memory manager
  persistTo(memory: MemoryManager, taskDescription: string): void {
    if (this.entries.length === 0) return;

    const content = capAggregate(
      this.entries
        .map((e) => `Step ${e.step}: ${e.query}\n→ ${truncResult(e.result, 200)} (score: ${e.criticScore.toFixed(2)})`)
        .join('\n\n'),
      MAX_PERSIST_CHARS,
    );

    memory.saveMemory(
      `task-${Date.now()}`,
      `# Task: ${taskDescription}\n\n${content}`,
      'Critic-approved task results',
      'project',
    );
    // Record the consolidation time so a readout can report freshness.
    this.lastConsolidatedAt = Date.now();
  }

  /**
   * Freshness readout for memory consolidation — parity with Claude's
   * consolidation-lock mtime read + MemoryFileSelector status row. Exposes the
   * last-consolidation timestamp (parity: readLastConsolidatedAt) alongside the
   * derived state/relative string. `now` is injectable for deterministic tests.
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
    };
  }

  getEntries(): GatedMemoryEntry[] { return [...this.entries]; }

  // Get context from approved results (for retriever)
  getApprovedContext(): string {
    return capAggregate(
      this.entries
        .map(e => `Q: ${e.query}\nA: ${truncResult(e.result, 500)}`)
        .join('\n\n'),
      MAX_CONTEXT_CHARS,
    );
  }

  reset(): void {
    this.entries = [];
    this.lastConsolidatedAt = null;
  }
}
