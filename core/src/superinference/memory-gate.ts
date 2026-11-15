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
        .map((e) => `Step ${e.step}: ${e.query}\n→ ${e.result.substring(0, 200)} (score: ${e.criticScore.toFixed(2)})`)
        .join('\n\n'),
      MAX_PERSIST_CHARS,
    );

    memory.saveMemory(
      `task-${Date.now()}`,
      `# Task: ${taskDescription}\n\n${content}`,
      'Critic-approved task results',
      'project',
    );
  }

  getEntries(): GatedMemoryEntry[] { return [...this.entries]; }

  // Get context from approved results (for retriever)
  getApprovedContext(): string {
    return capAggregate(
      this.entries
        .map(e => `Q: ${e.query}\nA: ${e.result.substring(0, 500)}`)
        .join('\n\n'),
      MAX_CONTEXT_CHARS,
    );
  }

  reset(): void { this.entries = []; }
}
