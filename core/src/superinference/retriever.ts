import type { MemoryManager } from '../memory';
import type { MemoryGate } from './memory-gate';

// Hard caps on how many entries each source may inject into the retrieved
// context. These bound the size of the assembled context regardless of how
// many memories / approved results have accumulated, and keep the most
// relevant entries only (the lists are sorted/ordered before slicing).
const MAX_MEMORIES = 5;
const MAX_APPROVED_ENTRIES = 5;

export class Retriever {
  private η: number; // Noise level — Eq. 2: C_η corruption channel

  constructor(noiseLevel: number = 0.1) {
    this.η = Math.max(0, Math.min(1, noiseLevel));
  }

  // Eq. 2: m̃_t ~ C_η(m_t)
  // With probability η, each retrieved entry may be dropped (corrupted).
  // η=0 → perfect retrieval, η=1 → maximally degraded retrieval.
  //
  // Each backing store is read defensively: a failing source (fs error, JSON
  // parse error, etc.) is skipped so retrieval degrades gracefully to the
  // parts that succeeded instead of aborting the whole turn.
  //
  // `opts.signal` makes retrieval cancellable: if the signal is already aborted
  // the parts assembled so far are returned immediately and the remaining
  // (potentially expensive) stages are skipped, so an interrupt/supersede does
  // not keep assembling context for a turn that is being torn down.
  //
  // `opts.surfacedMemories` is a session-scoped dedup ledger: stored memories
  // whose content is already in the set are skipped before scoring, and the
  // memories this call actually surfaces are recorded into it, so the same
  // memory is not re-injected into the retrieval preview on a later turn.
  retrieve(
    query: string,
    memory: MemoryManager,
    memoryGate: MemoryGate,
    opts?: { signal?: AbortSignal; surfacedMemories?: Set<string> },
  ): string {
    const parts: string[] = [];
    const signal = opts?.signal;
    const surfaced = opts?.surfacedMemories;

    if (signal?.aborted) return '';

    try {
      const instructions = memory.loadProjectInstructions();
      if (instructions) {
        parts.push(instructions);
      }
    } catch {
      // Skip project instructions on error; continue assembling other sources.
    }

    if (signal?.aborted) return parts.join('\n\n');

    // Get critic-approved results, cap, then apply noise channel.
    try {
      const approvedContext = memoryGate.getApprovedContext();
      if (approvedContext) {
        const entries = approvedContext
          .split('\n\n')
          .filter(Boolean)
          .slice(0, MAX_APPROVED_ENTRIES);
        const filtered = this.applyNoiseChannel(entries);
        if (filtered.length > 0) {
          parts.push(`# Previously Approved Results\n\n${filtered.join('\n\n')}`);
        }
      }
    } catch {
      // Skip approved context on error; continue assembling other sources.
    }

    if (signal?.aborted) return parts.join('\n\n');

    // Get stored memories, filter by query relevance, cap to top-K, apply noise.
    try {
      const memories = memory.loadMemories();
      // Cross-turn dedup: drop memories already surfaced in a prior preview so
      // the top-K budget is spent on fresh candidates (mirrors the reference's
      // alreadySurfaced filter in findRelevantMemories).
      const candidates = surfaced
        ? memories.filter(m => !surfaced.has(m.content))
        : memories;
      if (candidates.length > 0) {
        const relevant = candidates
          .map(m => ({ m, relevance: this.queryRelevance(query, m.content) }))
          .sort((a, b) => b.relevance - a.relevance)
          .filter(s => s.relevance > 0)
          .slice(0, MAX_MEMORIES)
          .map(s => s.m);

        // Apply the noise channel to the memory objects so the entries we
        // actually surface (post-noise survivors) are exactly those recorded
        // into the dedup ledger.
        const survivors = this.applyNoiseChannel(relevant);
        if (survivors.length > 0) {
          const memEntries = survivors.map(m => m.content.substring(0, 300));
          parts.push(`# Stored Memories\n\n${memEntries.join('\n')}`);
          if (surfaced) {
            for (const m of survivors) surfaced.add(m.content);
          }
        }
      }
    } catch {
      // Skip stored memories on error; continue assembling other sources.
    }

    return parts.join('\n\n');
  }

  // C_η: With probability η, each entry is independently dropped. Generic over
  // the entry type so both string (approved context) and memory-object
  // (stored memories) lists share one canonical noise channel.
  private applyNoiseChannel<T>(entries: T[]): T[] {
    if (this.η <= 0 || entries.length === 0) return entries;
    if (this.η >= 1) return [];

    return entries.filter(() => Math.random() > this.η);
  }

  // Simple keyword-overlap relevance scoring.
  // Matches on whole tokens (not substrings) so a query token like "cat" does
  // not spuriously match "category" inside the content.
  private queryRelevance(query: string, content: string): number {
    if (!query) return 1; // No query → everything is relevant
    const queryTokens = query.toLowerCase().split(/\W+/).filter(t => t.length > 2);
    if (queryTokens.length === 0) return 1;

    const contentTokens = new Set(content.toLowerCase().split(/\W+/).filter(Boolean));
    let matches = 0;
    for (const token of queryTokens) {
      if (contentTokens.has(token)) matches++;
    }
    return matches / queryTokens.length;
  }

  get noiseLevel(): number { return this.η; }
}
