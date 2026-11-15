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
  retrieve(
    query: string,
    memory: MemoryManager,
    memoryGate: MemoryGate,
  ): string {
    const parts: string[] = [];

    try {
      const instructions = memory.loadProjectInstructions();
      if (instructions) {
        parts.push(instructions);
      }
    } catch {
      // Skip project instructions on error; continue assembling other sources.
    }

    // Get critic-approved results, cap, then apply noise channel.
    try {
      const approvedContext = memoryGate.getApprovedContext();
      if (approvedContext) {
        const entries = approvedContext
          .split('\n\n')
          .filter(Boolean)
          .slice(0, MAX_APPROVED_ENTRIES);
        const filtered = this.applyNoiseChannel(entries, query);
        if (filtered.length > 0) {
          parts.push(`# Previously Approved Results\n\n${filtered.join('\n\n')}`);
        }
      }
    } catch {
      // Skip approved context on error; continue assembling other sources.
    }

    // Get stored memories, filter by query relevance, cap to top-K, apply noise.
    try {
      const memories = memory.loadMemories();
      if (memories.length > 0) {
        const relevant = memories
          .map(m => ({ m, relevance: this.queryRelevance(query, m.content) }))
          .sort((a, b) => b.relevance - a.relevance)
          .filter(s => s.relevance > 0)
          .slice(0, MAX_MEMORIES)
          .map(s => s.m);

        const memEntries = relevant.map(m => m.content.substring(0, 300));
        const filtered = this.applyNoiseChannel(memEntries, query);
        if (filtered.length > 0) {
          parts.push(`# Stored Memories\n\n${filtered.join('\n')}`);
        }
      }
    } catch {
      // Skip stored memories on error; continue assembling other sources.
    }

    return parts.join('\n\n');
  }

  // C_η: With probability η, each entry is independently dropped
  private applyNoiseChannel(entries: string[], _query: string): string[] {
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
