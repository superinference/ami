import type { MemoryManager } from '../memory';
import type { MemoryGate } from './memory-gate';

export class Retriever {
  private η: number; // Noise level — Eq. 2: C_η corruption channel

  constructor(noiseLevel: number = 0.1) {
    this.η = Math.max(0, Math.min(1, noiseLevel));
  }

  // Eq. 2: m̃_t ~ C_η(m_t)
  // With probability η, each retrieved entry may be dropped (corrupted).
  // η=0 → perfect retrieval, η=1 → maximally degraded retrieval.
  retrieve(
    query: string,
    memory: MemoryManager,
    memoryGate: MemoryGate,
  ): string {
    const parts: string[] = [];

    const instructions = memory.loadProjectInstructions();
    if (instructions) {
      parts.push(instructions);
    }

    // Get critic-approved results, apply noise channel
    const approvedContext = memoryGate.getApprovedContext();
    if (approvedContext) {
      const entries = approvedContext.split('\n\n').filter(Boolean);
      const filtered = this.applyNoiseChannel(entries, query);
      if (filtered.length > 0) {
        parts.push(`# Previously Approved Results\n\n${filtered.join('\n\n')}`);
      }
    }

    // Get stored memories, filter by query relevance, apply noise
    const memories = memory.loadMemories();
    if (memories.length > 0) {
      const scored = memories
        .map(m => ({ m, relevance: this.queryRelevance(query, m.content) }))
        .sort((a, b) => b.relevance - a.relevance)
        .filter(s => s.relevance > 0);

      const relevant = scored.map(s => s.m);
      const memEntries = relevant.map(m => m.content.substring(0, 300));
      const filtered = this.applyNoiseChannel(memEntries, query);
      if (filtered.length > 0) {
        parts.push(`# Stored Memories\n\n${filtered.join('\n')}`);
      }
    }

    return parts.join('\n\n');
  }

  // C_η: With probability η, each entry is independently dropped
  private applyNoiseChannel(entries: string[], _query: string): string[] {
    if (this.η <= 0 || entries.length === 0) return entries;
    if (this.η >= 1) return [];

    return entries.filter(() => Math.random() > this.η);
  }

  // Simple keyword-overlap relevance scoring
  private queryRelevance(query: string, content: string): number {
    if (!query) return 1; // No query → everything is relevant
    const queryTokens = query.toLowerCase().split(/\W+/).filter(t => t.length > 2);
    if (queryTokens.length === 0) return 1;

    const contentLower = content.toLowerCase();
    let matches = 0;
    for (const token of queryTokens) {
      if (contentLower.includes(token)) matches++;
    }
    return matches / queryTokens.length;
  }

  get noiseLevel(): number { return this.η; }
}
