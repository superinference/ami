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
      return true; // Memory updated
    }
    return false; // Rejected, memory unchanged
  }

  // Persist gated entries to the memory manager
  persistTo(memory: MemoryManager, taskDescription: string): void {
    if (this.entries.length === 0) return;

    const content = this.entries
      .map((e) => `Step ${e.step}: ${e.query}\n→ ${e.result.substring(0, 200)} (score: ${e.criticScore.toFixed(2)})`)
      .join('\n\n');

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
    return this.entries
      .map(e => `Q: ${e.query}\nA: ${e.result.substring(0, 500)}`)
      .join('\n\n');
  }

  reset(): void { this.entries = []; }
}
