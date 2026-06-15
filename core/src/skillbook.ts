import * as fs from 'fs';
import * as path from 'path';

export interface SkillbookEntry {
  id: string;
  section: 'context' | 'harness';
  keywords: string[];
  issue: string;
  insight: string;
  helpfulCount: number;
  harmfulCount: number;
  neutralCount: number;
  occurrences: Array<{ sessionId: string; timestamp: number; spanId?: string }>;
  createdAt: number;
  updatedAt: number;
}

export type SkillbookOperation =
  | { type: 'ADD'; entry: Omit<SkillbookEntry, 'id' | 'createdAt' | 'updatedAt' | 'helpfulCount' | 'harmfulCount' | 'neutralCount' | 'occurrences'> }
  | { type: 'TAG'; id: string; feedback: 'helpful' | 'harmful' | 'neutral' }
  | { type: 'UPDATE'; id: string; insight?: string; keywords?: string[] }
  | { type: 'REMOVE'; id: string };

export class Skillbook {
  private entries: Map<string, SkillbookEntry> = new Map();
  private persistPath: string;

  constructor(cwd: string) {
    this.persistPath = path.join(cwd, '.superinference', 'skillbook.json');
    this.loadFromDisk();
  }

  private loadFromDisk(): void {
    try {
      if (fs.existsSync(this.persistPath)) {
        const data = JSON.parse(fs.readFileSync(this.persistPath, 'utf-8'));
        for (const entry of data.entries ?? []) {
          this.entries.set(entry.id, entry);
        }
      }
    } catch { /* corrupt file — start fresh */ }
  }

  saveToDisk(): void {
    try {
      fs.mkdirSync(path.dirname(this.persistPath), { recursive: true });
      fs.writeFileSync(this.persistPath, JSON.stringify({
        version: 1,
        entries: [...this.entries.values()],
      }, null, 2));
    } catch { /* best-effort persistence */ }
  }

  apply(op: SkillbookOperation): SkillbookEntry | null {
    const now = Date.now();
    switch (op.type) {
      case 'ADD': {
        const id = `sk-${now}-${Math.random().toString(36).slice(2, 8)}`;
        const entry: SkillbookEntry = {
          ...op.entry, id, helpfulCount: 0, harmfulCount: 0, neutralCount: 0,
          occurrences: [], createdAt: now, updatedAt: now,
        };
        this.entries.set(id, entry);
        this.saveToDisk();
        return entry;
      }
      case 'TAG': {
        const entry = this.entries.get(op.id);
        if (!entry) return null;
        if (op.feedback === 'helpful') entry.helpfulCount++;
        else if (op.feedback === 'harmful') entry.harmfulCount++;
        else entry.neutralCount++;
        entry.updatedAt = now;
        this.saveToDisk();
        return entry;
      }
      case 'UPDATE': {
        const entry = this.entries.get(op.id);
        if (!entry) return null;
        if (op.insight) entry.insight = op.insight;
        if (op.keywords) entry.keywords = op.keywords;
        entry.updatedAt = now;
        this.saveToDisk();
        return entry;
      }
      case 'REMOVE': {
        const entry = this.entries.get(op.id);
        this.entries.delete(op.id);
        this.saveToDisk();
        return entry ?? null;
      }
    }
  }

  getAll(): SkillbookEntry[] { return [...this.entries.values()]; }

  getBySection(section: 'context' | 'harness'): SkillbookEntry[] {
    return this.getAll().filter(e => e.section === section);
  }

  search(query: string): SkillbookEntry[] {
    const terms = query.toLowerCase().split(/\s+/);
    return this.getAll().filter(e => {
      const text = `${e.issue} ${e.insight} ${e.keywords.join(' ')}`.toLowerCase();
      return terms.some(t => text.includes(t));
    }).sort((a, b) => b.helpfulCount - a.helpfulCount);
  }

  getContext(): string {
    const entries = this.getBySection('context').filter(e => e.helpfulCount > e.harmfulCount);
    if (entries.length === 0) return '';
    return '## Learned Strategies\n\n' + entries.map(e =>
      `- **${e.keywords.join(', ')}**: ${e.insight} (${e.helpfulCount} helpful)`
    ).join('\n');
  }
}
