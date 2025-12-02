export type SelectionStrategy = 'fill_first' | 'round_robin' | 'least_used';

export interface CredentialEntry {
  id: string;
  apiKey: string;
  label?: string;
  status: 'ok' | 'exhausted' | 'dead';
  cooldownUntil: number;
  usageCount: number;
  lastUsed: number;
}

export class CredentialPool {
  private entries: CredentialEntry[] = [];
  private strategy: SelectionStrategy;
  private roundRobinIndex = 0;

  constructor(strategy: SelectionStrategy = 'fill_first') {
    this.strategy = strategy;
  }

  addKey(apiKey: string, label?: string): void {
    this.entries.push({
      id: `key-${this.entries.length}`,
      apiKey,
      label,
      status: 'ok',
      cooldownUntil: 0,
      usageCount: 0,
      lastUsed: 0,
    });
  }

  acquire(): { apiKey: string; id: string } | null {
    const now = Date.now();
    const available = this.entries.filter(
      e => e.status !== 'dead' && (e.status === 'ok' || now >= e.cooldownUntil),
    );

    for (const e of available) {
      if (e.status === 'exhausted' && now >= e.cooldownUntil) {
        e.status = 'ok';
      }
    }

    const ok = available.filter(e => e.status === 'ok');
    if (ok.length === 0) return null;

    let selected: CredentialEntry;

    switch (this.strategy) {
      case 'round_robin':
        this.roundRobinIndex = this.roundRobinIndex % ok.length;
        selected = ok[this.roundRobinIndex]!;
        this.roundRobinIndex++;
        break;
      case 'least_used':
        selected = ok.sort((a, b) => a.usageCount - b.usageCount)[0]!;
        break;
      case 'fill_first':
      default:
        selected = ok[0]!;
        break;
    }

    selected.usageCount++;
    selected.lastUsed = now;

    return { apiKey: selected.apiKey, id: selected.id };
  }

  markExhausted(id: string, cooldownMs: number = 60000): void {
    const entry = this.entries.find(e => e.id === id);
    if (entry) {
      entry.status = 'exhausted';
      entry.cooldownUntil = Date.now() + cooldownMs;
    }
  }

  markDead(id: string): void {
    const entry = this.entries.find(e => e.id === id);
    if (entry) {
      entry.status = 'dead';
    }
  }

  get size(): number {
    return this.entries.length;
  }

  get availableCount(): number {
    const now = Date.now();
    return this.entries.filter(
      e => e.status !== 'dead' && (e.status === 'ok' || now >= e.cooldownUntil),
    ).length;
  }
}
