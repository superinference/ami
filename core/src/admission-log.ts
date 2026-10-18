export type AdmissionAction = 'register' | 'connect' | 'load' | 'activate';
export type AdmissionOutcome = 'allowed' | 'denied' | 'error';
export type AdmissionSubject = 'tool' | 'mcp_server' | 'plugin' | 'skill' | 'hook';

export interface AdmissionEntry {
  timestamp: number;
  subject: AdmissionSubject;
  action: AdmissionAction;
  name: string;
  source: string;
  outcome: AdmissionOutcome;
  reason?: string;
  metadata?: Record<string, unknown>;
}

export class AdmissionLog {
  private _entries: AdmissionEntry[] = [];
  private _maxEntries: number;

  constructor(maxEntries: number = 10_000) {
    this._maxEntries = maxEntries;
  }

  record(entry: Omit<AdmissionEntry, 'timestamp'>): AdmissionEntry {
    const full: AdmissionEntry = { ...entry, timestamp: Date.now() };
    if (this._entries.length < this._maxEntries) {
      this._entries.push(full);
    }
    return full;
  }

  allowed(subject: AdmissionSubject, action: AdmissionAction, name: string, source: string): AdmissionEntry {
    return this.record({ subject, action, name, source, outcome: 'allowed' });
  }

  denied(subject: AdmissionSubject, action: AdmissionAction, name: string, source: string, reason: string): AdmissionEntry {
    return this.record({ subject, action, name, source, outcome: 'denied', reason });
  }

  error(subject: AdmissionSubject, action: AdmissionAction, name: string, source: string, reason: string): AdmissionEntry {
    return this.record({ subject, action, name, source, outcome: 'error', reason });
  }

  getEntries(): readonly AdmissionEntry[] {
    return this._entries;
  }

  getEntriesByOutcome(outcome: AdmissionOutcome): AdmissionEntry[] {
    return this._entries.filter(e => e.outcome === outcome);
  }

  getEntriesBySubject(subject: AdmissionSubject): AdmissionEntry[] {
    return this._entries.filter(e => e.subject === subject);
  }

  getEntriesByName(name: string): AdmissionEntry[] {
    return this._entries.filter(e => e.name === name);
  }

  getDeniedCount(): number {
    return this._entries.filter(e => e.outcome === 'denied').length;
  }

  getErrorCount(): number {
    return this._entries.filter(e => e.outcome === 'error').length;
  }

  getSummary(): Record<AdmissionOutcome, number> {
    const summary: Record<AdmissionOutcome, number> = { allowed: 0, denied: 0, error: 0 };
    for (const e of this._entries) summary[e.outcome]++;
    return summary;
  }

  clear(): void {
    this._entries = [];
  }
}
