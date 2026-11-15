export const ADMISSION_ACTIONS = ['register', 'connect', 'load', 'activate'] as const;
export type AdmissionAction = (typeof ADMISSION_ACTIONS)[number];
export const ADMISSION_OUTCOMES = ['allowed', 'denied', 'error'] as const;
export type AdmissionOutcome = (typeof ADMISSION_OUTCOMES)[number];
export const ADMISSION_SUBJECTS = ['tool', 'mcp_server', 'plugin', 'skill', 'hook'] as const;
export type AdmissionSubject = (typeof ADMISSION_SUBJECTS)[number];

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
  private _evicted = 0;

  constructor(maxEntries: number = 10_000) {
    this._maxEntries = maxEntries;
  }

  record(entry: Omit<AdmissionEntry, 'timestamp'>): AdmissionEntry {
    const full: AdmissionEntry = { ...entry, timestamp: Date.now() };
    this._entries.push(full);
    // Bounded FIFO ring buffer: once the cap is reached, evict the OLDEST
    // entries so the most recent (and most investigation-worthy) admission
    // decisions are always retained. Dropping the newest would permanently
    // freeze a full audit log on stale data, hiding the events right before
    // and after an incident.
    while (this._entries.length > this._maxEntries) {
      this._entries.shift();
      this._evicted++;
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

  /**
   * Number of oldest entries evicted by the bounded ring buffer since
   * construction (or the last clear()). A non-zero value signals the log
   * overflowed its cap and earlier admission decisions are no longer retained.
   */
  getEvictedCount(): number {
    return this._evicted;
  }

  clear(): void {
    this._entries = [];
    this._evicted = 0;
  }
}
