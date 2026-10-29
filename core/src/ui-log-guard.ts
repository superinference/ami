export const LOG_TIERS = ['builtin', 'prepend', 'user', 'append', 'core', 'engine'] as const;
export type LogTier = (typeof LOG_TIERS)[number];

export interface LogEntry {
  message: string;
  tier: LogTier;
  source: string;
  timestamp: number;
  level: 'debug' | 'info' | 'warn' | 'error';
}

export function isProtectedLogTier(tier: LogTier): boolean {
  return tier === 'prepend' || tier === 'append' || tier === 'builtin' || tier === 'engine';
}

export function canModifyLog(logTier: LogTier, modifierTier: LogTier): boolean {
  if (!isProtectedLogTier(logTier)) return true;
  const tierOrder: Record<LogTier, number> = {
    engine: 0,
    builtin: 1,
    prepend: 2,
    user: 3,
    append: 4,
    core: 5,
  };
  return tierOrder[modifierTier] <= tierOrder[logTier];
}

export class UILogGuard {
  private _logs: LogEntry[] = [];
  private _maxLogs: number;
  private _suppressed = 0;

  constructor(maxLogs: number = 50_000) {
    this._maxLogs = maxLogs;
  }

  addLog(message: string, tier: LogTier, source: string, level: LogEntry['level'] = 'info'): LogEntry {
    const entry: LogEntry = { message, tier, source, timestamp: Date.now(), level };
    if (this._logs.length < this._maxLogs) {
      this._logs.push(entry);
    } else {
      this._suppressed++;
    }
    return entry;
  }

  checkModification(logIndex: number, modifierTier: LogTier): boolean {
    const entry = this._logs[logIndex];
    if (!entry) return false;
    return canModifyLog(entry.tier, modifierTier);
  }

  getLogsByTier(tier: LogTier): LogEntry[] {
    return this._logs.filter(e => e.tier === tier);
  }

  getLogsByLevel(level: LogEntry['level']): LogEntry[] {
    return this._logs.filter(e => e.level === level);
  }

  getLogsBySource(source: string): LogEntry[] {
    return this._logs.filter(e => e.source === source);
  }

  getLogs(): readonly LogEntry[] {
    return this._logs;
  }

  getLogCount(): number {
    return this._logs.length;
  }

  getSuppressedCount(): number {
    return this._suppressed;
  }

  clear(): void {
    this._logs = [];
    this._suppressed = 0;
  }
}
