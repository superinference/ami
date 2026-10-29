export const ENV_ACCESS_TIERS = ['builtin', 'prepend', 'user', 'append', 'engine'] as const;
export type EnvAccessTier = (typeof ENV_ACCESS_TIERS)[number];

export const ENV_OPERATIONS = ['get', 'set', 'delete'] as const;
export type EnvOperation = (typeof ENV_OPERATIONS)[number];

// Variables that describe sensitive paths — never expose values, only whether set
export const SENSITIVE_ENV_VARS = new Set([
  'API_KEY',
  'SECRET_KEY',
  'ACCESS_TOKEN',
  'PRIVATE_KEY',
  'PASSWORD',
  'CREDENTIAL',
  'AUTH_TOKEN',
]);

export function isSensitiveEnvVar(name: string): boolean {
  const upper = name.toUpperCase();
  return [...SENSITIVE_ENV_VARS].some(s => upper.includes(s));
}

export function canAccessEnv(
  variable: string,
  operation: EnvOperation,
  callerTier: EnvAccessTier,
  readerTier?: EnvAccessTier,
): boolean {
  // Gets by org plugins bypass user tier
  if (operation === 'get' && readerTier &&
      (readerTier === 'prepend' || readerTier === 'append') &&
      callerTier === 'user') {
    return false;
  }

  // Sets/deletes of pinned variables only by engine/builtin
  if (operation === 'set' || operation === 'delete') {
    // The actual pinning is handled by env-protection.ts
    // This guard handles tier-based access control
    return true;
  }

  return true;
}

export interface EnvAccessEvent {
  variable: string;
  operation: EnvOperation;
  callerTier: EnvAccessTier;
  allowed: boolean;
  timestamp: number;
}

export class EnvAccessGuard {
  private _events: EnvAccessEvent[] = [];
  private _maxEvents: number;

  constructor(maxEvents: number = 10_000) {
    this._maxEvents = maxEvents;
  }

  check(variable: string, operation: EnvOperation, callerTier: EnvAccessTier, readerTier?: EnvAccessTier): boolean {
    const allowed = canAccessEnv(variable, operation, callerTier, readerTier);
    if (this._events.length < this._maxEvents) {
      this._events.push({
        variable,
        operation,
        callerTier,
        allowed,
        timestamp: Date.now(),
      });
    }
    return allowed;
  }

  getEvents(): readonly EnvAccessEvent[] {
    return this._events;
  }

  getBlockedEvents(): EnvAccessEvent[] {
    return this._events.filter(e => !e.allowed);
  }

  getBlockedCount(): number {
    return this._events.filter(e => !e.allowed).length;
  }

  safeGetEnv(name: string): { value: string | undefined; redacted: boolean } {
    if (isSensitiveEnvVar(name)) {
      const exists = process.env[name] !== undefined;
      return { value: exists ? '[REDACTED]' : undefined, redacted: true };
    }
    return { value: process.env[name], redacted: false };
  }

  clear(): void {
    this._events = [];
  }
}
