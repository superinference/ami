export const SELECTION_STRATEGIES = ['fill_first', 'round_robin', 'least_used'] as const;
export type SelectionStrategy = (typeof SELECTION_STRATEGIES)[number];

/** Default fallback cooldown applied to an exhausted credential when no
 *  server-directed hint is supplied (named so it is not an inline magic number). */
export const DEFAULT_COOLDOWN_MS = 60000;
/** Upper clamp for any cooldown. Server-provided durations (Retry-After /
 *  reset timestamps) are untrusted, so a hostile or misconfigured endpoint
 *  cannot pin a credential far into the future (self-inflicted DoS defense). */
export const MAX_COOLDOWN_MS = 3600000; // 1 hour

export interface CredentialEntry {
  id: string;
  apiKey: string;
  label?: string;
  status: 'ok' | 'exhausted' | 'dead';
  cooldownUntil: number;
  usageCount: number;
  lastUsed: number;
  /** Number of back-to-back exhaustions since the last successful use.
   *  Drives exponential backoff; reset to 0 by markSuccess(). */
  consecutiveExhaustions: number;
}

/** Options for {@link CredentialPool.markExhausted}. All fields are optional and
 *  additive — passing a bare number is still supported for backward compatibility. */
export interface MarkExhaustedOptions {
  /** Base cooldown in ms (before escalation). Defaults to DEFAULT_COOLDOWN_MS. */
  cooldownMs?: number;
  /** Server-directed relative delay (e.g. parsed Retry-After). Authoritative:
   *  used verbatim (clamped) instead of the escalating fallback. */
  retryAfterMs?: number;
  /** Absolute server-provided reset epoch (ms). Authoritative: cooldown runs
   *  until this instant (clamped), immune to local wait-time guessing. */
  resetAt?: number;
  /** Clamp ceiling for the computed cooldown. Defaults to MAX_COOLDOWN_MS. */
  maxCooldownMs?: number;
  /** Bounded random jitter (ms) added on top of the cooldown to spread
   *  simultaneous recoveries (thundering-herd defense). Defaults to 0 (off). */
  jitterMs?: number;
  /** When true (default) the fallback cooldown escalates exponentially with
   *  consecutiveExhaustions. Ignored when retryAfterMs/resetAt is given. */
  escalate?: boolean;
  /** Injectable RNG in [0,1) for deterministic jitter in tests. Defaults to Math.random. */
  rng?: () => number;
}

/** Replace a secret value with an opaque marker for serialization/inspection. */
const REDACTED = '[REDACTED]';

export class CredentialPool {
  private entries: CredentialEntry[] = [];
  private strategy: SelectionStrategy;
  private roundRobinIndex = 0;

  constructor(strategy: SelectionStrategy = 'fill_first') {
    this.strategy = strategy;
  }

  addKey(apiKey: string, label?: string): void {
    // Reject junk credentials before they enter the secret store: an empty or
    // whitespace-only value would later be handed out as an auth header and
    // produce confusing downstream failures.
    const trimmed = apiKey.trim();
    if (trimmed.length === 0) {
      throw new Error('CredentialPool.addKey: apiKey must be a non-empty string');
    }
    // Format guard: a credential is later placed verbatim into an Authorization /
    // x-api-key HTTP header. A value with internal whitespace or a control
    // character (newline, CR, tab, NUL, DEL, ...) can never be a valid header
    // value and is a header-injection / request-corruption risk — reject it
    // instead of admitting structurally-impossible key material to the store.
    // eslint-disable-next-line no-control-regex -- intentional control-char match on untrusted key material
    if (/[\s\u0000-\u001f\u007f]/.test(trimmed)) {
      throw new Error('CredentialPool.addKey: apiKey contains invalid characters');
    }
    this.entries.push({
      id: `key-${this.entries.length}`,
      apiKey: trimmed,
      label,
      status: 'ok',
      cooldownUntil: 0,
      usageCount: 0,
      lastUsed: 0,
      consecutiveExhaustions: 0,
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

  markExhausted(id: string, options?: number | MarkExhaustedOptions): void {
    const entry = this.entries.find(e => e.id === id);
    if (!entry) return;

    const opts: MarkExhaustedOptions =
      typeof options === 'number' ? { cooldownMs: options } : (options ?? {});

    const now = Date.now();
    entry.status = 'exhausted';
    entry.consecutiveExhaustions += 1;

    const maxCooldown = opts.maxCooldownMs ?? MAX_COOLDOWN_MS;

    let base: number;
    if (typeof opts.resetAt === 'number') {
      // Absolute server-provided reset timestamp wins (clock-drift immune).
      base = opts.resetAt - now;
    } else if (typeof opts.retryAfterMs === 'number') {
      // Server-directed relative delay wins over the local guess.
      base = opts.retryAfterMs;
    } else {
      const cooldown = opts.cooldownMs ?? DEFAULT_COOLDOWN_MS;
      base =
        opts.escalate === false
          ? cooldown
          : // Exponential escalation: a chronically-throttled key is retried
            // progressively less often instead of at the same cadence forever.
            cooldown * 2 ** (entry.consecutiveExhaustions - 1);
    }

    // Clamp into [0, maxCooldown] so untrusted/huge values cannot pin the key.
    base = Math.min(Math.max(0, base), maxCooldown);

    const jitterMs = opts.jitterMs ?? 0;
    const rng = opts.rng ?? Math.random;
    const jitter = jitterMs > 0 ? Math.floor(rng() * jitterMs) : 0;

    entry.cooldownUntil = now + base + jitter;
  }

  /** Record that a credential was used successfully, clearing its escalation
   *  streak so a later exhaustion starts the backoff from the base again. */
  markSuccess(id: string): void {
    const entry = this.entries.find(e => e.id === id);
    if (entry) {
      entry.consecutiveExhaustions = 0;
      if (entry.status === 'exhausted') {
        entry.status = 'ok';
        entry.cooldownUntil = 0;
      }
    }
  }

  markDead(id: string): void {
    const entry = this.entries.find(e => e.id === id);
    if (entry) {
      entry.status = 'dead';
    }
  }

  /** Earliest epoch (ms) at which acquire() could succeed, so a caller can wait
   *  for the reset instead of hammering an exhausted pool. Returns now when a
   *  credential is already available, or null when every credential is dead. */
  nextAvailableAt(): number | null {
    const now = Date.now();
    const alive = this.entries.filter(e => e.status !== 'dead');
    if (alive.length === 0) return null;
    const availableNow = alive.some(e => e.status === 'ok' || now >= e.cooldownUntil);
    if (availableNow) return now;
    return Math.min(...alive.map(e => e.cooldownUntil));
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

  /** Serialization guard: never emit raw key material. Any JSON.stringify(pool)
   *  or structured clone masks every apiKey to [REDACTED]. */
  toJSON(): { strategy: SelectionStrategy; entries: CredentialEntry[] } {
    return {
      strategy: this.strategy,
      entries: this.entries.map(e => ({ ...e, apiKey: REDACTED })),
    };
  }

  /** Inspection guard: console.log(pool) / util.inspect(pool) mask secrets too. */
  [Symbol.for('nodejs.util.inspect.custom')](): {
    strategy: SelectionStrategy;
    entries: CredentialEntry[];
  } {
    return this.toJSON();
  }
}
