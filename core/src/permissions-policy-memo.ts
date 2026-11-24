// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/**
 * True when `command` is covered by a rule prefix (the reference's `:*` convention): the
 * command equals the prefix exactly, or starts with the prefix followed by a space
 * (its arguments). Anchored on a word boundary so `ls:*` matches `ls -la` and bare
 * `ls` but NOT `lsof` (a different command that merely shares the leading letters).
 */
export function commandHasRulePrefix(command: string, prefix: string): boolean {
  // Callers only invoke this when `permissionRuleExtractPrefix` returned a non-null
  // prefix (which already requires `prefix.length > 0`), so an empty-prefix guard
  // was unreachable.
  const c = command.trim();
  return c === prefix || c.startsWith(prefix + ' ');
}

/** Strip single-quoted spans — in bash their contents are fully literal (no
 *  `$`/backtick/`$(…)` expansion), so a `$SECRET` inside `'…'` is inert. Double
 *  quotes are kept because they DO expand. Single quotes cannot be escaped in
 *  bash, so no backslash handling is needed. */
export function stripSingleQuotedSpans(s: string): string {
  let out = '';
  let inSingle = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    if (c === "'") { inSingle = !inSingle; continue; }
    if (!inSingle) out += c;
  }
  return out;
}

/**
 * Build a denial key from a tool name and its input for deduplication.
 */
export function denialKey(toolName: string, input: Record<string, unknown>): string {
  const inputStr = typeof input.command === 'string'
    ? input.command
    : typeof input.file_path === 'string'
      ? input.file_path
      : JSON.stringify(input);
  return `${toolName}::${inputStr}`;
}

// ---------------------------------------------------------------------------
// PolicyMemo — memoize permission decisions across bursts of tool calls
// ---------------------------------------------------------------------------

export const POLICY_MEMO_MAX_ENTRIES = 1000;

export interface MemoEntry {
  decision: 'allow' | 'deny' | 'ask';
  expiresAt: number;
  lastAccess: number;
}

export class PolicyMemo {
  private readonly _ttlMs: number;
  private readonly _cache = new Map<string, MemoEntry>();

  constructor(ttlMs: number = 5000) {
    this._ttlMs = ttlMs;
  }

  get(toolName: string, inputHash: string): 'allow' | 'deny' | 'ask' | null {
    const key = `${toolName}:${inputHash}`;
    const entry = this._cache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this._cache.delete(key);
      return null;
    }
    entry.lastAccess = Date.now();
    return entry.decision;
  }

  set(toolName: string, inputHash: string, decision: 'allow' | 'deny' | 'ask'): void {
    const key = `${toolName}:${inputHash}`;
    this._cache.set(key, {
      decision,
      expiresAt: Date.now() + this._ttlMs,
      lastAccess: Date.now(),
    });
    if (this._cache.size > POLICY_MEMO_MAX_ENTRIES) {
      this._evictLRU();
    }
  }

  clear(): void {
    this._cache.clear();
  }

  size(): number {
    return this._cache.size;
  }

  prune(): number {
    const now = Date.now();
    let removed = 0;
    for (const [key, entry] of this._cache) {
      if (now > entry.expiresAt) {
        this._cache.delete(key);
        removed++;
      }
    }
    return removed;
  }

  private _evictLRU(): void {
    let oldestKey: string | null = null;
    let oldestAccess = Infinity;
    for (const [key, entry] of this._cache) {
      if (entry.lastAccess < oldestAccess) {
        oldestAccess = entry.lastAccess;
        oldestKey = key;
      }
    }
    if (oldestKey) this._cache.delete(oldestKey);
  }
}
