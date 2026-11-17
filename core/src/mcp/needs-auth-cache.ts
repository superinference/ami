/**
 * MCP "needs-auth" cache — Claude Code parity (services/mcp/client.ts:L257-316,
 * `mcp-needs-auth-cache.json`, `MCP_AUTH_CACHE_TTL_MS`).
 *
 * A remote (http/sse/ws) MCP server that returns a 401 — or that has been
 * OAuth-probed before but holds no usable token — is a *guaranteed-401* on the
 * next connect: re-probing it burns a network round-trip plus a full OAuth
 * discovery leg every startup for no benefit. Claude Code persists a small cache
 * keyed by server name recording the last time a server reported "needs auth",
 * and on each subsequent startup/connect pass SKIPS connecting any server whose
 * entry is still within a time-to-live, short-circuiting straight to a
 * `needs-auth` result.
 *
 * This module is the pure, side-effect-free core of that behaviour (the cache
 * shape + TTL logic + tolerant on-disk read/write). {@link McpManager} owns the
 * wiring: it records an entry when a remote connect fails with an auth-required
 * error, skips a cached server during {@link McpManager.connectAll}, and clears
 * an entry on a successful connect or an explicit re-auth
 * ({@link McpManager.reconnect}).
 *
 * Persistence is cross-session (a file in the config dir) precisely so a server
 * that needed auth in a prior run is short-circuited on the first connect of the
 * next run — matching Claude Code's `hasMcpDiscoveryButNoToken` short-circuit.
 */
import * as fs from 'fs';
import * as path from 'path';

/** Time-to-live for a needs-auth entry — Claude-parity `MCP_AUTH_CACHE_TTL_MS` (15 min). */
export const MCP_NEEDS_AUTH_CACHE_TTL_MS = 15 * 60 * 1000;

/** A single cache entry: the epoch-ms timestamp a server last reported needs-auth. */
export interface NeedsAuthCacheEntry {
  /** `Date.now()` at the moment the server returned 401 / had discovery-but-no-token. */
  ts: number;
}

/** The on-disk cache: a server-name → entry map. */
export type NeedsAuthCache = Record<string, NeedsAuthCacheEntry>;

/**
 * True when `name` has a cache entry whose age is within `ttlMs` of `now` — i.e.
 * a connect to it right now is a guaranteed 401 and should be skipped. A missing
 * or expired entry returns false (the server is re-probed).
 */
export function isNeedsAuthCached(
  cache: NeedsAuthCache,
  name: string,
  now: number,
  ttlMs: number = MCP_NEEDS_AUTH_CACHE_TTL_MS,
): boolean {
  const entry = cache[name];
  if (!entry) return false;
  return now - entry.ts < ttlMs;
}

/**
 * Return a NEW cache with `name` stamped at `now` (immutable update so a caller
 * can detect whether anything changed by identity/content).
 */
export function setNeedsAuthEntry(cache: NeedsAuthCache, name: string, now: number): NeedsAuthCache {
  return { ...cache, [name]: { ts: now } };
}

/**
 * Return a NEW cache with `name` removed. Returns the SAME reference when `name`
 * was absent, so a caller can skip a redundant disk write.
 */
export function clearNeedsAuthEntry(cache: NeedsAuthCache, name: string): NeedsAuthCache {
  if (!(name in cache)) return cache;
  const next = { ...cache };
  delete next[name];
  return next;
}

/**
 * Drop every entry older than `ttlMs` relative to `now`. Returns the SAME
 * reference when nothing was pruned (so the manager avoids a needless rewrite).
 */
export function pruneNeedsAuthCache(
  cache: NeedsAuthCache,
  now: number,
  ttlMs: number = MCP_NEEDS_AUTH_CACHE_TTL_MS,
): NeedsAuthCache {
  let changed = false;
  const next: NeedsAuthCache = {};
  for (const [name, entry] of Object.entries(cache)) {
    if (now - entry.ts < ttlMs) next[name] = entry;
    else changed = true;
  }
  return changed ? next : cache;
}

/**
 * Read + parse the cache file, tolerating a missing file, unreadable file, or
 * malformed JSON (any of which yields an empty cache — a corrupt cache must never
 * block connecting). Non-object entries and non-numeric timestamps are dropped so
 * a hand-edited file cannot inject a bogus shape downstream.
 */
export function readNeedsAuthCache(filePath: string): NeedsAuthCache {
  let raw: string;
  try {
    raw = fs.readFileSync(filePath, 'utf8');
  } catch {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
  const out: NeedsAuthCache = {};
  for (const [name, value] of Object.entries(parsed as Record<string, unknown>)) {
    const ts = (value as { ts?: unknown } | null)?.ts;
    if (typeof ts === 'number' && Number.isFinite(ts)) out[name] = { ts };
  }
  return out;
}

/**
 * Atomically persist the cache (write to a temp sibling + rename) so a crash
 * mid-write cannot leave a truncated/corrupt file. Best-effort: a write failure
 * (read-only FS, missing parent that cannot be created) is swallowed — the cache
 * is an optimization, never a correctness requirement.
 */
export function writeNeedsAuthCache(filePath: string, cache: NeedsAuthCache): void {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    const tmp = `${filePath}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(cache), 'utf8');
    fs.renameSync(tmp, filePath);
  } catch {
    /* best-effort: a persistence failure must not break connecting */
  }
}
