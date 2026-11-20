/**
 * Session-scoped environment variables set by the user via the `/env` slash
 * command. These are applied ONLY to child processes spawned by the `bash`
 * tool (merged into the subprocess env) — never to the REPL/engine process
 * itself. This mirrors the reference's `sessionEnvVars` affordance
 * (src/utils/sessionEnvVars.ts) where `/env NAME=VALUE` overrides are injected
 * into the shell provider's `getEnvironmentOverrides()` for spawned commands.
 *
 * Distinct from the hook/env-file captured "session environment" (see
 * {@link ./session-environment}), which is a shell *script* sourced into the
 * command string so activation state (venv/conda/exports) survives across
 * stateless bash calls. These `/env` vars are plain spawn-env overrides from an
 * explicit user action, so they are kept in their own store and applied at the
 * spawn boundary.
 *
 * The store is keyed by session id so concurrent sessions and forked sub-agents
 * cannot leak vars into each other. An absent/empty session id falls back to a
 * shared default bucket (bare engines and tests with no session id).
 */

/** Valid POSIX-ish shell environment variable name: letter/underscore start,
 *  then letters/digits/underscores. Rejecting anything else keeps a user value
 *  from being usable as a shell-injection vector when exported into a child. */
const ENV_VAR_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

const DEFAULT_BUCKET = '__default__';

/** sessionId → (NAME → VALUE). Lazily created per session. */
const store = new Map<string, Map<string, string>>();

const EMPTY: ReadonlyMap<string, string> = new Map();

function bucketKey(sessionId?: string): string {
  return sessionId && sessionId.length > 0 ? sessionId : DEFAULT_BUCKET;
}

/** True when `name` is a valid environment variable identifier. */
export function isValidEnvVarName(name: string): boolean {
  return ENV_VAR_NAME_RE.test(name);
}

/** Read-only view of the session's user-set env vars (never mutate the engine's
 *  own environment). Returns an empty map when the session has none. */
export function getSessionEnvVars(sessionId?: string): ReadonlyMap<string, string> {
  return store.get(bucketKey(sessionId)) ?? EMPTY;
}

/** Plain-object snapshot of the session's user-set env vars, suitable for
 *  merging into a spawn env record. */
export function sessionEnvOverrides(sessionId?: string): Record<string, string> {
  const m = store.get(bucketKey(sessionId));
  if (!m || m.size === 0) return {};
  return Object.fromEntries(m);
}

/** Outcome of a {@link setSessionEnvVar} call. */
export interface SetSessionEnvVarResult {
  ok: boolean;
  name?: string;
  value?: string;
  /** Present when `ok` is false — a human-readable reason. */
  error?: string;
}

/** Set a session-scoped env var. Rejects an invalid variable name. */
export function setSessionEnvVar(
  name: string,
  value: string,
  sessionId?: string,
): SetSessionEnvVarResult {
  if (!isValidEnvVarName(name)) {
    return {
      ok: false,
      name,
      error: `invalid variable name "${name}" (must match [A-Za-z_][A-Za-z0-9_]*)`,
    };
  }
  const key = bucketKey(sessionId);
  let m = store.get(key);
  if (!m) {
    m = new Map();
    store.set(key, m);
  }
  m.set(name, value);
  return { ok: true, name, value };
}

/** Remove a single session-scoped env var. Returns true if it existed. */
export function deleteSessionEnvVar(name: string, sessionId?: string): boolean {
  const key = bucketKey(sessionId);
  const m = store.get(key);
  if (!m) return false;
  const existed = m.delete(name);
  if (m.size === 0) store.delete(key);
  return existed;
}

/** Clear all session-scoped env vars for a session. Returns how many were removed. */
export function clearSessionEnvVars(sessionId?: string): number {
  const key = bucketKey(sessionId);
  const m = store.get(key);
  if (!m) return 0;
  const n = m.size;
  store.delete(key);
  return n;
}

/** Parse a `NAME=VALUE` assignment token. A bare `NAME=` yields an empty value.
 *  Returns null when there is no `=` or the name is invalid. The value keeps any
 *  further `=` characters (e.g. `FOO=a=b` → value `a=b`). */
export function parseEnvAssignment(
  token: string,
): { name: string; value: string } | null {
  const eq = token.indexOf('=');
  if (eq <= 0) return null;
  const name = token.slice(0, eq);
  if (!isValidEnvVarName(name)) return null;
  return { name, value: token.slice(eq + 1) };
}
