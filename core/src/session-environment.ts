/**
 * Persisted session environment script (the reference `sessionEnvironment.ts` parity).
 *
 * AMI's `bash` tool spawns every command with a freshly scrubbed environment and
 * only the static skill-env map carried across calls, so shell state — a
 * `source .venv/bin/activate`, a `conda activate`, an `export FOO=bar`, a `PATH`
 * edit — does NOT survive to the next, separate tool invocation. Any multi-step
 * flow that depends on an activated environment therefore silently degrades.
 *
 * This module assembles a single shell script from two sources:
 *   1. a parent-process-supplied env file (`CLAUDE_ENV_FILE` / `AMI_ENV_FILE`),
 *      e.g. a trajectory/sandbox runner that hands AMI a venv/conda activation
 *      script to honor;
 *   2. per-session hook env fragments written by lifecycle hooks into the
 *      session-env directory (`<configDir>/session-env/<sessionId>/`), named
 *      `<event>-hook-<index>.sh` so they order deterministically.
 *
 * The assembled script is prepended to each bash command so activation state
 * persists across stateless invocations. The result is cached tri-state
 * (undefined=unloaded / null=checked-empty / string=loaded) so disk is read at
 * most once per session; {@link invalidateSessionEnvCache} resets it (fired when
 * the cwd changes). Reads are ENOENT-tolerant — a missing/unreadable fragment
 * never aborts the pipeline — and Windows is unsupported (returns null).
 *
 * [FUNCTIONAL CHANGE] New capability — persisted shell env across bash calls.
 */
import { readdir, readFile, writeFile } from 'fs/promises';
import * as path from 'path';
import { log } from './logger';
import { resolveConfigDir } from './reference-journal';
import { assertPathWithin } from './path-safety';
import { isENOENT } from './utils/errno';

/**
 * DI seam for the host platform so the Windows-unsupported branch is testable on
 * any host (mirrors `utils/path.ts` `_pathHooks`). Production reads the real
 * `process.platform`; tests override it.
 */
export const _sessionEnvHooks: { platform: NodeJS.Platform } = { platform: process.platform };

/**
 * Tri-state cache:
 *   undefined = not yet loaded (check disk)
 *   null      = checked disk, nothing found (don't check again)
 *   string    = loaded and cached (reuse)
 * Keyed by the session id it was computed for, so a second session in the same
 * process recomputes rather than reusing a stale script.
 */
let sessionEnvScript: string | null | undefined = undefined;
let cachedSessionId: string | undefined = undefined;

/** Max session-id length mirrored from the journal id sanitizer. */
const MAX_SESSION_ID_SEGMENT = 200;

/** Collapse a session id to a single safe path segment (no separators / `..`). */
function safeSessionSegment(sessionId: string | undefined): string {
  const raw = (sessionId ?? '').trim();
  const safe = raw.replace(/[^A-Za-z0-9._-]/g, '_').replace(/\.\.+/g, '_');
  const trimmed = safe.slice(0, MAX_SESSION_ID_SEGMENT);
  return trimmed.length > 0 ? trimmed : 'default';
}

/**
 * The per-session env directory — `<configDir>/session-env/<sessionId>/` —
 * mirroring the reference's `<configHome>/session-env/<sessionId>/`. Pure string
 * build (no I/O), guarded to stay within the session-env base so a crafted
 * session id cannot escape. Exposed to lifecycle hooks (as an env var) so a hook
 * can write an `<event>-hook-<index>.sh` fragment that later bash commands pick up.
 */
export function sessionEnvDirPath(sessionId: string | undefined, env: NodeJS.ProcessEnv = process.env): string {
  const base = path.join(resolveConfigDir(env), 'session-env');
  const dir = path.join(base, safeSessionSegment(sessionId));
  assertPathWithin(dir, base);
  return dir;
}

const HOOK_ENV_PRIORITY: Record<string, number> = {
  setup: 0,
  sessionstart: 1,
  cwdchanged: 2,
  filechanged: 3,
};
const HOOK_ENV_REGEX = /^(setup|sessionstart|cwdchanged|filechanged)-hook-(\d+)\.sh$/;

/**
 * The deterministic per-hook env-fragment path a lifecycle hook may write
 * `export KEY=VAL` lines into to persist environment across later bash commands
 * (the reference's `hookIndex`-derived `CLAUDE_ENV_FILE` for SessionStart hooks).
 * Only the env-ingestible events (`setup`, `sessionStart`, `cwdChanged`,
 * `fileChanged` — the ones {@link getSessionEnvironmentScript} re-reads) get a
 * path; any other event returns null (writing one would never be re-ingested, so
 * handing a hook a dead path would be misleading). The event is lowercased to the
 * on-disk fragment naming (`<event>-hook-<index>.sh`) so the file sorts and
 * re-loads deterministically. The index keys the file per-hook so two hooks on
 * the same event do not clobber each other's exports.
 */
export function hookEnvFragmentPath(
  sessionId: string | undefined,
  event: string,
  index: number,
  env: NodeJS.ProcessEnv = process.env,
): string | null {
  const key = event.toLowerCase();
  if (!(key in HOOK_ENV_PRIORITY)) return null;
  if (!Number.isInteger(index) || index < 0) return null;
  const dir = sessionEnvDirPath(sessionId, env);
  const file = path.join(dir, `${key}-hook-${index}.sh`);
  assertPathWithin(file, dir);
  return file;
}

/**
 * Deterministic order for hook env fragments: first by event-type priority
 * (setup < sessionstart < cwdchanged < filechanged; unknown → 99), then by the
 * numeric hook index. Makes the concatenated env reproducible regardless of
 * `readdir` order. Exported so unit tests can drive the unknown-type / missing-
 * match arms that the production filter never feeds through.
 */
export function sortHookEnvFiles(a: string, b: string): number {
  const aMatch = a.match(HOOK_ENV_REGEX);
  const bMatch = b.match(HOOK_ENV_REGEX);
  const aType = aMatch?.[1] || '';
  const bType = bMatch?.[1] || '';
  if (aType !== bType) {
    return (HOOK_ENV_PRIORITY[aType] ?? 99) - (HOOK_ENV_PRIORITY[bType] ?? 99);
  }
  const aIndex = parseInt(aMatch?.[2] || '0', 10);
  const bIndex = parseInt(bMatch?.[2] || '0', 10);
  return aIndex - bIndex;
}

/** Normalize an unknown thrown value to a human-readable message for logs. Exported for coverage. */
export function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/**
 * Truncate only the cwd-scoped fragments (`filechanged-hook-*`,
 * `cwdchanged-hook-*`) on a cwd change, preserving session-wide `setup-` /
 * `sessionstart-` fragments. Prevents stale directory-specific env from leaking
 * into a new cwd while session env survives. ENOENT-tolerant (the dir may not
 * exist yet); other errors are logged, never thrown.
 */
export async function clearCwdEnvFiles(sessionId: string | undefined, env: NodeJS.ProcessEnv = process.env): Promise<void> {
  try {
    const dir = sessionEnvDirPath(sessionId, env);
    const files = await readdir(dir);
    await Promise.all(
      files
        .filter(
          (f) =>
            (f.startsWith('filechanged-hook-') || f.startsWith('cwdchanged-hook-')) &&
            HOOK_ENV_REGEX.test(f),
        )
        .map((f) => writeFile(path.join(dir, f), '')),
    );
  } catch (e: unknown) {
    if (!isENOENT(e)) {
      log('session-env', `Failed to clear cwd env files: ${errText(e)}`);
    }
  }
}

/** Reset the tri-state cache so the next read re-scans disk (fired on cwd change). */
export function invalidateSessionEnvCache(): void {
  log('session-env', 'Invalidating session environment cache');
  sessionEnvScript = undefined;
  cachedSessionId = undefined;
}

/**
 * Assemble (and cache) the shell script prepended to bash commands so env
 * mutations persist across separate invocations. Returns null when there is
 * nothing to prepend (or on Windows, which is unsupported).
 */
export async function getSessionEnvironmentScript(
  sessionId: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): Promise<string | null> {
  if (_sessionEnvHooks.platform === 'win32') {
    log('session-env', 'Session environment not yet supported on Windows');
    return null;
  }

  const effectiveId = safeSessionSegment(sessionId);
  if (sessionEnvScript !== undefined && cachedSessionId === effectiveId) {
    return sessionEnvScript;
  }

  const scripts: string[] = [];

  // 1. Parent-process env file (CLAUDE_ENV_FILE / AMI_ENV_FILE) — e.g. a
  //    trajectory/sandbox runner handing AMI a venv/conda activation script.
  //    Prepended FIRST so launcher activation wins. ENOENT is silent; other
  //    read errors are logged but never abort.
  const envFile = (env.AMI_ENV_FILE ?? env.CLAUDE_ENV_FILE)?.trim();
  if (envFile) {
    try {
      const envScript = (await readFile(envFile, 'utf8')).trim();
      if (envScript) {
        scripts.push(envScript);
        log('session-env', `Session environment loaded from env file: ${envFile} (${envScript.length} chars)`);
      }
    } catch (e: unknown) {
      if (!isENOENT(e)) {
        log('session-env', `Failed to read session env file: ${errText(e)}`);
      }
    }
  }

  // 2. Per-session hook env fragments, in deterministic order.
  const dir = sessionEnvDirPath(sessionId, env);
  try {
    const files = (await readdir(dir)).filter((f) => HOOK_ENV_REGEX.test(f)).sort(sortHookEnvFiles);
    for (const file of files) {
      const filePath = path.join(dir, file);
      try {
        const content = (await readFile(filePath, 'utf8')).trim();
        if (content) {
          scripts.push(content);
        }
      } catch (e: unknown) {
        if (!isENOENT(e)) {
          log('session-env', `Failed to read hook env file ${filePath}: ${errText(e)}`);
        }
      }
    }
    if (files.length > 0) {
      log('session-env', `Session environment loaded from ${files.length} hook file(s)`);
    }
  } catch (e: unknown) {
    if (!isENOENT(e)) {
      log('session-env', `Failed to load session environment from hooks: ${errText(e)}`);
    }
  }

  cachedSessionId = effectiveId;
  if (scripts.length === 0) {
    log('session-env', 'No session environment scripts found');
    sessionEnvScript = null;
    return sessionEnvScript;
  }

  sessionEnvScript = scripts.join('\n');
  log('session-env', `Session environment script ready (${sessionEnvScript.length} chars total)`);
  return sessionEnvScript;
}

/**
 * Read-only introspection of the persisted-session-environment ASSEMBLY policy —
 * the diagnostic/affordance surface a user needs to answer "why did (or didn't) my
 * venv/conda activation survive across bash calls?" without reading source. Pure
 * (reads the configured env + resolves the base dir path; no disk I/O, no spawn).
 *
 * Parity: the reference's `sessionEnvironment.ts` emits a chain of provenance
 * messages (`loaded from CLAUDE_ENV_FILE`, `loaded from N hook file(s)`, `script
 * ready`, `not yet supported on Windows`) as it assembles the env. AMI already
 * logs the same provenance via `log('session-env', …)` during assembly; this adds
 * the matching STATIC introspection — what AMI *would* source and from where —
 * so the capability is discoverable through `ami --introspect`
 * (introspection-on-every-change rule), complementing
 * {@link describeSubprocessEnvIsolation} (what is STRIPPED) with what is SOURCED.
 */
export function describeSessionEnvironment(env: NodeJS.ProcessEnv = process.env): {
  /** False on Windows (unsupported — `getSessionEnvironmentScript` returns null). */
  supported: boolean;
  /** Parent-process env-file vars honored, in precedence order (first wins). */
  envFileVars: string[];
  /** The env-file var that is currently set (precedence-resolved), or null. */
  envFileConfigured: string | null;
  /** Base dir under which per-session fragment dirs live (`<configDir>/session-env`). */
  sessionEnvBaseDir: string;
  /** Lifecycle events whose `<event>-hook-<n>.sh` fragments are sourced, in load order. */
  hookEvents: string[];
  /** On-disk fragment filename pattern a hook writes to persist exports. */
  fragmentNamePattern: string;
  /** Tri-state assembly cache states (per-session keyed). */
  cacheStates: string[];
  note: string;
} {
  const envFileVars = ['AMI_ENV_FILE', 'CLAUDE_ENV_FILE'];
  const envFileConfigured = envFileVars.find((v) => (env[v] ?? '').trim().length > 0) ?? null;
  return {
    supported: _sessionEnvHooks.platform !== 'win32',
    envFileVars,
    envFileConfigured,
    sessionEnvBaseDir: path.join(resolveConfigDir(env), 'session-env'),
    hookEvents: Object.keys(HOOK_ENV_PRIORITY).sort((a, b) => HOOK_ENV_PRIORITY[a] - HOOK_ENV_PRIORITY[b]),
    fragmentNamePattern: '<event>-hook-<index>.sh',
    cacheStates: ['unloaded', 'empty', 'loaded'],
    note: 'Assembled script (env file first, then deterministically-ordered hook fragments) is prepended to every bash command so venv/conda/export state persists across otherwise-stateless invocations; Windows unsupported, reads are ENOENT-tolerant. Complements subprocessEnvIsolation (what is stripped) with what is sourced.',
  };
}
