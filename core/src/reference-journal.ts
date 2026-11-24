/**
 * reference-journal — persist per-session agent telemetry as append-only JSONL under the
 * user's data dir, using the SAME on-disk conventions a reference-style analytics
 * dashboard reads (projects/<cwd-slug>/<sessionId>.jsonl with `assistant`/`user` records).
 *
 * This is the WRITE side only: pure path/schema/mapping helpers plus a crash-safe append.
 * It imports nothing from common/cli/vscode (it is a core building block). The read/aggregate
 * side lives in common/src/usage-journal-reader.ts.
 *
 * ALIGNMENT INVARIANTS (so an external dashboard can price + group our data out of the box):
 *  - directory layout: <configDir>/projects/<projectSlug(cwd)>/<sessionId>.jsonl
 *  - every line has a top-level `type` + ISO `timestamp`
 *  - `version` is a semver >= COMPAT_VERSION (so sidechain usage is NOT estimated away)
 *  - usage is emitted in snake_case API fields; NO cost field is ever written (the reader
 *    re-prices from model + token counts — writing cost would double-count / drift)
 *  - tool names + model ids are mapped into the dashboard's own vocabulary
 */

import 'os';
import * as path from 'path';
import * as fs from 'fs';
import 'crypto';
import 'child_process';
import { assertPathWithin } from './path-safety';
import { atomicWriteFileSync, secureMkdirSync, OWNER_ONLY_FILE_MODE } from './utils/fs-atomic';
import { memoizeWithLRU } from './utils/memoize';
import { log, logError } from './logger';
import { getGlobalConfigPath, updateConfigFile, loadConfigFileChecked, stripBom, runConfigKeyMigrations } from './config';

import { CURRENT_MIGRATION_VERSION, GIT_ROOT_CACHE_MAX, expandConfigDir, isDangerousStorageRoot, migrateLegacyDataDir, resolveHome, resolveStorageOverride } from './reference-journal-persist-positive-capability';
import type { ModelAliasMigration, StartupMigrationResult } from './reference-journal-persist-positive-capability';
import { AMI_TO_CLAUDE_TOOL, projectSlug, resolveGitRootUncached, sanitizeId } from './reference-journal-build-usage-fields';
import type { UserEntry, UserEntryArgs } from './reference-journal-build-usage-fields';
export { COMPAT_VERSION, CURRENT_MIGRATION_VERSION, MAX_SESSION_ID_LENGTH, MAX_SLUG_LENGTH, canonicalizePath, expandConfigDir, formatStartupMigrationNotices, getUserBinDir, isDangerousStorageRoot, isDirOnPath, migrateLegacyDataDir, persistPositiveCapability, readCachedCapability, resolveHome } from './reference-journal-persist-positive-capability';
export type { ModelAliasMigration, StartupMigrationNoticeOptions, StartupMigrationResult } from './reference-journal-persist-positive-capability';
export { AMI_TO_CLAUDE_TOOL, AMI_TO_REFERENCE_MODEL, buildAssistantEntry, buildUsageFields, projectSlug, toReferenceModelId, toReferenceToolName } from './reference-journal-build-usage-fields';
export type { AssistantEntry, AssistantEntryArgs, JournalContentBlock, JournalUsage, JournalUsageInput, StructuredPatchHunk, UserEntry, UserEntryArgs } from './reference-journal-build-usage-fields';

/**
 * Env-keyed memo for {@link resolveConfigDir}: this is on many hot paths (every
 * journal line, checkpoint, memory read). Keyed off the inputs that determine the
 * result (the two override env vars, HOME, and any injected homedir) so a changed
 * env value refreshes automatically without a manual cache clear — parity with
 * envUtils.ts's memoized `getClaudeConfigHomeDir` (keyed off CLAUDE_CONFIG_DIR).
 */
const _configDirCache = new Map<string, string>();

/**
 * Resolve the data dir the journal is written under, matching the dashboard's own
 * precedence: a validated CLAUDE_CONFIG_DIR wins, then SUPERINFERENCE_DATA_DIR,
 * else `~/.ami`. We never write into `~/.claude`. Overrides are tilde-expanded
 * (gh-30794), rejected if dangerous ({@link isDangerousStorageRoot}) and
 * NFC-normalized; the default is resolved against {@link resolveHome} (HOME-aware)
 * and NFC-normalized. `homedir` is an injectable test seam. Memoized per env.
 */
export function resolveConfigDir(env: NodeJS.ProcessEnv = process.env, homedir?: string): string {
  const cacheKey = JSON.stringify([env.CLAUDE_CONFIG_DIR, env.SUPERINFERENCE_DATA_DIR, env.HOME, homedir]);
  const cached = _configDirCache.get(cacheKey);
  if (cached !== undefined) return cached;
  const override = resolveStorageOverride(env, homedir);
  const resolved = override ?? path.join(resolveHome(env, homedir), '.ami').normalize('NFC');
  _configDirCache.set(cacheKey, resolved);
  return resolved;
}

/** Env-keyed memo for {@link resolveCacheDir} (see {@link _configDirCache}). */
const _cacheDirCache = new Map<string, string>();

/**
 * Resolve the TRANSIENT cache dir for discardable diagnostics (per-server MCP logs,
 * error traces) as a config knob distinct from the durable data dir — parity with
 * the reference's `cachePaths` (`envPaths('claude-cli').cache`, which honors
 * `XDG_CACHE_HOME` and the platform cache root). Precedence:
 *  1. An explicit storage override (CLAUDE_CONFIG_DIR / SUPERINFERENCE_DATA_DIR)
 *     wins and keeps cache co-located with data, so a user/test that pins one dir
 *     gets everything in it (deterministic).
 *  2. Else `XDG_CACHE_HOME` when set + non-dangerous ({@link isDangerousStorageRoot})
 *     → `<XDG_CACHE_HOME>/ami`, honoring the XDG Base Directory spec so transient
 *     cache lives under the platform cache root, separate from durable state.
 *  3. Else the durable data dir ({@link resolveConfigDir}) — cache and data share
 *     one root (prior behaviour; no XDG env present).
 * Overrides are tilde-expanded (gh-30794) and NFC-normalized. Memoized per env;
 * `homedir` is an injectable test seam.
 */
export function resolveCacheDir(env: NodeJS.ProcessEnv = process.env, homedir?: string): string {
  const cacheKey = JSON.stringify([
    env.CLAUDE_CONFIG_DIR, env.SUPERINFERENCE_DATA_DIR, env.XDG_CACHE_HOME, env.HOME, homedir,
  ]);
  const cached = _cacheDirCache.get(cacheKey);
  if (cached !== undefined) return cached;
  let resolved: string;
  const override = resolveStorageOverride(env, homedir);
  if (override) {
    resolved = override;
  } else {
    const xdg = env.XDG_CACHE_HOME;
    if (xdg && xdg.trim()) {
      const expanded = expandConfigDir(xdg, env, homedir);
      if (isDangerousStorageRoot(expanded)) {
        logError('resolve-cache-dir', 'rejecting dangerous XDG_CACHE_HOME override', { value: xdg });
        resolved = resolveConfigDir(env, homedir);
      } else {
        resolved = path.join(expanded.normalize('NFC'), 'ami');
      }
    } else {
      resolved = resolveConfigDir(env, homedir);
    }
  }
  _cacheDirCache.set(cacheKey, resolved);
  return resolved;
}

/**
 * One-time, idempotent startup migration of a persisted, DEPRECATED/renamed pinned
 * `model` string in the GLOBAL (user-level) settings to its current successor id —
 * so the settings file, `/model`, and downstream pricing all reflect a live model
 * instead of a retired one (parity: the reference's migrateLegacyOpusToCurrent /
 * migrateFennecToOpus settings-value migrations). Runs alongside
 * {@link migrateLegacyDataDir} at startup.
 *
 * Scoped to the SINGLE global/user settings source (read+write the same file), which
 * keeps it idempotent with no completion flag: once rewritten, the new id is not
 * deprecated, so {@link suggestDeprecatedReplacement} returns nothing and the next run
 * is a no-op. Project/local/managed `model` values are deliberately left alone — we
 * can't safely rewrite those, and merging them here would risk silent global promotion.
 *
 * The deprecated→successor knowledge is the SINGLE canonical table in model-registry
 * ({@link suggestDeprecatedReplacement}), reused via a LAZY import so this module —
 * on many hot paths — never statically pulls in the heavy provider/model-registry
 * graph. The actual write goes through {@link updateConfigFile} (atomic temp+rename,
 * lost-update + corrupt-file guard). Returns the migration that happened (observable
 * outcome), or null when nothing was stale. Best-effort: never throws into startup.
 */
export async function migrateDeprecatedModelAlias(
  configPath: string | null = getGlobalConfigPath(),
): Promise<ModelAliasMigration | null> {
  if (!configPath) return null;
  let suggest: (id: string, available: string[]) => string | undefined;
  try {
    ({ suggestDeprecatedReplacement: suggest } = await import('./model-registry'));
  } catch (err) {
    logError('migrate-model-alias', 'could not load the model-registry', { error: String(err) });
    return null;
  }
  let pending: ModelAliasMigration | null = null;
  try {
    const res = updateConfigFile(configPath, current => {
      const model = current.model;
      if (typeof model !== 'string' || !model.trim()) return current;
      const to = suggest(model, []);
      // Not deprecated, or already current → same reference = no write (idempotent).
      if (!to || to === model) return current;
      pending = { from: model, to, path: configPath };
      return { ...current, model: to };
    });
    // The assertion re-widens `pending` (TS keeps the let narrowed to `null` because
    // the only assignment is inside the updater callback).
    const migration = pending as ModelAliasMigration | null;
    if (migration) {
      if (res.written) {
        log('migrate-model-alias', `rewrote deprecated model "${migration.from}" -> "${migration.to}"`, { path: configPath }, 'info');
        return migration;
      }
      // The updater matched but the write was refused (corrupt/no-change) — surface it
      // so a failed settings migration is diagnosable rather than silently dropped.
      logError('migrate-model-alias', `deprecated model "${migration.from}" not rewritten (${res.reason})`, { path: configPath });
    }
  } catch (err) {
    logError('migrate-model-alias', 'migration failed', { error: String(err) });
  }
  return null;
}

/**
 * Versioned, ordered, run-once startup migration framework. Gates on the persisted
 * `migrationVersion` in the GLOBAL settings: when it already equals
 * {@link CURRENT_MIGRATION_VERSION} the whole set is a no-op (so steady-state startups
 * skip the fs scans + settings read entirely); otherwise it runs the ordered batch —
 *   1. {@link migrateLegacyDataDir} (relocate durable config out of `~/.superinference`)
 *   2. {@link migrateDeprecatedModelAlias} (rewrite a stale pinned model id)
 * — then stamps the version LAST (after the migrations' own writes, so a concurrent
 * re-read in {@link updateConfigFile} lands on top of any model rewrite). Returns the
 * structured outcome so the caller can show a ONE-TIME notice of what silently changed
 * ({@link formatStartupMigrationNotices}); the stamp guarantees the notice appears at
 * most once per generation. Without a resolvable global settings path the version can't
 * be read/stamped, so the (individually idempotent) migrations run unconditionally and
 * report their outcome with `ranVersion` set but nothing stamped. Best-effort: a failed
 * stamp is logged, never thrown into startup.
 */
export async function runStartupMigrations(
  env: NodeJS.ProcessEnv = process.env,
  configPath: string | null = getGlobalConfigPath(),
): Promise<StartupMigrationResult> {
  if (configPath) {
    const stored = loadConfigFileChecked(configPath).config?.migrationVersion ?? 0;
    if (stored === CURRENT_MIGRATION_VERSION) {
      return { ranVersion: null, relocatedDirs: [], modelAlias: null, renamedConfigKeys: [] };
    }
  }
  // Ordered migration set — append new migrations here and bump CURRENT_MIGRATION_VERSION.
  const relocatedDirs = migrateLegacyDataDir(env);
  const modelAlias = await migrateDeprecatedModelAlias(configPath);
  const renamedConfigKeys = runConfigKeyMigrations(configPath);
  if (configPath) {
    try {
      updateConfigFile(configPath, current =>
        current.migrationVersion === CURRENT_MIGRATION_VERSION
          ? current
          : { ...current, migrationVersion: CURRENT_MIGRATION_VERSION },
      );
    } catch (err) {
      logError('run-migrations', 'failed to stamp migration version', { error: String(err) });
    }
  }
  return { ranVersion: CURRENT_MIGRATION_VERSION, relocatedDirs, modelAlias, renamedConfigKeys };
}

/**
 * Every data dir the Usage/spend view should READ from — so it aggregates spend across ALL
 * agents, not just ours: our own journal, the reference's (~/.claude), and any dirs registered
 * in the dashboard config. An explicit CLAUDE_CONFIG_DIR / SUPERINFERENCE_DATA_DIR override is
 * treated as authoritative and REPLACES the defaults (keeps tests + power-users deterministic).
 */
export function resolveReadDirs(env: NodeJS.ProcessEnv = process.env): string[] {
  const override = resolveStorageOverride(env);
  if (override) return [override];
  const home = resolveHome(env);
  const dirs: string[] = [
    path.join(home, '.ami'),
    path.join(home, '.claude'),
  ];
  // Dirs an external dashboard was told to scan (claudeConfigDirs) — include them too.
  const cfgPath = dashboardConfigPath(env);
  if (cfgPath) {
    try {
      const cfg = JSON.parse(stripBom(fs.readFileSync(cfgPath, 'utf8'))) as { claudeConfigDirs?: unknown };
      if (Array.isArray(cfg.claudeConfigDirs)) {
        for (const d of cfg.claudeConfigDirs) if (typeof d === 'string') dirs.push(d);
      }
    } catch {
      /* missing/corrupt dashboard config → just use the home defaults */
    }
  }
  return dirs.filter((d, i) => dirs.indexOf(d) === i); // dedup, preserve order
}

/**
 * Resolve `cwd` to the canonical MAIN git-worktree root so EVERY worktree (and
 * every subdirectory) of one repository shares a single project identity —
 * memory, sessions and checkpoints follow the repo, not the exact directory you
 * happened to `cd` into (parity: the reference's findCanonicalGitRoot, fixing
 * per-worktree / per-subdir state fragmentation).
 *
 * `git rev-parse --git-common-dir` returns the SHARED .git directory even from a
 * linked worktree, so its parent is the main worktree root; we fall back to
 * `--show-toplevel` (current worktree root) and finally to {@link canonicalizePath}
 * of the cwd when `cwd` is not inside a git repo. Result is canonicalized and
 * cached (bounded LRU, {@link GIT_ROOT_CACHE_MAX} entries, keyed by `cwd`).
 * Never throws.
 *
 * The `.cache` surface (`size`/`clear`/`has`/`get`) is an introspection/diagnostic
 * seam: it lets an operator (or a test) observe that the cache stays bounded and
 * reset it between isolated runs.
 */
export const canonicalGitRoot = memoizeWithLRU(
  resolveGitRootUncached,
  (cwd: string) => cwd,
  GIT_ROOT_CACHE_MAX,
);

/**
 * The GLOBAL per-project state dir — `<configDir>/projects/<cwd-slug>/` — mirroring the reference's
 * `~/.claude/projects/<cwd-slug>/`. All per-project RUNTIME state (session transcripts, resume
 * blobs, checkpoints, tasks, plans, memory, …) lives here keyed by cwd slug, so the in-repo
 * `<cwd>/.ami/` holds only committed CONFIG. An optional `sub` names a subfolder (e.g. 'sessions').
 */
export function projectStateDir(cwd: string, sub = '', env: NodeJS.ProcessEnv = process.env): string {
  const base = path.join(resolveConfigDir(env), 'projects', projectSlug(cwd));
  const dir = sub ? path.join(base, sub) : base;
  assertPathWithin(dir, base);
  return dir;
}

/** Absolute path of a session's journal file under <configDir>/projects/<slug>/. */
export function journalPathFor(cwd: string, sessionId: string, env: NodeJS.ProcessEnv = process.env): string {
  const base = path.join(resolveConfigDir(env), 'projects', projectSlug(cwd));
  const file = path.join(base, `${sanitizeId(sessionId)}.jsonl`);
  assertPathWithin(file, base);
  return file;
}

/**
 * Absolute path of a sidechain/subagent journal file
 * (`projects/<slug>/subagents/[<run>/]agent-<id>.jsonl`). An optional `subdir`
 * groups related subagent transcripts under a run folder (e.g. a workflow /
 * task run id) so a downstream dashboard can roll the run up as one unit —
 * parity with the reference's agentId→subdir grouping map
 * (`setAgentTranscriptSubdir` → `subagents/workflows/<runId>/agent-<id>.jsonl`).
 * The subdir is sanitized ({@link sanitizeId}) so a hostile / separator-bearing
 * run id can never traverse outside the `subagents` root; an empty / whitespace
 * subdir keeps the prior flat layout. The result is boundary-checked.
 */
export function subagentJournalPathFor(
  cwd: string,
  agentId: string,
  env: NodeJS.ProcessEnv = process.env,
  subdir?: string,
): string {
  const base = path.join(resolveConfigDir(env), 'projects', projectSlug(cwd), 'subagents');
  const dir = subdir && subdir.trim() ? path.join(base, sanitizeId(subdir)) : base;
  const file = path.join(dir, `agent-${sanitizeId(agentId)}.jsonl`);
  assertPathWithin(file, base);
  return file;
}

/**
 * Reverse of {@link AMI_TO_CLAUDE_TOOL}: a persisted (reference-vocabulary) tool
 * name → the canonical internal name. The forward map is many-to-one
 * (`grep`/`find_references`/`search_symbols` all → `Grep`); the reverse keeps the
 * FIRST internal name seen for each reference name (insertion order), which is the
 * canonical one (`Grep` → `grep`, not `find_references`). Unknown / already-internal
 * names (and MCP `mcp__x__y`) pass through unchanged.
 */
const REFERENCE_TO_AMI_TOOL: Record<string, string> = (() => {
  const rev: Record<string, string> = {};
  for (const [ami, ref] of Object.entries(AMI_TO_CLAUDE_TOOL)) {
    if (!(ref in rev)) rev[ref] = ami;
  }
  return rev;
})();

/** Map a reference-vocabulary tool name back to the internal name. The inverse of
 *  {@link toReferenceToolName}; identity for internal/unknown/MCP names. */
export function toAmiToolName(name: string): string {
  return REFERENCE_TO_AMI_TOOL[name] ?? name;
}

/** Build a user journal line (prompt submit, or a tool_result). */
export function buildUserEntry(a: UserEntryArgs): UserEntry {
  const entry: UserEntry = {
    type: 'user',
    uuid: a.uuid,
    parentUuid: a.parentUuid,
    timestamp: a.timestamp,
    sessionId: a.sessionId,
    cwd: a.cwd,
    gitBranch: a.gitBranch,
    isSidechain: a.isSidechain,
    message: { role: 'user', content: a.content },
  };
  if (a.toolUseResult) entry.toolUseResult = a.toolUseResult;
  return entry;
}

// ---------------------------------------------------------------------------
// Crash-safe append
// ---------------------------------------------------------------------------

/**
 * Append one owner-only JSONL line to `filePath`. On the FIRST write the file is
 * created race-free with mode 0o600 via the `ax` flag (atomic create-with-mode —
 * no TOCTOU window between an existence check and open); once it exists the `ax`
 * open fails EEXIST and we fall through to a plain append (perms were set at
 * create time). A single appendFileSync of "<json>\n" is atomic enough for our
 * line sizes; a torn tail from a crash mid-write is tolerated by the reader
 * (it skips unparseable lines). The journal holds full prompts, `thinking`
 * blocks and tool_use inputs, so it must never be group/world-readable.
 */
function appendJsonlLine(filePath: string, line: string): void {
  try {
    // `ax` = append + exclusive-create; creates the file 0o600 atomically, or
    // throws EEXIST if it is already there.
    fs.appendFileSync(filePath, line, { flag: 'ax', mode: OWNER_ONLY_FILE_MODE });
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'EEXIST') {
      fs.appendFileSync(filePath, line); // already created with the restrictive mode
      return;
    }
    throw err; // e.g. ENOENT (parent dir missing) — handled by appendEntry's retry
  }
}

/**
 * Append one JSONL entry to `filePath`. Append-first: the common path (the
 * project dir already exists) is a single syscall — we only `mkdir` the parent
 * (owner-only, 0o700) on failure and retry once, instead of walking the dir tree
 * before every line (one assistant turn can emit many lines).
 * Best-effort: never throws into the engine loop.
 */
export function appendEntry(filePath: string, entry: object): void {
  const line = ndjsonSafeStringify(entry) + '\n';
  try {
    appendJsonlLine(filePath, line);
  } catch {
    // Most likely the parent dir does not exist yet — create it owner-only and
    // retry once. Any further failure (disk/permission) is swallowed: telemetry
    // is best-effort and must not break the turn.
    try {
      secureMkdirSync(path.dirname(filePath));
      appendJsonlLine(filePath, line);
    } catch {
      /* best-effort — a disk/permission error must not break the turn */
    }
  }
}

/**
 * JSON.stringify that ALSO escapes U+2028 / U+2029. Both are valid inside JSON
 * strings but are line terminators in ECMAScript and split lines for many
 * NDJSON / line-based readers — an unescaped one in message content would tear a
 * single JSONL record in two. Use this for every persisted session / transcript /
 * journal line so a record can never be silently split. `space` matches
 * JSON.stringify's indent argument (omit for compact one-line output).
 */
export function ndjsonSafeStringify(value: unknown, space?: number): string {
  return JSON.stringify(value, null, space)
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');
}

// ---------------------------------------------------------------------------
// Optional dashboard-config registration (OOTB discovery)
// ---------------------------------------------------------------------------

/**
 * Path of an external dashboard's config file to register our data dir into, if the user
 * opted in via SUPERINFERENCE_DASHBOARD_CONFIG. We do NOT hardcode a third-party config
 * path; OOTB discovery is otherwise achieved by pointing CLAUDE_CONFIG_DIR at our data dir.
 */
export function dashboardConfigPath(env: NodeJS.ProcessEnv = process.env): string | null {
  const p = env.SUPERINFERENCE_DASHBOARD_CONFIG;
  return p && p.trim() ? p : null;
}

/**
 * Idempotently add `dataDir` to the dashboard config's extra-dirs array so it scans our
 * journal. Merges into an existing config, preserving other keys. Best-effort (never throws).
 * Returns true if a write happened.
 *
 * The config can be the user's real shared config (when CLAUDE_CONFIG_DIR points
 * at it), so the rewrite goes through the canonical crash-safe writer (temp file →
 * fsync → atomic rename, owner-only 0o600, non-atomic fallback): a crash / ENOSPC
 * mid-write can never truncate it and lose every other key.
 */
export function registerDataDir(dataDir: string, configPath: string): boolean {
  try {
    let cfg: Record<string, unknown> = {};
    if (fs.existsSync(configPath)) {
      try {
        cfg = JSON.parse(stripBom(fs.readFileSync(configPath, 'utf8'))) as Record<string, unknown>;
      } catch {
        cfg = {};
      }
    }
    const key = 'claudeConfigDirs';
    const existing = Array.isArray(cfg[key]) ? (cfg[key] as unknown[]).map(String) : [];
    if (existing.includes(dataDir)) return false;
    cfg[key] = [...existing, dataDir];
    atomicWriteFileSync(configPath, JSON.stringify(cfg, null, 2) + '\n');
    return true;
  } catch {
    return false;
  }
}

/** Register our data dir with an opted-in dashboard config, if configured. */
export function maybeRegisterDataDir(env: NodeJS.ProcessEnv = process.env): boolean {
  const cfg = dashboardConfigPath(env);
  if (!cfg) return false;
  return registerDataDir(resolveConfigDir(env), cfg);
}
