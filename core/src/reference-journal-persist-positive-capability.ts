// Split from reference-journal.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { execSync } from 'child_process';
import { logError } from './logger';
import { getGlobalConfigPath, updateConfigFile, loadConfigFileChecked } from './config';

/** Compat floor: dashboards below this treat sidechain usage as estimated. Must be semver. */
export const COMPAT_VERSION = '2.1.307';

// ---------------------------------------------------------------------------
// Directory resolution + path building
// ---------------------------------------------------------------------------

/**
 * Resolve the user's home directory with the reference-parity precedence (parity:
 * xdg.ts `resolveOptions`): an explicit `homedir` override wins, then the `HOME`
 * env var, then the OS passwd lookup. On POSIX `os.homedir()` reads the passwd
 * entry and does NOT reflect a re-exported `$HOME`, so a sandbox / CI / `su`
 * process that redirects HOME would otherwise resolve config/cache/data under
 * the wrong home — preferring `env.HOME` fixes that. `homedir` is an injectable
 * DI/test seam so paths can be resolved against an injected home without
 * mutating `process.env` or the global home.
 */
export function resolveHome(env: NodeJS.ProcessEnv = process.env, homedir?: string): string {
  if (homedir) return homedir;
  const h = env.HOME;
  return h && h.trim() ? h : os.homedir();
}

/**
 * Expand a leading `~` in a config-dir override to the user's home directory.
 * A value supplied via settings.json `env` (as opposed to a shell) is NOT
 * tilde-expanded by the OS, so an override of `~/.ami` would otherwise create a
 * literal `~` directory in the cwd (gh-30794 / CC-212). We expand only a leading
 * `~` segment; every other value (absolute or otherwise) passes through verbatim
 * so existing precedence/behaviour is unchanged. Home is resolved via
 * {@link resolveHome} so a redirected `$HOME` is honored.
 */
export function expandConfigDir(value: string, env: NodeJS.ProcessEnv = process.env, homedir?: string): string {
  if (value === '~' || value.startsWith('~/') || value.startsWith('~\\')) {
    return path.join(resolveHome(env, homedir), value.slice(1));
  }
  return value;
}

/**
 * Reject an externally-supplied storage root that is dangerous to trust as the
 * base for ALL per-project state (sessions, memory, checkpoints) — parity with
 * the reference's `validateMemoryPath` (memdir/paths.ts). A bad override must never
 * silently redirect every write under the cwd or near the filesystem root.
 * Rejects (after `path.normalize` + trailing-separator strip):
 *  - non-absolute ("../foo" — interpreted relative to cwd)
 *  - root / near-root (normalized length < 3: "/", "/a")
 *  - Windows drive-root ("C:")
 *  - UNC / network path ("\\\\server", "//server") — opaque trust boundary
 *  - embedded NUL (survives `normalize()`, can truncate in syscalls)
 */
export function isDangerousStorageRoot(p: string): boolean {
  // UNC/network prefixes and NUL are checked on the RAW value too: POSIX
  // `path.normalize` collapses a leading `//` to `/`, which would otherwise let a
  // UNC path slip past the normalized check.
  if (p.startsWith('\\\\') || p.startsWith('//') || p.includes('\0')) return true;
  const normalized = path.normalize(p).replace(/[/\\]+$/, '');
  return (
    !path.isAbsolute(normalized) ||
    normalized.length < 3 ||
    /^[A-Za-z]:$/.test(normalized) ||
    normalized.startsWith('\\\\') ||
    normalized.startsWith('//')
  );
}

/**
 * Resolve the first TRUSTED, validated config-dir override from the env —
 * CLAUDE_CONFIG_DIR (lets a user point an external dashboard and us at one dir),
 * then SUPERINFERENCE_DATA_DIR. Each candidate is tilde-expanded (gh-30794),
 * boundary-validated ({@link isDangerousStorageRoot}) and NFC-normalized (so
 * decomposed-Unicode paths — common on macOS APFS/HFS — compare and join
 * consistently; parity: envUtils.ts `getClaudeConfigHomeDir`). An unset / empty
 * / dangerous override is skipped (falling through to the next source, ultimately
 * `~/.ami`) and the rejection is logged — a bad value can never silently
 * redirect state. Returns null when no trusted override is present.
 */
export function resolveStorageOverride(env: NodeJS.ProcessEnv, homedir?: string): string | null {
  for (const key of ['CLAUDE_CONFIG_DIR', 'SUPERINFERENCE_DATA_DIR'] as const) {
    const raw = env[key];
    if (!raw || !raw.trim()) continue;
    const expanded = expandConfigDir(raw, env, homedir);
    if (isDangerousStorageRoot(expanded)) {
      logError('resolve-config-dir', `rejecting dangerous ${key} override`, { value: raw });
      continue;
    }
    return expanded.normalize('NFC');
  }
  return null;
}

/**
 * The conventional per-user executable directory — `~/.local/bin` — honoring the
 * same HOME-aware precedence as every other path resolver here ({@link resolveHome}:
 * an explicit `homedir` override wins, then `$HOME`, then the OS passwd entry).
 * Parity with the reference's `getUserBinDir()` (xdg.ts): "not technically XDG but
 * follows the convention", the location a native per-user installer would place (or
 * symlink) an executable so it lands on a dir that is commonly already on `PATH`.
 * AMI's own native install root is `~/.ami/bin`; this exposes the broader
 * cross-tool convention so a diagnostic (or future installer step) can reason about
 * the standard user bin dir without re-deriving it. `homedir` is a test seam.
 */
export function getUserBinDir(env: NodeJS.ProcessEnv = process.env, homedir?: string): string {
  return path.join(resolveHome(env, homedir), '.local', 'bin');
}

/**
 * Whether `dir` is a member of the `PATH` env var. Each entry is trimmed, empty
 * entries are skipped, and both sides are {@link path.resolve}d before comparing so
 * `~/.local/bin`, `./bin` and a trailing-slash variant all match their canonical
 * form; on Windows the comparison is case-insensitive (the filesystem is). A
 * non-string / unset `PATH` is treated as empty. Shared by the native-install PATH
 * reachability check and the `~/.local/bin` diagnostic so the membership rule lives
 * in exactly one place.
 */
export function isDirOnPath(
  dir: string,
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): boolean {
  let resolvedDir: string;
  try {
    resolvedDir = path.resolve(dir);
  } catch {
    resolvedDir = dir;
  }
  const isWindows = platform === 'win32';
  const rawPath = typeof env.PATH === 'string' ? env.PATH : '';
  return rawPath.split(path.delimiter).some((entry) => {
    const e = entry.trim();
    if (!e) return false;
    try {
      const r = path.resolve(e);
      return isWindows ? r.toLowerCase() === resolvedDir.toLowerCase() : r === resolvedDir;
    } catch {
      return false;
    }
  });
}

/**
 * Read a previously-cached machine-local capability-detection boolean from the
 * GLOBAL config, or `undefined` when it was never cached. Pure read — no probe, no
 * write — so a startup-critical path can consult a prior POSITIVE detection without
 * shelling out. The map lives under the single `cachedCapabilities` config key.
 * Best-effort: a missing / corrupt / non-boolean value yields `undefined`.
 */
export function readCachedCapability(
  key: string,
  configPath: string | null = getGlobalConfigPath(),
): boolean | undefined {
  if (!configPath) return undefined;
  const caps = loadConfigFileChecked(configPath).config?.cachedCapabilities;
  if (caps && typeof caps === 'object' && typeof (caps as Record<string, unknown>)[key] === 'boolean') {
    return (caps as Record<string, boolean>)[key];
  }
  return undefined;
}

/**
 * Asymmetric capability cache — persist ONLY a POSITIVE machine-local capability
 * detection into the shared GLOBAL config, NEVER a negative (parity: the reference's
 * `isChromeExtensionInstalled_CACHED_MAY_BE_STALE`, which writes only a positive
 * `cachedChromeExtensionInstalled`).
 *
 * A `~/.ami/config.json` can be shared across machines (synced home, remote-dev
 * bridge, a baked CI image), so caching a stale `false` from a machine that lacks
 * the capability — no local Docker daemon, no browser — would permanently poison
 * the detection for every session on every machine that reads that config. A stale
 * `true` costs at most one wasted live attempt, so we deliberately write `true` and
 * drop `false`. The write stores a boolean under `cachedCapabilities[key]` via the
 * canonical crash-safe, lock-guarded {@link updateConfigFile} (temp+rename, no
 * lost-update). Idempotent: a no-op when the value is already `true`. Best-effort —
 * a write failure is logged, never thrown (this is a cache, not durable state).
 * Returns true only when a write actually happened.
 */
export function persistPositiveCapability(
  key: string,
  detected: boolean,
  configPath: string | null = getGlobalConfigPath(),
): boolean {
  // Asymmetric by design: a negative detection is never persisted so it can never
  // poison a config shared across machines — a `false` is a deliberate no-op.
  if (!detected || !configPath) return false;
  try {
    const res = updateConfigFile(configPath, current => {
      const caps = (current.cachedCapabilities ?? {}) as Record<string, boolean>;
      if (caps[key] === true) return current; // already cached positive — idempotent no-op
      return { ...current, cachedCapabilities: { ...caps, [key]: true } };
    });
    return res.written;
  } catch (err) {
    logError('persist-capability', `failed to persist capability "${key}"`, { error: String(err) });
    return false;
  }
}

/**
 * One-time, idempotent migration of durable config from the pre-rename `~/.superinference` into
 * `~/.ami` (issue #21 defect 1): the .superinference→.ami rename moved every plugin/skill/
 * marketplace ROOT but no on-disk data, orphaning plugins installed by a pre-rename binary. We
 * MOVE each durable subdir only when the target is absent (never clobber newer `.ami` data), and
 * only for the DEFAULT data dir (an explicit CLAUDE_CONFIG_DIR/SUPERINFERENCE_DATA_DIR opts out).
 * Best-effort; returns the names migrated. `homeDir` is injectable for tests.
 */
export function migrateLegacyDataDir(env: NodeJS.ProcessEnv = process.env, homeDir: string = resolveHome(env)): string[] {
  if ((env.CLAUDE_CONFIG_DIR && env.CLAUDE_CONFIG_DIR.trim()) ||
      (env.SUPERINFERENCE_DATA_DIR && env.SUPERINFERENCE_DATA_DIR.trim())) return [];
  const legacy = path.join(homeDir, '.superinference');
  const target = path.join(homeDir, '.ami');
  if (!fs.existsSync(legacy)) return [];
  const migrated: string[] = [];
  // Durable, user-installed config only — NOT transient runtime (sessions/analytics/etc.), which
  // is regenerated and legitimately orphaned by the rename.
  for (const sub of ['plugins', 'marketplaces', 'marketplaces.json', 'installed-skills', 'skills', 'agents']) {
    try {
      const src = path.join(legacy, sub);
      const dst = path.join(target, sub);
      if (fs.existsSync(src) && !fs.existsSync(dst)) {
        fs.mkdirSync(target, { recursive: true });
        fs.renameSync(src, dst);
        migrated.push(sub);
      }
    } catch (err) {
      // Best-effort — a failed migration must never block startup, but record it
      // so a partial relocation is diagnosable instead of silently swallowed.
      logError('migrate-legacy', `failed to migrate ${sub}`, { error: String(err) });
    }
  }
  return migrated;
}

/** The observable outcome of a {@link migrateDeprecatedModelAlias} pass. */
export interface ModelAliasMigration {
  /** The deprecated/removed model id that was persisted in settings. */
  from: string;
  /** The current successor id it was rewritten to. */
  to: string;
  /** The settings file that was rewritten. */
  path: string;
}

/**
 * The current config-migration generation. The ordered startup migration set
 * ({@link runStartupMigrations}) runs once per generation: it executes only while a
 * user's persisted `migrationVersion` lags this constant, then stamps it. Bumping this
 * number re-runs the WHOLE set for every existing user, so a NEW migration is added by
 * appending its call to {@link runStartupMigrations} and incrementing this — an
 * extensible, ordered, idempotent framework (parity: the reference's
 * CURRENT_MIGRATION_VERSION / runMigrations(), replacing the previous single ad-hoc
 * migrateLegacyDataDir() call that had no version counter or ordered dispatch).
 */
export const CURRENT_MIGRATION_VERSION = 1;

/** The observable outcome of a {@link runStartupMigrations} pass. */
export interface StartupMigrationResult {
  /**
   * The generation stamped into the global config after this run (always
   * {@link CURRENT_MIGRATION_VERSION} when the set ran), or null when the set was
   * SKIPPED because the stored version already matched — an idempotent no-op.
   */
  ranVersion: number | null;
  /** Durable subdirs relocated out of `~/.superinference` by {@link migrateLegacyDataDir}. */
  relocatedDirs: string[];
  /** The deprecated→successor pinned-model rewrite applied to user settings, if any. */
  modelAlias: ModelAliasMigration | null;
  /** Deprecated config KEYS renamed to their successor by {@link runConfigKeyMigrations}. */
  renamedConfigKeys: string[];
}

/**
 * Options for {@link formatStartupMigrationNotices}.
 */
export interface StartupMigrationNoticeOptions {
  /**
   * Presentation-layer renderer that appends the "how to opt out" control to the
   * model-alias notice — the model remap is an AUTOMATIC change, so (parity with
   * the reference's useModelMigrationNotifications.tsx:L29 "Model updated … · Set
   * CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP=1 to opt out") the announcement also tells
   * the user the single env var that disables it. Injected (not hard-coded here) so the
   * canonical opt-out-control formatter stays in ONE place (the CLI's
   * `appendOptOutControl`) rather than this core layer re-deriving the "· Set X=1 to opt
   * out" wording. Omitted → the notice is emitted without the hint (plain data).
   */
  appendOptOut?: (message: string) => string;
}

/**
 * Human-readable one-time notices describing what {@link runStartupMigrations} silently
 * changed on disk, so the startup surface can tell the user (parity: the reference's
 * post-migration one-time notification driven by a persisted flag). Empty when the set
 * was skipped (version already current) or ran but changed nothing — so a caller prints
 * nothing on a steady-state startup, and the version stamp guarantees the notice shows at
 * most once per generation. When {@link StartupMigrationNoticeOptions.appendOptOut} is
 * supplied, the model-remap notice carries the opt-out control so the user learns how to
 * disable the auto-remap inline (parity with the reference's migration-notification hint).
 */
export function formatStartupMigrationNotices(
  result: StartupMigrationResult,
  opts: StartupMigrationNoticeOptions = {},
): string[] {
  const notices: string[] = [];
  if (result.relocatedDirs.length) {
    notices.push(
      `Relocated durable config (${result.relocatedDirs.join(', ')}) from ~/.superinference to ~/.ami.`,
    );
  }
  if (result.modelAlias) {
    const base =
      `Updated your pinned model from "${result.modelAlias.from}" to "${result.modelAlias.to}" ` +
      `(the previous id was retired) in ${result.modelAlias.path}.`;
    notices.push(opts.appendOptOut ? opts.appendOptOut(base) : base);
  }
  if (result.renamedConfigKeys.length) {
    notices.push(
      `Migrated deprecated config key(s) (${result.renamedConfigKeys.join(', ')}) to their current name.`,
    );
  }
  return notices;
}

/**
 * Longest project-slug we emit before switching to a truncated-plus-hash form.
 * A deep cwd can produce a slug that blows past the filesystem's path/segment
 * limit (ENAMETOOLONG), so a very long slug is capped and disambiguated with a
 * short deterministic hash of the raw cwd. (Mirrors the reference's slug cap.)
 */
export const MAX_SLUG_LENGTH = 200;

/**
 * Longest sanitized session/agent id we embed in a journal filename before
 * switching to a truncated-plus-hash form. A caller-supplied session id (e.g. a
 * resumed id, a workflow run id, or a hostile value) can be arbitrarily long;
 * `<id>.jsonl` / `agent-<id>.jsonl` must stay within a single filesystem path
 * component (commonly 255 bytes) or the append fails ENAMETOOLONG. Capped well
 * below 255 to leave room for the `agent-` prefix + `.jsonl` suffix.
 */
export const MAX_SESSION_ID_LENGTH = 200;

/**
 * Truncate an already-transformed path component to `maxLen`, disambiguating the
 * truncation with a short deterministic sha256 of the RAW (pre-transform) input
 * so two distinct long inputs sharing a truncated prefix never collide and the
 * mapping is stable across runs. The single canonical cap primitive — both the
 * project slug and the session-id filename route through it so the truncation /
 * hashing rule is defined exactly once (no per-site re-implementation).
 */
export function cappedComponent(transformed: string, raw: string, maxLen: number): string {
  if (transformed.length <= maxLen) return transformed;
  const hash = crypto.createHash('sha256').update(raw).digest('hex').slice(0, 8);
  return transformed.slice(0, maxLen - 9) + '-' + hash;
}

/**
 * Canonicalize a filesystem path before it is used to derive a stable project
 * identity (parity: the reference's canonicalizePath). Two things make the RAW cwd an
 * unstable key: symlinks (two paths that are the same directory) and Unicode
 * normalization (the same name encoded NFC vs NFD slugs differently). We resolve
 * the deepest existing ancestor via `realpathSync` (re-joining any not-yet-existing
 * tail) and NFC-normalize the result. A path that cannot be resolved at all
 * (never existed) falls back to its NFC form so the function is total and pure for
 * non-existent inputs (keeps `projectSlug` deterministic for fabricated paths).
 */
export function canonicalizePath(p: string): string {
  let dir = p;
  const tail: string[] = [];
  // Walk up to the deepest ancestor that exists on disk, then realpath it.
  for (let i = 0; i < 4096; i++) {
    try {
      const real = fs.realpathSync(dir);
      return (tail.length ? path.join(real, ...tail.reverse()) : real).normalize('NFC');
    } catch {
      const parent = path.dirname(dir);
      if (parent === dir) break; // reached the root without an existing ancestor
      tail.push(path.basename(dir));
      dir = parent;
    }
  }
  return p.normalize('NFC');
}

/** Hard ceiling so a wedged git invocation can never hang project-path resolution. */
export const GIT_ROOT_TIMEOUT_MS = 2_000;

/**
 * Upper bound on distinct cwds whose git-root resolution is cached. `projectStateDir`
 * and friends are called many times per turn (often with `dirname(file)` across many
 * directories), so the result MUST be memoized — but with a BOUNDED LRU, not an
 * unbounded Map: a long-running session editing files across hundreds of directories
 * would otherwise accumulate cache entries forever. Parity with the reference's
 * `findGitRoot`, which wraps the walk in `memoizeWithLRU(…, 50)` for the same reason.
 */
export const GIT_ROOT_CACHE_MAX = 50;

export function runGit(args: string, cwd: string): string | null {
  try {
    const out = execSync(`git ${args}`, {
      cwd,
      encoding: 'utf-8',
      timeout: GIT_ROOT_TIMEOUT_MS,
      stdio: ['ignore', 'pipe', 'ignore'],
      env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
    }).trim();
    return out || null;
  } catch {
    return null;
  }
}
