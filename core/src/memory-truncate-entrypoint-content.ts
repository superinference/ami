// Split from memory.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';
import { log as coreLog } from './logger';
import { isEnvTruthy } from './analytics';
import { isPathWithin } from './utils/path';
import { MAX_ENTRYPOINT_BYTES, MAX_ENTRYPOINT_LINES, MAX_PARENT_SCAN_DEPTH, redactLogPath } from './memory-memory-diagnostic';
import type { MemoryRelevanceConfig } from './memory-memory-diagnostic';

/** The default BM25 constants used when no env override is set. */
export const DEFAULT_MEMORY_RELEVANCE: MemoryRelevanceConfig = { k1: 1.2, b: 0.75, avgDl: 50 };

export const MEMORY_BM25_K1_ENV = 'AMI_MEMORY_BM25_K1';

export const MEMORY_BM25_B_ENV = 'AMI_MEMORY_BM25_B';

export const MEMORY_BM25_AVGDL_ENV = 'AMI_MEMORY_BM25_AVGDL';

/**
 * Parse one numeric BM25 env override, clamping it into `[min, max]` and falling
 * back to `fallback` when unset / non-finite. A malformed or out-of-range knob can
 * never corrupt the scorer (e.g. a zero `avgDl` would divide by zero, a negative
 * `k1` would invert the saturation curve) — it is coerced to a safe value instead.
 */
export function parseBm25Knob(raw: string | undefined, fallback: number, min: number, max: number): number {
  if (raw === undefined || raw.trim() === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

/**
 * Resolve the {@link MemoryRelevanceConfig} from `AMI_MEMORY_BM25_K1` / `_B` /
 * `_AVGDL`, each clamped to a safe range and defaulting to
 * {@link DEFAULT_MEMORY_RELEVANCE}. Lets an operator tune memory recall precision
 * without a code change (and keeps the scorer robust to a hostile/typo'd value).
 */
export function resolveMemoryRelevanceConfig(env: NodeJS.ProcessEnv = process.env): MemoryRelevanceConfig {
  return {
    k1: parseBm25Knob(env[MEMORY_BM25_K1_ENV], DEFAULT_MEMORY_RELEVANCE.k1, 0, 10),
    b: parseBm25Knob(env[MEMORY_BM25_B_ENV], DEFAULT_MEMORY_RELEVANCE.b, 0, 1),
    avgDl: parseBm25Knob(env[MEMORY_BM25_AVGDL_ENV], DEFAULT_MEMORY_RELEVANCE.avgDl, 1, 100_000),
  };
}

/**
 * Opt-in env flag gating whether instruction files (CLAUDE.md / AGENTS.md /
 * SUPERINFERENCE.md …) from ADDITIONAL directories — the extra roots a user grants
 * read access to via `--add-dir` / `--context-file` (the engine's
 * `allowedReadRoots`), beyond cwd and its parents — also contribute their
 * instructions to the system prompt. Parity with the reference's
 * `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD` (the reference's markdown-memory module): a granted
 * read-access dir does NOT silently inject its instruction file by default — the
 * user must opt in with a truthy value. When OFF, the additional directories still
 * grant tool read-access (unchanged) but add nothing to the prompt.
 */
export const ADDITIONAL_INSTRUCTION_DIRS_ENV = 'AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD';

/**
 * Whether {@link MemoryManager.loadProjectInstructions} should also load instruction
 * files from the constructor-supplied additional directories. Default OFF; enabled
 * by a truthy {@link ADDITIONAL_INSTRUCTION_DIRS_ENV} value ({@link isEnvTruthy}).
 */
export function additionalInstructionDirsEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[ADDITIONAL_INSTRUCTION_DIRS_ENV]);
}

/**
 * Classify an fs read/stat/mkdir failure as the EXPECTED "does not exist" case.
 * `ENOENT` (missing file/dir) and `ENOTDIR` (a parent segment is a file) are the
 * normal "no memory yet" fallbacks and are handled silently. Any OTHER errno
 * (`EACCES`/`EPERM`/`EISDIR`/`EIO`/`EEXIST`/…) means the path exists but is
 * genuinely inaccessible or malformed — NOT the same as "empty" — so callers
 * surface it via {@link logUnexpectedReadError} before falling back. Mirrors
 * the reference's errno-discriminated memory reads (SessionMemory/sessionMemoryUtils).
 */
export function isExpectedMissing(err: unknown): boolean {
  const code = errnoCode(err);
  return code === 'ENOENT' || code === 'ENOTDIR';
}

/** Extract a NodeJS errno string from an unknown thrown value ('' when absent). */
export function errnoCode(err: unknown): string {
  return err && typeof err === 'object' && 'code' in err
    ? String((err as NodeJS.ErrnoException).code)
    : '';
}

/**
 * Log a memory file-read failure that is NOT an expected "missing" case, so a
 * corrupt / permission-denied memory file is never silently equated with an
 * empty one. A no-op for ENOENT/ENOTDIR (the silent fallback).
 */
export function logUnexpectedReadError(err: unknown, filePath: string): void {
  if (isExpectedMissing(err)) return;
  coreLog('memory', `memory read failed for ${redactLogPath(filePath)}`, { code: errnoCode(err) || 'unknown' }, 'warn');
}

/**
 * Owner-only permissions for the auto-memory tree. Parity with the reference's
 * session-memory hardening (0o700 dir / 0o600 file): memory can hold personal
 * context (user profile, preferences, project state) and must not be group- or
 * world-readable. The mode is passed to mkdir/writeFile AND re-applied with an
 * explicit chmod — mkdir's mode is masked by the umask and ignored entirely when
 * the directory already exists, so the chmod is what actually defeats a loose
 * umask on a pre-existing memory dir/file.
 */
export const MEMORY_DIR_MODE = 0o700;

export const MEMORY_FILE_MODE = 0o600;

/** Best-effort chmod: a filesystem that rejects the mode must not abort a memory write. */
/** Best-effort chmod — exported so the catch arm is directly unit-testable. */
export function bestEffortChmod(filePath: string, mode: number): void {
  try { fs.chmodSync(filePath, mode); } catch { /* best effort */ }
}

/** Async counterpart of {@link bestEffortChmod}. */
export async function bestEffortChmodAsync(filePath: string, mode: number): Promise<void> {
  try { await fsp.chmod(filePath, mode); } catch { /* best effort */ }
}

/** mkdir -p the memory dir with owner-only mode, then chmod to defeat the umask
 *  / tighten an existing dir. Best-effort chmod: a filesystem that rejects the
 *  mode (e.g. a mounted volume) must not abort the memory write. */
export function ensureMemoryDirMode(dir: string): void {
  try {
    fs.mkdirSync(dir, { recursive: true, mode: MEMORY_DIR_MODE });
  } catch (err) {
    // Defensive mkdir with errno classification (parity with the reference's
    // error-code-discriminated memory write path): a read-only mount (EROFS),
    // permission-denied (EACCES/EPERM), or a path colliding with an existing
    // FILE (EEXIST/ENOTDIR) is diagnosed — naming the (home-redacted) dir and
    // the errno — rather than surfacing as an opaque throw to the caller. The
    // error is re-thrown so a failed memory write is never silently swallowed.
    coreLog('memory', `failed to create memory directory ${redactLogPath(dir)}`, { code: errnoCode(err) || 'unknown' }, 'warn');
    throw err;
  }
  bestEffortChmod(dir, MEMORY_DIR_MODE);
}

/**
 * Create `filePath` as an empty, owner-only file IF it does not already exist,
 * PRESERVING the content of an existing file. Uses the exclusive-create `wx`
 * flag (parity with the reference's `writeFile(path, '', { flag: 'wx' })` in
 * commands/memory/memory.tsx): the write atomically fails with `EEXIST` when the
 * file is already present, which is the ONE expected error and is swallowed so
 * the existing content survives; any OTHER errno (EACCES, EROFS, ENOENT on a
 * missing parent, …) is re-thrown so a genuine failure is never silently masked.
 *
 * This is the correct primitive for "ensure the index/log file exists before
 * appending" without the read-then-write race a `try-read / catch / write`
 * sequence has (two processes can both observe the file absent and the second
 * write clobbers the first).
 *
 * @returns `true` when this call created the file, `false` when it already existed.
 */
export function createFileIfAbsent(filePath: string): boolean {
  try {
    fs.writeFileSync(filePath, '', { encoding: 'utf-8', flag: 'wx', mode: MEMORY_FILE_MODE });
    return true;
  } catch (err) {
    if (errnoCode(err) === 'EEXIST') return false;
    throw err;
  }
}

/**
 * Environment variable that force-disables ALL auto-memory behavior (the stored-
 * memory / team-memory prompt injection, the oversized-file health warning, and
 * the memory save/recall guidance block). Parity with the reference's
 * `CLAUDE_CODE_DISABLE_AUTO_MEMORY` (memdir/paths.ts `isAutoMemoryEnabled`):
 * project/user INSTRUCTION files (CLAUDE.md / AGENTS.md) are NOT auto-memory and
 * always load; only the persistent memory system is gated.
 */
export const DISABLE_AUTO_MEMORY_ENV = 'AMI_DISABLE_AUTO_MEMORY';

/**
 * Whether the auto-memory system is enabled. Priority chain (first that applies
 * wins), mirroring the reference's `isAutoMemoryEnabled`:
 *   1. `AMI_DISABLE_AUTO_MEMORY` env var — a truthy value ({@link isEnvTruthy})
 *      forces OFF; an explicitly-falsy value (`0`/`false`/`no`/`off`) forces ON,
 *      overriding a settings opt-out. An unset/blank value defers downstream.
 *   2. `settingsEnabled` (the resolved `autoMemoryEnabled` setting) — supports a
 *      project-level opt-out when the env var is unset.
 *   3. Default: enabled.
 */
export function isAutoMemoryEnabled(
  env: NodeJS.ProcessEnv = process.env,
  settingsEnabled?: boolean,
): boolean {
  const raw = env[DISABLE_AUTO_MEMORY_ENV];
  if (raw !== undefined && raw.trim() !== '') {
    // Env var explicitly set: truthy ⇒ disabled, falsy ⇒ force-enabled.
    return !isEnvTruthy(raw);
  }
  if (settingsEnabled !== undefined) return settingsEnabled;
  return true;
}

/**
 * The independently-gateable instruction/memory SOURCE tiers. Parity with the reference
 * Code's per-source `isSettingSourceEnabled('userSettings' | 'projectSettings' |
 * 'localSettings')` gate (the reference's markdown-memory module): a host/SDK can disable whole
 * instruction sources (e.g. a sandboxed run that must ignore checked-in project
 * instructions, or a run that loads nothing user-global).
 *  - `user`: user-global instructions (`~/.ami`, `~/.claude/CLAUDE.md`).
 *  - `project`: checked-in project instruction files (CLAUDE.md / AGENTS.md / …).
 *  - `local`: gitignored project-local override variants (`*.local.md`).
 * Team-managed `.ami/rules/` are NOT a gateable source here — those are the
 * project's enforced rules and always load (mirroring the exclude-list rule that
 * never excludes managed rules).
 */
export const MEMORY_SOURCES = ['user', 'project', 'local'] as const;

export type MemorySource = (typeof MEMORY_SOURCES)[number];

/** Per-tier disable env var, mirroring {@link DISABLE_AUTO_MEMORY_ENV}. */
export const MEMORY_SOURCE_DISABLE_ENV: Record<MemorySource, string> = {
  user: 'AMI_DISABLE_USER_MEMORY',
  project: 'AMI_DISABLE_PROJECT_MEMORY',
  local: 'AMI_DISABLE_LOCAL_MEMORY',
};

/**
 * Whether a given instruction/memory SOURCE tier should load. Priority chain
 * (first that applies wins), mirroring {@link isAutoMemoryEnabled} and the reference
 * Code's per-source `isSettingSourceEnabled`:
 *   1. The tier's `AMI_DISABLE_{USER,PROJECT,LOCAL}_MEMORY` env var — a truthy
 *      value ({@link isEnvTruthy}) forces the tier OFF; an explicitly-falsy value
 *      (`0`/`false`/`no`/`off`) forces it ON, overriding a settings opt-out.
 *   2. `settingsEnabled` (a resolved per-tier setting), when the env var is unset.
 *   3. Default: enabled.
 * This is orthogonal to {@link isAutoMemoryEnabled} (which gates the stored/team
 * auto-memory system): these three gate the INSTRUCTION-FILE tiers.
 */
export function isMemorySourceEnabled(
  source: MemorySource,
  env: NodeJS.ProcessEnv = process.env,
  settingsEnabled?: boolean,
): boolean {
  const raw = env[MEMORY_SOURCE_DISABLE_ENV[source]];
  if (raw !== undefined && raw.trim() !== '') {
    return !isEnvTruthy(raw);
  }
  if (settingsEnabled !== undefined) return settingsEnabled;
  return true;
}

export const ENABLE_TEAM_MEMORY_ENV = 'AMI_ENABLE_TEAM_MEMORY';

/**
 * Whether the shared TEAM-memory capability is enabled. Parity with the reference
 * Code's `feature('TEAMMEM')` gate (utils/memory/types.ts, teamMemoryOps.ts):
 * team memory is OFF by default (it writes into VCS-shared files, so it is opt-in)
 * and is turned on with a truthy {@link ENABLE_TEAM_MEMORY_ENV} env value. Gates
 * {@link MemoryManager.classifyTeamMemoryToolUse} so team-memory op classification
 * is inert unless the capability has been enabled.
 */
export function isTeamMemoryEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[ENABLE_TEAM_MEMORY_ENV]);
}

/**
 * Truncate MEMORY.md entrypoint content to the line/byte caps, appending a
 * visible warning that names which cap was hit (so the model knows the index is
 * partial and the overflow is never silent). Returns the input unchanged when it
 * is already within both caps.
 */
export function truncateEntrypointContent(content: string): string {
  const lines = content.split('\n');
  let truncatedBy: 'lines' | 'bytes' | null = null;
  let out = content;
  if (lines.length > MAX_ENTRYPOINT_LINES) {
    out = lines.slice(0, MAX_ENTRYPOINT_LINES).join('\n');
    truncatedBy = 'lines';
  }
  if (Buffer.byteLength(out, 'utf-8') > MAX_ENTRYPOINT_BYTES) {
    // Byte-accurate cut (never splits a multi-byte char mid-sequence).
    const buf = Buffer.from(out, 'utf-8').subarray(0, MAX_ENTRYPOINT_BYTES);
    out = buf.toString('utf-8').replace(/�+$/, '');
    truncatedBy = 'bytes';
  }
  if (!truncatedBy) return content;
  const cap = truncatedBy === 'lines'
    ? `${MAX_ENTRYPOINT_LINES}-line`
    : `${MAX_ENTRYPOINT_BYTES}-byte`;
  return `${out}\n\n[MEMORY.md truncated to the ${cap} entrypoint cap — some index entries are omitted]`;
}

/**
 * realpathSync on the deepest existing ancestor of `p`, re-joining any
 * not-yet-existing tail. A file that does not exist yet still canonicalizes its
 * real (symlink-resolved) parent. Returns null when nothing in the chain
 * resolves. Shared by every workspace-confinement check below so symlink-escape
 * handling is identical across @-imports, @includes, and team-memory loading.
 */
export function realpathDeepest(p: string): string | null {
  let dir = path.resolve(p);
  const tail: string[] = [];
  for (;;) {
    try {
      const real = fs.realpathSync(dir);
      return tail.length ? path.join(real, ...tail.slice().reverse()) : real;
    } catch {
      const parent = path.dirname(dir);
      if (parent === dir) return null;
      tail.push(path.basename(dir));
      dir = parent;
    }
  }
}

/**
 * Canonical (symlink-resolved) workspace-containment check. Resolves symlinks on
 * both `root` and `target` before comparing, so a link that sits INSIDE the
 * workspace but points OUT of it is caught — a purely lexical prefix check
 * (path.normalize) misses that. Returns the resolved real path when contained,
 * else null.
 */
export function realpathWithin(target: string, root: string): string | null {
  const realRoot = realpathDeepest(root);
  const realTarget = realpathDeepest(target);
  if (realRoot === null || realTarget === null) return null;
  if (realTarget === realRoot || realTarget.startsWith(realRoot + path.sep)) return realTarget;
  return null;
}

/**
 * A directory is a git repository root if it contains `.git` as EITHER a
 * directory (normal clone) OR a file (linked worktree / submodule, where `.git`
 * is a `gitdir:` pointer file). The parent-walk must stop at both, or it leaks
 * instruction files from an enclosing project into a worktree/submodule.
 */
export function isGitRoot(dir: string): boolean {
  try {
    const st = fs.statSync(path.join(dir, '.git'));
    return st.isDirectory() || st.isFile();
  } catch {
    return false;
  }
}

/** `dir` is the MAIN working tree of a normal clone — `.git` is a DIRECTORY. */
export function hasGitDirDir(dir: string): boolean {
  try {
    return fs.statSync(path.join(dir, '.git')).isDirectory();
  } catch {
    return false;
  }
}

/**
 * Read `dir`'s `.git` file pointer when it is a LINKED-WORKTREE marker, returning the
 * canonical MAIN-repo root the worktree belongs to, else `null`. A linked worktree's
 * `.git` is a FILE containing `gitdir: <main>/.git/worktrees/<name>`; parsing it yields
 * the main root (`<main>`) directly. A SUBMODULE's pointer instead reads
 * `.git/modules/<name>`, which this deliberately does NOT match — a submodule must keep
 * stopping at its own root (never borrowing a parent project's instruction files). Any
 * read/parse failure or a non-worktree pointer returns `null`.
 */
export function worktreeMainRootFromPointer(dir: string): string | null {
  const gitPath = path.join(dir, '.git');
  try {
    if (!fs.statSync(gitPath).isFile()) return null;
  } catch {
    return null;
  }
  let raw: string;
  try {
    raw = fs.readFileSync(gitPath, 'utf-8');
  } catch {
    return null;
  }
  const m = raw.match(/gitdir:\s*(.+)/);
  if (!m) return null;
  const gitdir = m[1].trim().replace(/\\/g, '/');
  // `<main>/.git/worktrees/<name>` → the main-repo root is the part before `/.git/worktrees/`.
  const wt = gitdir.match(/^(.*)\/\.git\/worktrees\/[^/]+\/?$/);
  if (!wt) return null;
  return path.normalize(wt[1]);
}

/**
 * Detect the NESTED-WORKTREE case that `loadProjectInstructions` must handle for parity
 * with the reference's nested-worktree de-duplication (the reference's markdown-memory module): a git worktree
 * (`.git` pointer file) that lives physically INSIDE its own main repo tree — e.g. AMI's
 * `<main>/.claude/worktrees/<name>/`. Walking up from `cwd` would stop at the worktree's
 * `.git` and so MISS the main repo's gitignored Local files (CLAUDE.local.md), which only
 * exist in the main checkout. Returns the worktree root and the canonical main root when
 * the layout is a true nested worktree (main root resolved from the pointer, is a real
 * `.git`-dir clone, and the worktree sits within it); `null` otherwise — so every normal
 * repo / standalone dir / submodule / out-of-tree worktree keeps today's behavior exactly.
 */
export function detectNestedWorktree(cwd: string): { worktreeRoot: string; canonicalRoot: string } | null {
  let dir = path.resolve(cwd);
  const fsRoot = path.parse(dir).root;
  // Bound the probe to the normal scan depth plus the fixed `.claude/worktrees/<name>`
  // nesting so an arbitrarily deep cwd cannot turn detection into an unbounded walk.
  for (let i = 0; i <= MAX_PARENT_SCAN_DEPTH + 4; i++) {
    const mainRoot = worktreeMainRootFromPointer(dir);
    if (mainRoot !== null) {
      // A worktree marker: a nested worktree only when the resolved main root is a real
      // clone root AND the worktree dir is contained within it.
      if (hasGitDirDir(mainRoot) && isPathWithin(dir, mainRoot, cwd) && path.normalize(dir) !== path.normalize(mainRoot)) {
        return { worktreeRoot: dir, canonicalRoot: path.normalize(mainRoot) };
      }
      return null;
    }
    // A `.git` DIRECTORY before any worktree marker ⇒ a normal main clone, not nested.
    if (hasGitDirDir(dir)) return null;
    const parent = path.dirname(dir);
    if (parent === dir || dir === fsRoot) break;
    dir = parent;
  }
  return null;
}
