/**
 * reference-journal — persist per-session agent telemetry as append-only JSONL under the
 * user's data dir, using the SAME on-disk conventions a Claude-Code-style analytics
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

import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { execSync } from 'child_process';
import { assertPathWithin } from './path-safety';

/** Compat floor: dashboards below this treat sidechain usage as estimated. Must be semver. */
export const COMPAT_VERSION = '2.1.307';

// ---------------------------------------------------------------------------
// Directory resolution + path building
// ---------------------------------------------------------------------------

/**
 * Resolve the data dir the journal is written under, matching the dashboard's own
 * precedence: an explicit CLAUDE_CONFIG_DIR wins (lets a user point the dashboard and us
 * at one dir), then our SUPERINFERENCE_DATA_DIR override, else ~/.ami.
 * We never write into ~/.claude.
 */
export function resolveConfigDir(env: NodeJS.ProcessEnv = process.env): string {
  if (env.CLAUDE_CONFIG_DIR && env.CLAUDE_CONFIG_DIR.trim()) return env.CLAUDE_CONFIG_DIR;
  if (env.SUPERINFERENCE_DATA_DIR && env.SUPERINFERENCE_DATA_DIR.trim()) return env.SUPERINFERENCE_DATA_DIR;
  return path.join(os.homedir(), '.ami');
}

/**
 * One-time, idempotent migration of durable config from the pre-rename `~/.superinference` into
 * `~/.ami` (issue #21 defect 1): the .superinference→.ami rename moved every plugin/skill/
 * marketplace ROOT but no on-disk data, orphaning plugins installed by a pre-rename binary. We
 * MOVE each durable subdir only when the target is absent (never clobber newer `.ami` data), and
 * only for the DEFAULT data dir (an explicit CLAUDE_CONFIG_DIR/SUPERINFERENCE_DATA_DIR opts out).
 * Best-effort; returns the names migrated. `homeDir` is injectable for tests.
 */
export function migrateLegacyDataDir(env: NodeJS.ProcessEnv = process.env, homeDir: string = os.homedir()): string[] {
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
    } catch { /* best-effort — a failed migration must never block startup */ }
  }
  return migrated;
}

/**
 * Every data dir the Usage/spend view should READ from — so it aggregates spend across ALL
 * agents, not just ours: our own journal, Claude Code's (~/.claude), and any dirs registered
 * in the dashboard config. An explicit CLAUDE_CONFIG_DIR / SUPERINFERENCE_DATA_DIR override is
 * treated as authoritative and REPLACES the defaults (keeps tests + power-users deterministic).
 */
export function resolveReadDirs(env: NodeJS.ProcessEnv = process.env): string[] {
  if (env.CLAUDE_CONFIG_DIR && env.CLAUDE_CONFIG_DIR.trim()) return [env.CLAUDE_CONFIG_DIR];
  if (env.SUPERINFERENCE_DATA_DIR && env.SUPERINFERENCE_DATA_DIR.trim()) return [env.SUPERINFERENCE_DATA_DIR];
  const dirs: string[] = [
    path.join(os.homedir(), '.ami'),
    path.join(os.homedir(), '.claude'),
  ];
  // Dirs an external dashboard was told to scan (claudeConfigDirs) — include them too.
  const cfgPath = dashboardConfigPath(env);
  if (cfgPath) {
    try {
      const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8')) as { claudeConfigDirs?: unknown };
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
 * Longest project-slug we emit before switching to a truncated-plus-hash form.
 * A deep cwd can produce a slug that blows past the filesystem's path/segment
 * limit (ENAMETOOLONG), so a very long slug is capped and disambiguated with a
 * short deterministic hash of the raw cwd. (Mirrors the reference's slug cap.)
 */
export const MAX_SLUG_LENGTH = 200;

/**
 * Canonicalize a filesystem path before it is used to derive a stable project
 * identity (parity: Claude's canonicalizePath). Two things make the RAW cwd an
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

/** Memoize the expensive git-root resolution per input cwd — `projectStateDir`
 *  and friends are called many times per turn and must not re-spawn `git`. */
const _gitRootCache = new Map<string, string>();

/** Hard ceiling so a wedged git invocation can never hang project-path resolution. */
const GIT_ROOT_TIMEOUT_MS = 2_000;

/**
 * Resolve `cwd` to the canonical MAIN git-worktree root so EVERY worktree (and
 * every subdirectory) of one repository shares a single project identity —
 * memory, sessions and checkpoints follow the repo, not the exact directory you
 * happened to `cd` into (parity: Claude's findCanonicalGitRoot, fixing
 * per-worktree / per-subdir state fragmentation).
 *
 * `git rev-parse --git-common-dir` returns the SHARED .git directory even from a
 * linked worktree, so its parent is the main worktree root; we fall back to
 * `--show-toplevel` (current worktree root) and finally to {@link canonicalizePath}
 * of the cwd when `cwd` is not inside a git repo. Result is canonicalized and
 * cached. Never throws.
 */
export function canonicalGitRoot(cwd: string): string {
  const cached = _gitRootCache.get(cwd);
  if (cached !== undefined) return cached;
  const resolved = resolveGitRootUncached(cwd);
  _gitRootCache.set(cwd, resolved);
  return resolved;
}

function runGit(args: string, cwd: string): string | null {
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

function resolveGitRootUncached(cwd: string): string {
  const commonDir = runGit('rev-parse --path-format=absolute --git-common-dir', cwd);
  if (commonDir) {
    // `<root>/.git` for the main worktree (and the SHARED path for linked
    // worktrees) → the repo root is its parent. A bare repo (git-common-dir is
    // the repo dir itself, not ending in `.git`) keeps the dir as its identity.
    const root = path.basename(commonDir) === '.git' ? path.dirname(commonDir) : commonDir;
    return canonicalizePath(root);
  }
  const top = runGit('rev-parse --show-toplevel', cwd);
  if (top) return canonicalizePath(top);
  return canonicalizePath(cwd);
}

/** Project slug: the dashboard derives a per-cwd folder by replacing non-alphanumerics with '-'.
 *  A slug over {@link MAX_SLUG_LENGTH} is truncated and suffixed with an 8-hex-char
 *  sha256 of the raw cwd, so deep paths stay collision-resistant and within fs limits. */
export function projectSlug(cwd: string): string {
  const slug = cwd.replace(/[^a-zA-Z0-9]/g, '-');
  if (slug.length <= MAX_SLUG_LENGTH) return slug;
  const hash = crypto.createHash('sha256').update(cwd).digest('hex').slice(0, 8);
  return slug.slice(0, MAX_SLUG_LENGTH - 9) + '-' + hash;
}

/** Sanitize a session id for use as a filename (no separators / traversal). */
function sanitizeId(id: string): string {
  return id.replace(/[\/\\]/g, '_').replace(/\.\./g, '_');
}

/**
 * The GLOBAL per-project state dir — `<configDir>/projects/<cwd-slug>/` — mirroring Claude Code's
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

/** Absolute path of a sidechain/subagent journal file (projects/<slug>/subagents/...). */
export function subagentJournalPathFor(cwd: string, agentId: string, env: NodeJS.ProcessEnv = process.env): string {
  const base = path.join(resolveConfigDir(env), 'projects', projectSlug(cwd), 'subagents');
  const file = path.join(base, `agent-${sanitizeId(agentId)}.jsonl`);
  assertPathWithin(file, base);
  return file;
}

// ---------------------------------------------------------------------------
// Vocabulary mapping (our internal ids -> the dashboard's names)
// ---------------------------------------------------------------------------

/** Our tool ids -> the dashboard's tool vocabulary (PascalCase Claude-Code names). */
export const AMI_TO_CLAUDE_TOOL: Record<string, string> = {
  bash: 'Bash',
  file_read: 'Read',
  file_edit: 'Edit',
  file_write: 'Write',
  notebook_edit: 'NotebookEdit',
  grep: 'Grep',
  find_references: 'Grep',
  search_symbols: 'Grep',
  glob: 'Glob',
  list_dir: 'LS',
  web_fetch: 'WebFetch',
  agentic_fetch: 'WebFetch',
  web_search: 'WebSearch',
  skill: 'Skill',
  task: 'Task',
  workflow: 'Task',
  todo_write: 'TodoWrite',
  task_tracker: 'TodoWrite',
  plan_mode: 'EnterPlanMode',
  tool_search: 'ToolSearch',
};

/** Map an internal tool name to the dashboard vocabulary. MCP tools (mcp__x__y) pass through. */
export function toReferenceToolName(name: string): string {
  if (name.startsWith('mcp__')) return name;
  return AMI_TO_CLAUDE_TOOL[name] ?? name;
}

/**
 * Map our model ids to ids the dashboard's pricing table can resolve. Already-dated
 * snapshot ids (…-YYYYMMDD / …-NNNNNNNN) pass through; known families map to a
 * representative priceable snapshot; everything else passes through unchanged.
 * (ALIGNMENT RISK: a model the table can't price reads $0 downstream.)
 */
export const AMI_TO_REFERENCE_MODEL: Record<string, string> = {
  'claude-opus-4-8': 'claude-opus-4-20250514',
  'claude-opus-4-7': 'claude-opus-4-20250514',
  'claude-opus-4': 'claude-opus-4-20250514',
  'claude-sonnet-4': 'claude-sonnet-4-20250514',
  'claude-haiku': 'claude-3-5-haiku-20241022',
};

export function toReferenceModelId(model: string): string {
  if (!model) return model;
  if (/\d{8}$/.test(model)) return model; // already a dated snapshot
  if (AMI_TO_REFERENCE_MODEL[model]) return AMI_TO_REFERENCE_MODEL[model];
  // Family fallbacks for forward-compat variants (claude-opus-*, claude-sonnet-*, …).
  if (/^claude-opus/.test(model)) return 'claude-opus-4-20250514';
  if (/^claude-sonnet/.test(model)) return 'claude-sonnet-4-20250514';
  if (/^claude-haiku|^claude-3-5-haiku/.test(model)) return 'claude-3-5-haiku-20241022';
  return model;
}

// ---------------------------------------------------------------------------
// Record schema
// ---------------------------------------------------------------------------

/** The subset of our per-call usage we persist (any extra fields are ignored). */
export interface JournalUsageInput {
  promptTokens: number;
  completionTokens: number;
  reasoningTokens?: number;
  cachedPromptTokens?: number;
  cacheCreationTokens?: number;
}

export interface JournalUsage {
  input_tokens: number;
  output_tokens: number;
  cache_creation_input_tokens: number;
  cache_read_input_tokens: number;
  cache_creation: { ephemeral_5m_input_tokens: number; ephemeral_1h_input_tokens: number };
  output_tokens_details: { thinking_tokens: number };
}

/**
 * Map our usage into the dashboard's snake_case API usage. `input_tokens` is the FRESH
 * (uncached) prompt portion — total prompt minus cache-read and cache-creation — so the
 * dashboard's input + cache_read + cache_creation sum reconstructs the full prompt.
 * The full cache-creation total is attributed to the 5m TTL (correct for pricing) until
 * our provider layer surfaces the ephemeral 5m/1h split.
 */
export function buildUsageFields(u: JournalUsageInput): JournalUsage {
  const cacheRead = Math.max(0, u.cachedPromptTokens ?? 0);
  const cacheCreate = Math.max(0, u.cacheCreationTokens ?? 0);
  const input = Math.max(0, u.promptTokens - cacheRead - cacheCreate);
  return {
    input_tokens: input,
    output_tokens: Math.max(0, u.completionTokens),
    cache_creation_input_tokens: cacheCreate,
    cache_read_input_tokens: cacheRead,
    cache_creation: { ephemeral_5m_input_tokens: cacheCreate, ephemeral_1h_input_tokens: 0 },
    output_tokens_details: { thinking_tokens: Math.max(0, u.reasoningTokens ?? 0) },
  };
}

export type JournalContentBlock =
  | { type: 'text'; text: string }
  | { type: 'thinking'; thinking: string }
  | { type: 'tool_use'; id: string; name: string; input: unknown };

export interface AssistantEntryArgs {
  uuid: string;
  parentUuid: string | null;
  timestamp: string;
  sessionId: string;
  cwd: string;
  gitBranch: string;
  isSidechain: boolean;
  messageId: string;
  model: string;
  stopReason: string | null;
  usage: JournalUsageInput;
  content: JournalContentBlock[];
}

export interface AssistantEntry {
  type: 'assistant';
  uuid: string;
  parentUuid: string | null;
  timestamp: string;
  sessionId: string;
  cwd: string;
  version: string;
  gitBranch: string;
  isSidechain: boolean;
  message: {
    type: 'message';
    role: 'assistant';
    id: string;
    model: string;
    stop_reason: string | null;
    usage: JournalUsage;
    content: JournalContentBlock[];
  };
}

/** Build an assistant journal line (one per completed API call). NEVER includes cost. */
export function buildAssistantEntry(a: AssistantEntryArgs): AssistantEntry {
  return {
    type: 'assistant',
    uuid: a.uuid,
    parentUuid: a.parentUuid,
    timestamp: a.timestamp,
    sessionId: a.sessionId,
    cwd: a.cwd,
    version: COMPAT_VERSION,
    gitBranch: a.gitBranch,
    isSidechain: a.isSidechain,
    message: {
      type: 'message',
      role: 'assistant',
      id: a.messageId,
      model: toReferenceModelId(a.model),
      stop_reason: a.stopReason,
      usage: buildUsageFields(a.usage),
      content: a.content.map(b =>
        b.type === 'tool_use' ? { ...b, name: toReferenceToolName(b.name) } : b,
      ),
    },
  };
}

export interface StructuredPatchHunk {
  oldStart: number;
  oldLines: number;
  newStart: number;
  newLines: number;
  lines: string[];
}

export interface UserEntryArgs {
  uuid: string;
  parentUuid: string | null;
  timestamp: string;
  sessionId: string;
  cwd: string;
  gitBranch: string;
  isSidechain: boolean;
  content: unknown;
  toolUseResult?: {
    structuredPatch?: StructuredPatchHunk[];
    is_error?: boolean;
    agentId?: string;
  };
}

export interface UserEntry {
  type: 'user';
  uuid: string;
  parentUuid: string | null;
  timestamp: string;
  sessionId: string;
  cwd: string;
  gitBranch: string;
  isSidechain: boolean;
  message: { role: 'user'; content: unknown };
  toolUseResult?: UserEntryArgs['toolUseResult'];
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
 * Append one JSONL entry to `filePath`, creating parent dirs as needed. A single
 * appendFileSync of "<json>\n" is atomic enough for our line sizes; a torn tail from a
 * crash mid-write is tolerated by the reader (it skips unparseable lines).
 * Best-effort: never throws into the engine loop.
 */
export function appendEntry(filePath: string, entry: object): void {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.appendFileSync(filePath, ndjsonSafeStringify(entry) + '\n');
  } catch {
    /* telemetry is best-effort — a disk/permission error must not break the turn */
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
 */
export function registerDataDir(dataDir: string, configPath: string): boolean {
  try {
    let cfg: Record<string, unknown> = {};
    if (fs.existsSync(configPath)) {
      try {
        cfg = JSON.parse(fs.readFileSync(configPath, 'utf8')) as Record<string, unknown>;
      } catch {
        cfg = {};
      }
    }
    const key = 'claudeConfigDirs';
    const existing = Array.isArray(cfg[key]) ? (cfg[key] as unknown[]).map(String) : [];
    if (existing.includes(dataDir)) return false;
    cfg[key] = [...existing, dataDir];
    fs.mkdirSync(path.dirname(configPath), { recursive: true });
    fs.writeFileSync(configPath, JSON.stringify(cfg, null, 2) + '\n');
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
