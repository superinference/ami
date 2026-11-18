import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { execCommand } from '../utils/shell';
import { generateTempFilePath } from '../utils/tempfile';
import { detectGitTransientState, classifyRepoRemote, getStagedDiffSize, isFileStagedForDeletion } from '../utils/git';
import { parseEnvBool } from '../utils/env';
import { scanForSecrets } from './tool-utils';
import { getSessionFileAttribution, computeCommitEffort, advanceEffortBaselines, setStartingHeadSha, buildAttributionData, normalizeAttributionPath, type AttributionData } from './attribution-store';
import { resolveEntrypoint } from '../version';
import { agentContributionRatio } from '../session';
import { MemoryManager } from '../memory';
import { journalPathFor, toAmiToolName } from '../reference-journal';
import type { MemoryAccessEntry } from '../memory-file-detection';
import { loadProjectConfig, loadLocalConfig, loadGlobalConfig, loadManagedConfig, mergeConfigs } from '../config';

const CO_AUTHOR_TRAILER = 'Co-Authored-By: AMI <ami@superinference.org>';
const TRAILER_REGEX = /Co-Authored-By:\s*AMI\s*<ami@superinference\.org>/i;

/**
 * Secret-bearing FILE guard. Mirrors the sensitive-name/ext/pattern list that
 * permissions.ts applies to reads so the sanctioned commit path can never stage
 * and push a credential file (`.env`, `credentials.json`, `id_rsa`, `*.pem`, …).
 * These constants are local because the permissions.ts equivalents are
 * function-scoped and not exported; keep the two lists in sync if either moves.
 */
const SECRET_FILE_NAMES = new Set([
  'id_rsa', 'id_ed25519', 'id_ecdsa', 'id_dsa', '.netrc',
  'credentials.json',
]);
const SECRET_FILE_EXTS = ['.pem', '.key', '.p12', '.pfx', '.keystore'];
/** Suffixes that make an `.env`-prefixed file a safe template, not a real secret. */
const SAFE_ENV_SUFFIX_REGEX = /\.(example|sample|template|dist|defaults?)$/i;
const SECRET_FILE_PATTERNS = [/^secrets\.ya?ml$/i];

/** True when a staged file's basename matches a known secret-file shape. */
function isSecretFile(basename: string): boolean {
  if (/^\.env($|\.)/i.test(basename) && !SAFE_ENV_SUFFIX_REGEX.test(basename)) return true;
  if (SECRET_FILE_NAMES.has(basename)) return true;
  if (SECRET_FILE_EXTS.some(ext => basename.toLowerCase().endsWith(ext))) return true;
  return SECRET_FILE_PATTERNS.some(p => p.test(basename));
}

/**
 * Strip foreign / disallowed attribution from a commit message before the AMI
 * trailer is applied. Removes any `Co-Authored-By:` trailer that is NOT AMI's
 * own and any AI-tool provenance footer (lines carrying the 🤖 marker), so an
 * external assistant's byline can never land in a commit — matching the repo's
 * attribution policy regardless of what the drafted message contained.
 */
function stripDisallowedAttribution(message: string): string {
  const kept = message.split('\n').filter(line => {
    if (/^\s*Co-Authored-By:/i.test(line) && !TRAILER_REGEX.test(line)) return false;
    if (line.includes('🤖')) return false;
    return true;
  });
  return kept.join('\n').replace(/\n{3,}/g, '\n\n').trimEnd();
}

/**
 * Dependency seam for tests (mirrors the fsHooks pattern in config.ts). The
 * temp-file write/cleanup routes through this object so tests can simulate an
 * unlink failure (e.g. the temp file already removed) via
 * `mock.method(_gitCommitFs, 'unlinkSync', ...)`. Defaults are the real fs calls,
 * so production behaviour is unchanged.
 */
export const _gitCommitFs = {
  writeFileSync: fs.writeFileSync as typeof fs.writeFileSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
  readFileSync: fs.readFileSync as typeof fs.readFileSync,
  /**
   * Async, cancellable staging seam. Parity with Claude Code, which routes every
   * git invocation through an async child that always forwards `signal` (and
   * `timeout`): the former blocking `execFileSync('git', ['add', …])` could not
   * honor `context.abortSignal` and pinned the event loop for the whole `git add`
   * over a large tree. This argv-form (no shell → injection-safe) async wrapper
   * forwards the abort signal so staging is interruptible and non-blocking, like
   * the diff/commit/log steps already are. Mockable in tests via
   * `mock.method(_gitCommitFs, 'execFileAsync', …)`.
   */
  execFileAsync(
    file: string,
    args: readonly string[],
    options: child_process.ExecFileOptions,
  ): Promise<{ stdout: string; stderr: string }> {
    return new Promise((resolve, reject) => {
      child_process.execFile(file, args as string[], options, (err, stdout, stderr) => {
        if (err) reject(err);
        else resolve({ stdout: String(stdout), stderr: String(stderr) });
      });
    });
  },
};

/**
 * Resolve the client-surface identifier that originated this commit, so git
 * hooks / downstream tooling can see which frontend drove it. Reuses the single
 * entrypoint knob reader ({@link resolveEntrypoint}: `AMI_ENTRYPOINT` then
 * `CLAUDE_CODE_ENTRYPOINT`, blank-safe) shared with the HTTP/MCP User-Agent, and
 * defaults to `'cli'` when nothing is set — parity with Claude Code's
 * `getClientSurface()` (`CLAUDE_CODE_ENTRYPOINT ?? 'cli'`).
 */
export function resolveClientSurface(env: NodeJS.ProcessEnv = process.env): string {
  return resolveEntrypoint(env) ?? 'cli';
}

/**
 * Collapse a raw model identifier to a stable, public short name for the commit
 * surface. Parity with Claude Code's `sanitizeModelName` (buildSurfaceKey +
 * sanitizeModelName): strip a provider/vendor prefix (`anthropic/claude-…`,
 * `openai/…`, a bedrock `….anthropic.*` ARN segment) and a trailing
 * `-YYYYMMDD` / `@date` snapshot suffix so `claude-opus-4-8-20260115` and
 * `anthropic/claude-opus-4-8` both normalize to `opus-4-8`. A `claude-` family
 * prefix is dropped too (the AMI trailer already carries the AMI identity). An
 * unrecognizable/blank value yields `undefined` rather than stamping noise.
 */
export function sanitizeModelName(raw: string | undefined): string | undefined {
  if (!raw) return undefined;
  let name = raw.trim().toLowerCase();
  if (!name) return undefined;
  // Drop a provider prefix segment (last path/colon component wins).
  name = name.split(/[/:]/).pop() ?? name;
  // Bedrock/ARN style "us.anthropic.claude-…": keep the trailing model token.
  name = name.split('.').pop() ?? name;
  // Strip a trailing date snapshot suffix (-YYYYMMDD or @anything).
  name = name.replace(/@.*$/, '').replace(/-\d{8}$/, '');
  // Drop a leading vendor-family prefix.
  name = name.replace(/^(claude|gpt|gemini)-/, '');
  name = name.trim();
  return name.length > 0 ? name : undefined;
}

/**
 * Resolve the model name to stamp onto the commit (consumed by git hooks /
 * downstream tooling, parallel to {@link resolveClientSurface}'s AI_SURFACE).
 * Honors `AMI_COMMIT_MODEL` first, then the generic `ANTHROPIC_MODEL` /
 * `AMI_MODEL` the engine already sets, passing the raw value through
 * {@link sanitizeModelName}. Returns `undefined` when none is set.
 */
export function resolveCommitModel(env: NodeJS.ProcessEnv = process.env): string | undefined {
  return sanitizeModelName(env.AMI_COMMIT_MODEL ?? env.AMI_MODEL ?? env.ANTHROPIC_MODEL);
}

/**
 * Whether to PRESERVE the repo's own commit hooks / GPG signing even in detached
 * mode. AMI bypasses them by default in detached runs (anti-hang: a blocking
 * pre-commit hook or a passphrase prompt would wedge an unattended run), but
 * Claude's protocol is "NEVER skip hooks unless explicitly requested". Setting
 * `AMI_DETACHED_PRESERVE_HOOKS=1` is that explicit request — the repo's
 * pre-commit guards (secret scanners, linters, policy hooks) and signing stay
 * active in detached mode too. Default/interactive behavior is unchanged.
 */
export function shouldPreserveCommitHooks(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.AMI_DETACHED_PRESERVE_HOOKS === '1';
}

/** Git notes ref the per-commit attribution introspection record is written to. */
export const ATTRIBUTION_NOTES_REF = 'ami-attribution';

/**
 * Opt-in gate for the per-commit AI-contribution introspection surface. Parity
 * with Claude Code's per-commit attribution record (`calculateCommitAttribution`
 * → structured git-notes JSON), but OFF by default so a commit never silently
 * mutates a repo's notes refs: set `AMI_COMMIT_ATTRIBUTION_NOTES=1`/`true`/`yes`/
 * `on` to have `git_commit` record a structured attribution note for each commit
 * it creates. Uses the shared {@link parseEnvBool} truthiness convention.
 */
export function attributionNotesEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return parseEnvBool(env.AMI_COMMIT_ATTRIBUTION_NOTES);
}

/**
 * Per-file agent character-contribution figure for a committed file, derived from
 * the session-scoped attribution store (populated by the edit/write tools).
 * Parity: Claude Code's AttributionSnapshot `fileStates` → per-file
 * `claudeContribution` / claudePercent.
 */
export interface CommitFileContribution {
  /** Staged (repo-relative) path. */
  path: string;
  /** Characters the agent authored in this file across the session. */
  agentContribution: number;
  /** The file's current character size (committed content length), 0 when unreadable. */
  totalChars: number;
  /** Fraction (0..1) of the file's current character size the agent authored. */
  ratio: number;
  /** Present and `true` only when this path is staged for DELETION — its content no
   *  longer exists on disk, so `totalChars`/`ratio` are naturally 0. Parity:
   *  Claude Code's `isFileDeleted` gate ({@link isFileStagedForDeletion}). */
  deleted?: boolean;
}

/** Structured per-commit attribution introspection record. */
export interface CommitAttributionNote {
  /** Always 'ami' — the sanctioned commit tool only creates agent-authored commits. */
  authoredBy: 'ami';
  /** Client surface that drove the commit ({@link resolveClientSurface}). */
  surface: string;
  /** Normalized model name ({@link resolveCommitModel}), or null when unknown. */
  model: string | null;
  /** Whether a Co-Authored-By attribution trailer was applied to the message. */
  trailerApplied: boolean;
  /** The files committed (staged paths), for a per-file contribution view. */
  files: string[];
  /** Count of files in the commit. */
  fileCount: number;
  /** Approximate character volume of the staged changes this commit recorded,
   *  from `git diff --cached --stat` via the chars-per-line heuristic
   *  ({@link getStagedDiffSize}) — a cheap, MEASURED "how much changed" proxy that
   *  also counts human/untracked-staged edits the per-file agent store misses.
   *  Omitted when zero. Parity: Claude Code's `getGitDiffSize`. */
  stagedChangeChars?: number;
  /** Per-file agent character contribution for committed files that the agent
   *  authored this session (omitted when none was tracked). */
  fileContributions?: CommitFileContribution[];
  /** Overall integer percent (0..100) of the committed, agent-tracked files'
   *  CURRENT size that the agent authored this session — the honest, MEASURED
   *  aggregate of {@link fileContributions} (omitted when nothing was tracked).
   *  Parity: Claude Code's `claudePercent`, but computed from real char counts. */
  contributionPercent?: number;
  /** User prompts ("shots") recorded for the session when the commit was made —
   *  the "N-shotted" figure. Parity: Claude Code's transcript prompt count. */
  promptCount?: number;
  /** Distinct memory files the agent recalled (read) this session — the
   *  "M memories recalled" figure. Parity: Claude Code's `memoryAccessCount`. */
  memoriesRecalled?: number;
  /** The rendered one-line contribution byline (e.g. "93% 3-shotted by opus-4-8,
   *  2 memories recalled"), or omitted when no signal was present (all-zero).
   *  Parity: Claude Code's enhanced PR-attribution suffix ({@link formatContributionByline}). */
  byline?: string;
  /** Versioned, structured authorship payload (per-file agent/human chars,
   *  surface breakdown, excluded generated files) — the durable schema a
   *  downstream tool can re-parse. Omitted when nothing was tracked this session.
   *  Parity: Claude Code's `AttributionData` git-notes JSON. */
  attributionData?: AttributionData;
  /** ISO timestamp the attribution record was produced. */
  committedAt: string;
}

/**
 * The three computed attribution signals that compose the contribution byline
 * (parity: Claude Code's `getEnhancedPRAttribution` inputs): the overall
 * agent-authored percent, the user-prompt ("shot") count, and the
 * memories-recalled count, plus the normalized model name.
 */
export interface ContributionStats {
  /** Overall integer percent (0..100) the agent authored across committed files. */
  contributionPercent: number;
  /** User-prompt ("shot") count for the session. */
  promptCount: number;
  /** Distinct memory files recalled this session. */
  memoriesRecalled: number;
  /** Normalized short model name ({@link sanitizeModelName}), or undefined. */
  model: string | undefined;
}

/**
 * Overall integer percent (0..100) of the committed files' CURRENT size that the
 * agent authored this session, aggregated from the per-file
 * {@link CommitFileContribution} list: `round(100 * Σ agentContribution / Σ totalChars)`,
 * clamped to [0,100]. Returns `0` when nothing was tracked or every tracked file
 * is empty (zero total size). This is a truthful, MEASURED aggregate — never a
 * fabricated figure — the aggregate analogue of {@link agentContributionRatio}.
 */
export function aggregateAgentContributionPercent(
  contributions: readonly CommitFileContribution[],
): number {
  let authored = 0;
  let total = 0;
  for (const c of contributions) {
    authored += c.agentContribution > 0 ? c.agentContribution : 0;
    total += c.totalChars > 0 ? c.totalChars : 0;
  }
  if (total <= 0) return 0;
  const pct = Math.round((100 * authored) / total);
  return Math.min(100, Math.max(0, pct));
}

/**
 * Count the memory-file accesses the agent made this session for the
 * "N memories recalled" attribution suffix. Walks the session reference journal
 * (`journalPathFor(cwd, sessionId)`) — every completed assistant turn's
 * `tool_use` blocks — and applies the canonical
 * {@link MemoryManager.countMemoryFileAccessFromEntries} counter, which reuses the
 * SAME {@link MemoryManager.detectMemoryFileAccess} predicate as the session
 * file-access guard (§0b.6, one taxonomy). This covers read / grep / glob / edit /
 * write accesses, not just reads. The journal persists REFERENCE tool names
 * (`Read`/`Grep`/…), so {@link toAmiToolName} maps them back to the internal
 * vocabulary the predicate speaks. Returns `0` when there is no session id, no
 * journal yet, or no memory file was touched. Parity: Claude Code's
 * `countMemoryFileAccessFromEntries` / `memoryAccessCount`.
 */
export function countMemoriesRecalled(
  cwd: string,
  sessionId: string | undefined,
): number {
  if (!sessionId) return 0;
  let raw: string;
  try {
    raw = fs.readFileSync(journalPathFor(cwd, sessionId), 'utf-8');
  } catch {
    return 0; // no journal for this session yet (bare/test context)
  }
  const entries: MemoryAccessEntry[] = [];
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    try {
      entries.push(JSON.parse(line) as MemoryAccessEntry);
    } catch {
      /* skip a malformed journal line */
    }
  }
  if (entries.length === 0) return 0;
  return new MemoryManager(cwd).countMemoryFileAccessFromEntries(entries, toAmiToolName);
}

/**
 * Render the agent-contribution introspection byline from its three computed
 * signals, e.g. `93% 3-shotted by opus-4-8, 2 memories recalled`. Parity with
 * Claude Code's enhanced PR-attribution suffix (attribution.ts L369-374): the
 * `N-shotted` prompt count, the short model name, and a `M memories recalled`
 * clause with correct singular/plural handling (`1 memory recalled` vs
 * `2 memories recalled`, omitted at zero). When every signal is zero/absent (no
 * percent, no prompts, no memories) it returns the empty string — the all-zero
 * fallback, so a caller can omit the byline entirely rather than stamp noise.
 * The AMI commit trailer identity is unchanged; this is purely the computed stat.
 */
export function formatContributionByline(stats: ContributionStats): string {
  const { contributionPercent, promptCount, memoriesRecalled, model } = stats;
  if (contributionPercent <= 0 && promptCount <= 0 && memoriesRecalled <= 0) {
    return '';
  }
  const by = model ? ` by ${model}` : '';
  const memSuffix =
    memoriesRecalled > 0
      ? `, ${memoriesRecalled} ${memoriesRecalled === 1 ? 'memory' : 'memories'} recalled`
      : '';
  return `${contributionPercent}% ${promptCount}-shotted${by}${memSuffix}`;
}

/**
 * Build the structured attribution introspection record for a commit. This is
 * the data AMI can compute truthfully at commit time (surface / model / authored
 * files / trailer). It deliberately does NOT fabricate Claude Code's
 * character-level `claudePercent` — accurate per-character authorship needs a
 * cross-cutting Edit/Write char tracker and is a separate concern; a made-up
 * percentage would be worse than none. When the edit/write tools tracked
 * per-file character contributions for this session, they are passed through as
 * {@link CommitFileContribution}s — a truthful, measured figure (not a fabricated
 * overall percent). The overall `contributionPercent`, `promptCount`
 * ("N-shotted"), and `memoriesRecalled` signals are likewise MEASURED (char
 * aggregate, session prompt counter, memory-file reads) and composed into a
 * human-readable `byline` via {@link formatContributionByline}; each is omitted
 * when it carries no signal. `now` is injectable for deterministic tests.
 */
export function buildAttributionNote(
  params: {
    surface: string;
    model: string | undefined;
    trailerApplied: boolean;
    files: string[];
    fileContributions?: CommitFileContribution[];
    contributionPercent?: number;
    promptCount?: number;
    memoriesRecalled?: number;
    attributionData?: AttributionData;
    stagedChangeChars?: number;
  },
  now: Date = new Date(),
): CommitAttributionNote {
  const note: CommitAttributionNote = {
    authoredBy: 'ami',
    surface: params.surface,
    model: params.model ?? null,
    trailerApplied: params.trailerApplied,
    files: params.files,
    fileCount: params.files.length,
    committedAt: now.toISOString(),
  };
  if (params.fileContributions && params.fileContributions.length > 0) {
    note.fileContributions = params.fileContributions;
  }
  const contributionPercent = params.contributionPercent ?? 0;
  const promptCount = params.promptCount ?? 0;
  const memoriesRecalled = params.memoriesRecalled ?? 0;
  if (contributionPercent > 0) note.contributionPercent = contributionPercent;
  if (promptCount > 0) note.promptCount = promptCount;
  if (memoriesRecalled > 0) note.memoriesRecalled = memoriesRecalled;
  const stagedChangeChars = params.stagedChangeChars ?? 0;
  if (stagedChangeChars > 0) note.stagedChangeChars = stagedChangeChars;
  const byline = formatContributionByline({
    contributionPercent,
    promptCount,
    memoriesRecalled,
    model: params.model,
  });
  if (byline) note.byline = byline;
  if (params.attributionData) note.attributionData = params.attributionData;
  return note;
}

/**
 * Build the per-file agent character-contribution list for the committed paths
 * from the session-scoped attribution store (populated by the edit/write tools).
 * Only files the agent actually authored this session appear; each carries the
 * running character count plus the fraction of the file's CURRENT size that
 * represents ({@link agentContributionRatio}, reading the committed file to size
 * it — best-effort, a read failure just yields a 0 ratio). Returns `[]` when no
 * tracked file is part of this commit.
 */
export function collectCommitFileContributions(
  sessionId: string | undefined,
  cwd: string,
  stagedPaths: string[],
): CommitFileContribution[] {
  const attribution = getSessionFileAttribution(sessionId);
  const out: CommitFileContribution[] = [];
  for (const rel of stagedPaths) {
    const abs = path.resolve(cwd, rel);
    // Look up by the SAME realpath-normalized repo-relative key the edit/write
    // tools store under (parity: Claude Code's normalizeFilePath diff-key matching).
    const state = attribution[normalizeAttributionPath(abs, cwd)];
    if (!state) continue;
    let totalChars = 0;
    try { totalChars = _gitCommitFs.readFileSync(abs, { encoding: 'utf-8' }).length; } catch { totalChars = 0; }
    const entry: CommitFileContribution = {
      path: rel,
      agentContribution: state.agentContribution,
      totalChars,
      ratio: agentContributionRatio(state, totalChars),
    };
    // Honestly flag a file staged for deletion (its content is gone on disk, so
    // the 0 totalChars/ratio above are expected, not an unreadable-file error).
    if (isFileStagedForDeletion(cwd, rel)) entry.deleted = true;
    out.push(entry);
  }
  return out;
}

/**
 * Build the normalized one-line commit affordance from `git log --oneline -1`
 * output ("<shortsha> <subject>") → "Committed <shortsha>: <subject>", so a
 * frontend can render a clean commit line. Parity with Claude's structured
 * commit-result summary (sha + kind). Returns '' for empty input; a sha with no
 * subject yields "Committed <shortsha>".
 */
export function formatCommitSummary(onelineOutput: string): string {
  const line = onelineOutput.trim();
  if (!line) return '';
  const m = line.match(/^(\S+)\s+(.*)$/);
  if (m && m[2].length > 0) return `Committed ${m[1]}: ${m[2]}`;
  return `Committed ${line}`;
}

/**
 * Write the structured attribution introspection record as a git note on the
 * just-created commit (ref `refs/notes/ami-attribution`). Best-effort by design —
 * the caller invokes this only when {@link attributionNotesEnabled} and inside a
 * try/catch, so a notes failure (e.g. a notes.rewriteRef hook, a read-only object
 * store) can never fail an otherwise-successful commit.
 *
 * The note body is staged through a temp file whose path is derived in
 * content-hash mode ({@link generateTempFilePath}): identical attribution content
 * resolves to the same stable path, exercising the deterministic temp-path branch
 * with a real consumer. `-f` overwrites any pre-existing note on the same commit.
 */
export async function writeCommitAttributionNote(
  sha: string,
  note: CommitAttributionNote,
  execOpts: Parameters<typeof execCommand>[1],
): Promise<void> {
  const json = JSON.stringify(note, null, 2);
  const noteFile = generateTempFilePath('ami-attr-note', '.json', { contentHash: json });
  try {
    _gitCommitFs.writeFileSync(noteFile, json, { encoding: 'utf-8' });
    await execCommand(
      `git notes --ref=${ATTRIBUTION_NOTES_REF} add -f -F "${noteFile}" ${sha}`,
      execOpts,
    );
  } finally {
    try { _gitCommitFs.unlinkSync(noteFile); } catch { /* already cleaned or never written */ }
  }
}

export function buildCommitEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  env.GIT_EDITOR = 'false';
  env.GIT_PAGER = 'cat';
  env.GIT_TERMINAL_PROMPT = '0';
  env.AI_AGENT = 'superinference';
  env.AI_SURFACE = resolveClientSurface(env);
  const model = resolveCommitModel(env);
  if (model) env.AI_MODEL = model;
  return env;
}

/**
 * Resolve the commit trailer to append for a repo at `cwd`, honoring (in order):
 *  1. The merged config's `attribution.commit` (parity: Claude Code's
 *     `attribution.commit` setting): a non-empty string REPLACES the default AMI
 *     trailer; the empty string `""` suppresses it (→ `null`). When explicitly set
 *     it takes PRECEDENCE over the master toggle and the deprecated binary env
 *     opt-out below — matching Claude Code, whose `attribution` setting overrides
 *     the legacy `includeCoAuthoredBy` toggle.
 *  2. The merged config's `attribution.enabled === false` — the settings-driven
 *     MASTER disable toggle (parity: Claude Code's deprecated
 *     `includeCoAuthoredBy: false`): suppress the trailer (→ `null`) via
 *     settings.json alone, with no env var, unless a per-surface `commit` override
 *     (step 1) already decided it.
 *  3. `SUPERINFERENCE_DISABLE_CO_AUTHOR=1` — the deprecated binary env opt-out:
 *     suppress the trailer (→ `null`) only when no explicit config override exists.
 *  4. Absent → the default AMI trailer.
 * All config layers participate via {@link mergeConfigs} (managed > cli > … > global),
 * so an enterprise/global/project policy can set the custom trailer. A broken config
 * never blocks a commit — it falls back to the env opt-out / default AMI trailer.
 */
export function resolveCommitAttribution(cwd: string): string | null {
  let attribution: { commit?: string; auto?: boolean; ownRemotes?: string[]; enabled?: boolean } | undefined;
  try {
    const merged = mergeConfigs(
      {}, {},
      loadProjectConfig(cwd),
      loadGlobalConfig(),
      loadLocalConfig(cwd),
      loadManagedConfig(),
    );
    attribution = merged.attribution;
  } catch {
    attribution = undefined;
  }
  // Explicit config attribution takes precedence over the deprecated env opt-out.
  const custom = attribution?.commit;
  if (custom !== undefined) {
    const trimmed = custom.trim();
    return trimmed.length === 0 ? null : trimmed;
  }
  // Settings-driven MASTER disable toggle (parity: Claude Code's deprecated
  // includeCoAuthoredBy=false). Suppresses the trailer via settings.json alone,
  // no env var — but a per-surface `commit` override (above) still wins.
  if (attribution?.enabled === false) return null;
  // FUNCTIONAL CHANGE (additive): tolerant parse so `=true`/`yes`/`on` (not only
  // the strict `'1'`) suppress the co-author trailer, matching the shared convention.
  if (parseEnvBool(process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR)) return null;
  // Opt-in repo-class gate (parity: Claude Code's undercover auto-default). When
  // `attribution.auto` is enabled, keep the trailer ONLY when the origin remote
  // is positively classified as owned; external / unknown / no-remote all
  // suppress it (fail-safe-ON — the strict side wins unless ownership is
  // confirmed). Explicit `attribution.commit` and the env opt-out above retain
  // precedence; this gate only decides the otherwise-default case.
  if (attribution?.auto === true) {
    if (classifyRepoRemote(cwd, attribution.ownRemotes ?? []) !== 'owned') return null;
  }
  return CO_AUTHOR_TRAILER;
}

function buildFinalMessage(message: string, trailer: string | null = CO_AUTHOR_TRAILER): string {
  // Always remove foreign/disallowed attribution first — a drafted message must
  // never carry another assistant's byline, even when the trailer is opted out.
  const trimmed = stripDisallowedAttribution(message.trim());
  // Attribution opt-out (env / config `attribution.commit: ""`) → no trailer.
  if (trailer === null) {
    return trimmed;
  }
  // Avoid a duplicate trailer when the drafted message already carries it.
  const alreadyPresent = trailer === CO_AUTHOR_TRAILER
    ? TRAILER_REGEX.test(trimmed)
    : trimmed.includes(trailer);
  if (alreadyPresent) {
    return trimmed;
  }
  return trimmed + '\n\n' + trailer;
}

export const gitCommitTool: ToolDefinition = {
  name: 'git_commit',
  description:
    'Create a git commit with automatic Co-Authored-By attribution for AMI. ' +
    'Use this instead of running git commit via bash. ' +
    'Optionally stage files before committing. The Co-Authored-By trailer is appended automatically unless SUPERINFERENCE_DISABLE_CO_AUTHOR=1 is set or the config `attribution.commit` overrides/suppresses it. ' +
    'Writing the message: look at the recent commits (`git log --oneline -10`) and follow this repository\'s existing style; pick the verb for the nature of the change — "add" for a wholly new feature, "update" for an enhancement to an existing one, "fix" for a bug fix (also refactor/test/docs/chore); keep it concise (1-2 sentences) and focused on the WHY, not a restatement of the what. ' +
    'Do not stage files that likely contain secrets (.env, credentials.json, *.pem, id_rsa); this tool refuses them and refuses a staged diff containing an embedded credential. ' +
    'When AMI_COMMIT_ATTRIBUTION_NOTES is set, a structured per-commit attribution record (surface, model, committed files, and per-file agent character contribution for files edited this session) is written as a git note on ref ami-attribution for introspection.',
  inputSchema: {
    type: 'object',
    properties: {
      message: {
        type: 'string',
        description:
          'The commit message. Follow the repo\'s recent-commit style, use the right verb ' +
          '(add = new feature, update = enhancement, fix = bug fix), and keep it a concise ' +
          '1-2 sentences focused on the "why" rather than the "what". ' +
          'The Co-Authored-By trailer is appended automatically (unless SUPERINFERENCE_DISABLE_CO_AUTHOR=1) — do not include it.',
      },
      files: {
        type: 'array',
        items: { type: 'string' },
        description:
          'Files to stage before committing. If omitted, commits whatever is already staged. ' +
          'Pass ["."] to stage all tracked modified files. ' +
          'Do NOT stage files that likely contain secrets (.env, credentials.json, *.pem, id_rsa, ' +
          '.git-credentials); such files are refused, and a staged diff whose added lines contain ' +
          'an API key / token / private key is also refused.',
      },
    },
    required: ['message'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const message = input.message as string;
    const files = input.files as string[] | undefined;
    const env = buildCommitEnv();
    const execOpts = { cwd: context.cwd, timeout: 30_000, abortSignal: context.abortSignal, env };

    if (!message || message.trim().length === 0) {
      return { output: 'Error: commit message must not be empty.', isError: true };
    }

    // Transient git-state guard: refuse to create a fresh commit while the repo
    // is mid-way through a rebase / merge / cherry-pick / revert / bisect. The
    // git_commit tool is for authoring new agent commits, not for continuing a
    // multi-step operation (which needs the original message/author). Committing
    // here would hijack the in-progress operation with the drafted message. The
    // user should finish or abort it with git, or use bash `git commit` to
    // deliberately continue the operation.
    const transient = detectGitTransientState(context.cwd);
    if (transient) {
      return {
        output:
          `Error: a git ${transient} is in progress; refusing to create a new commit. ` +
          `Finish or abort the ${transient} with git first, ` +
          'or use bash `git commit` if you intend to continue that operation.',
        isError: true,
      };
    }

    // Guard against flag injection via files array
    if (files) {
      const flagEntry = files.find(f => typeof f === 'string' && f.startsWith('-') && f !== '.' && f !== '..');
      if (flagEntry) {
        return {
          output: `Error: files entries must be file paths, not flags ("${flagEntry}").`,
          isError: true,
        };
      }
    }

    // Stage files if specified. Runs through the async, cancellable execFileAsync
    // seam (carries context.abortSignal) so a large `git add` is interruptible and
    // never blocks the event loop — same cancellation boundary as the diff/commit
    // steps below.
    if (files && files.length > 0) {
      const safeFiles = files.filter(f => typeof f === 'string');
      const addArgs = safeFiles.length === 1 && safeFiles[0] === '.'
        ? ['add', '-u']
        : ['add', '--', ...safeFiles];
      try {
        await _gitCommitFs.execFileAsync('git', addArgs, {
          cwd: context.cwd, timeout: 30_000, env, signal: context.abortSignal,
        });
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
        return { output: `Error staging files: ${err}`, isError: true };
      }
    }

    // Check that there are staged changes
    const diffResult = await execCommand('git diff --cached --quiet', execOpts);
    if (diffResult.exitCode === 0) {
      return {
        output: 'Error: nothing staged for commit. Stage files with the "files" parameter, or use bash to run git add first.',
        isError: true,
      };
    }

    // Secret-bearing FILE guard: refuse to commit staged paths that look like
    // credential files (.env, credentials.json, id_rsa, *.pem, …). git_commit
    // stages/commits directly via git, bypassing the bash permission layer, so
    // this is the only place that can stop a credential file reaching a remote.
    const stagedResult = await execCommand('git diff --cached --name-only', execOpts);
    const stagedPaths = stagedResult.stdout
      .split('\n')
      .map(p => p.trim())
      .filter(Boolean);
    const secretFiles = stagedPaths.filter(p => isSecretFile(path.basename(p)));
    if (secretFiles.length > 0) {
      return {
        output:
          `Error: refusing to commit files that likely contain secrets: ${secretFiles.join(', ')}. ` +
          'Unstage them (git restore --staged <file>) or add them to .gitignore before committing.',
        isError: true,
      };
    }

    // Secret-bearing CONTENT guard: scan the ADDED lines of the staged diff (the
    // surface where credentials actually leak — an API key / token / private key
    // pasted into a committed file, not into the subject line). Parity with
    // Claude's "do not commit files that likely contain secrets" protocol, which
    // AMI previously enforced only over file NAMES and the message text. Only `+`
    // (added) lines are scanned so removing a secret is never flagged, reusing the
    // same shared scanForSecrets ruleset as the message scan.
    const stagedDiff = await execCommand('git diff --cached --no-color', execOpts);
    const addedLines = stagedDiff.stdout
      .split('\n')
      .filter(l => l.startsWith('+') && !l.startsWith('+++'))
      .map(l => l.slice(1))
      .join('\n');
    const contentSecrets = scanForSecrets(addedLines);
    if (contentSecrets.length > 0) {
      const labels = [...new Set(contentSecrets.map(s => s.label))].join(', ');
      return {
        output:
          `Error: refusing to commit — potential secrets detected in the staged changes: ${labels}. ` +
          'Remove the secret(s) from the staged diff (or unstage the offending hunk) before committing.',
        isError: true,
      };
    }

    const secrets = scanForSecrets(message);
    if (secrets.length > 0) {
      return { output: `Warning: Potential secrets detected in commit message: ${secrets.map(s => s.label).join(', ')}. Remove secrets before committing.`, isError: true };
    }

    // Gather the staged-state-dependent attribution signals BEFORE committing —
    // once the commit lands the index matches HEAD, so `git diff --cached` is
    // empty (staged change volume reads 0) and a staged deletion is no longer
    // reported by `--name-status`. These MEASURED figures can only be read now.
    // Computed solely for the opt-in attribution-note introspection surface, so a
    // commit without it enabled pays no extra git calls.
    const notesEnabled = attributionNotesEnabled(env);
    const stagedChangeChars = notesEnabled ? getStagedDiffSize(context.cwd) : 0;
    const fileContributions = notesEnabled
      ? collectCommitFileContributions(context.sessionId, context.cwd, stagedPaths)
      : [];
    // Capture the session's starting HEAD (idempotent — only the first commit of a
    // session sets it) so later tooling can detect commits made OUTSIDE this
    // session by comparing against it (parity: Claude Code's startingHeadSha). Best
    // effort and gated on the opt-in note so a plain commit pays no extra git call;
    // a repo with no commits yet yields an empty SHA and is a no-op.
    if (notesEnabled) {
      const preHead = (await execCommand('git rev-parse HEAD', execOpts)).stdout.trim();
      setStartingHeadSha(context.sessionId, preHead);
    }

    // Build the final commit message with trailer (config/env-aware attribution).
    const trailer = resolveCommitAttribution(context.cwd);
    const finalMessage = buildFinalMessage(message, trailer);

    // Write message to temp file to avoid shell escaping issues. The shared
    // generateTempFilePath helper derives an unpredictable randomUUID name under
    // the AMI temp base (honoring AMI_TMPDIR / CLAUDE_CODE_TMPDIR); combined with
    // the 'wx' (O_EXCL) flag a pre-planted symlink or predictable-path attacker
    // can't hijack the write target.
    const tmpFile = generateTempFilePath('ami-commit', '.txt');

    try {
      try {
        _gitCommitFs.writeFileSync(tmpFile, finalMessage, { encoding: 'utf-8', flag: 'wx' });
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
        return { output: `Error writing commit message file: ${err}`, isError: true };
      }

      // In detached/unattended mode, bypass user hooks and GPG signing so a
      // pre-commit hook or a passphrase prompt can't hang the run. In the
      // interactive TUI we deliberately keep the user's hooks/signing active.
      // Parity with Claude's "NEVER skip hooks unless explicitly requested": the
      // bypass can be opted OUT of (preserving the repo's own pre-commit guards /
      // GPG signing even in detached runs) via AMI_DETACHED_PRESERVE_HOOKS=1.
      const unattendedFlags = context.detachedMode && !shouldPreserveCommitHooks(env)
        ? ' -c core.hooksPath=/dev/null -c commit.gpgsign=false'
        : '';
      const commitResult = await execCommand(`git${unattendedFlags} commit -F "${tmpFile}"`, execOpts);
      if (commitResult.exitCode !== 0) {
        const err = (commitResult.stderr || commitResult.stdout).trim();
        return { output: `Error: git commit failed:\n${err}`, isError: true };
      }

      // Get the commit summary
      const logResult = await execCommand('git log --oneline -1', execOpts);
      const commitLine = logResult.stdout.trim();

      // Per-commit AI-contribution introspection surface (opt-in via
      // AMI_COMMIT_ATTRIBUTION_NOTES). Records a structured attribution note —
      // surface / model / authored files / trailer — on the new commit so
      // downstream tooling can inspect agent authorship via
      // `git notes --ref=ami-attribution show <sha>`. Best-effort: a notes
      // failure must never fail a successful commit.
      if (notesEnabled) {
        try {
          // rev-parse HEAD always resolves right after a successful commit.
          const sha = (await execCommand('git rev-parse HEAD', execOpts)).stdout.trim();
          const clientSurface = resolveClientSurface(env);
          // Versioned, structured authorship payload (per-file agent/human chars,
          // surface breakdown, excluded generated files) — attached only when
          // something was tracked this session. Parity: Claude Code's AttributionData.
          // Uses the PRE-COMMIT fileContributions captured above (post-commit the
          // index matches HEAD, so staged-state signals would read empty).
          const attributionData =
            fileContributions.length > 0
              ? buildAttributionData({
                  surface: clientSurface,
                  sessions: context.sessionId ? [context.sessionId] : [],
                  stagedFiles: stagedPaths,
                  contributions: fileContributions,
                })
              : undefined;
          const note = buildAttributionNote({
            surface: clientSurface,
            model: resolveCommitModel(env),
            trailerApplied: trailer !== null,
            files: stagedPaths,
            fileContributions,
            attributionData,
            // Computed contribution-introspection signals (all MEASURED): overall
            // agent-authored percent across the committed tracked files, the
            // session's user-prompt ("N-shotted") count, and the number of memory
            // files recalled this session. buildAttributionNote renders these into
            // a `byline` and omits any zero signal.
            contributionPercent: aggregateAgentContributionPercent(fileContributions),
            // PER-COMMIT steer count (prompts since the previous commit), not a
            // cumulative session total — parity with Claude Code's per-commit
            // "N-shotted" via the promptCountAtLastCommit baseline.
            promptCount: computeCommitEffort(context.sessionId).steers,
            memoriesRecalled: countMemoriesRecalled(context.cwd, context.sessionId),
            // MEASURED staged change volume (incl. human/untracked-staged edits the
            // per-file agent store misses), captured pre-commit via the
            // chars-per-line heuristic.
            stagedChangeChars,
          });
          await writeCommitAttributionNote(sha, note, execOpts);
        } catch { /* best-effort introspection — never fail the commit */ }
      }

      // A commit landed: snapshot the running effort counters into their per-commit
      // baselines so the NEXT commit's steer/permission/escape figures measure only
      // the effort AFTER this commit (parity: Claude Code advancing the effort
      // baselines on commit). Done AFTER the note read above (which consumes the
      // current deltas) and unconditionally so the baselines stay correct even if
      // attribution notes are toggled between commits.
      advanceEffortBaselines(context.sessionId);

      const parts: string[] = [];
      // Normalized affordance line the frontend can render cleanly ("Committed
      // <shortsha>: <subject>"), parity with Claude's structured commit summary.
      const summary = formatCommitSummary(commitLine);
      if (summary) {
        parts.push(summary);
      }
      if (commitResult.stdout.trim()) {
        parts.push(commitResult.stdout.trim());
      }

      return { output: parts.join('\n'), isError: false };
    } finally {
      try { _gitCommitFs.unlinkSync(tmpFile); } catch { /* already cleaned or never written */ }
    }
  },
};

export { buildFinalMessage, CO_AUTHOR_TRAILER, TRAILER_REGEX, isSecretFile, stripDisallowedAttribution };
