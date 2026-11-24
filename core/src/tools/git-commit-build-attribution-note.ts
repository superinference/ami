// Split from git-commit.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import { classifyRepoRemote } from '../utils/git';
import { parseEnvBool } from '../utils/env';
import { type AttributionData } from './attribution-store';
import { resolveEntrypoint } from '../version';
import { MemoryManager } from '../memory';
import { journalPathFor, toAmiToolName } from '../reference-journal';
import type { MemoryAccessEntry } from '../memory-file-detection';
import { loadProjectConfig, loadLocalConfig, loadGlobalConfig, loadManagedConfig, mergeConfigs } from '../config';

export const CO_AUTHOR_TRAILER = 'Co-Authored-By: AMI <ami@superinference.org>';

export const TRAILER_REGEX = /Co-Authored-By:\s*AMI\s*<ami@superinference\.org>/i;

export const SECRET_FILE_EXTS = ['.pem', '.key', '.p12', '.pfx', '.keystore'];

/** Suffixes that make an `.env`-prefixed file a safe template, not a real secret. */
export const SAFE_ENV_SUFFIX_REGEX = /\.(example|sample|template|dist|defaults?)$/i;

export const SECRET_FILE_PATTERNS = [/^secrets\.ya?ml$/i];

/**
 * Strip foreign / disallowed attribution from a commit message before the AMI
 * trailer is applied. Removes any `Co-Authored-By:` trailer that is NOT AMI's
 * own and any AI-tool provenance footer (lines carrying the 🤖 marker), so an
 * external assistant's byline can never land in a commit — matching the repo's
 * attribution policy regardless of what the drafted message contained.
 */
export function stripDisallowedAttribution(message: string): string {
  const kept = message.split('\n').filter(line => {
    if (/^\s*Co-Authored-By:/i.test(line) && !TRAILER_REGEX.test(line)) return false;
    if (line.includes('🤖')) return false;
    return true;
  });
  return kept.join('\n').replace(/\n{3,}/g, '\n\n').trimEnd();
}

/**
 * Resolve the client-surface identifier that originated this commit, so git
 * hooks / downstream tooling can see which frontend drove it. Reuses the single
 * entrypoint knob reader ({@link resolveEntrypoint}: `AMI_ENTRYPOINT` then
 * `CLAUDE_CODE_ENTRYPOINT`, blank-safe) shared with the HTTP/MCP User-Agent, and
 * defaults to `'cli'` when nothing is set — parity with the reference's
 * `getClientSurface()` (`CLAUDE_CODE_ENTRYPOINT ?? 'cli'`).
 */
export function resolveClientSurface(env: NodeJS.ProcessEnv = process.env): string {
  return resolveEntrypoint(env) ?? 'cli';
}

/**
 * Collapse a raw model identifier to a stable, public short name for the commit
 * surface. Parity with the reference's `sanitizeModelName` (buildSurfaceKey +
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
 * the reference's protocol is "NEVER skip hooks unless explicitly requested". Setting
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
 * with the reference's per-commit attribution record (`calculateCommitAttribution`
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
 * Parity: the reference's AttributionSnapshot `fileStates` → per-file
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
   *  the reference's `isFileDeleted` gate ({@link isFileStagedForDeletion}). */
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
   *  Omitted when zero. Parity: the reference's `getGitDiffSize`. */
  stagedChangeChars?: number;
  /** Per-file agent character contribution for committed files that the agent
   *  authored this session (omitted when none was tracked). */
  fileContributions?: CommitFileContribution[];
  /** Overall integer percent (0..100) of the committed, agent-tracked files'
   *  CURRENT size that the agent authored this session — the honest, MEASURED
   *  aggregate of {@link fileContributions} (omitted when nothing was tracked).
   *  Parity: the reference's `claudePercent`, but computed from real char counts. */
  contributionPercent?: number;
  /** User prompts ("shots") recorded for the session when the commit was made —
   *  the "N-shotted" figure. Parity: the reference's transcript prompt count. */
  promptCount?: number;
  /** Distinct memory files the agent recalled (read) this session — the
   *  "M memories recalled" figure. Parity: the reference's `memoryAccessCount`. */
  memoriesRecalled?: number;
  /** The rendered one-line contribution byline (e.g. "93% 3-shotted by opus-4-8,
   *  2 memories recalled"), or omitted when no signal was present (all-zero).
   *  Parity: the reference's enhanced PR-attribution suffix ({@link formatContributionByline}). */
  byline?: string;
  /** Versioned, structured authorship payload (per-file agent/human chars,
   *  surface breakdown, excluded generated files) — the durable schema a
   *  downstream tool can re-parse. Omitted when nothing was tracked this session.
   *  Parity: the reference's `AttributionData` git-notes JSON. */
  attributionData?: AttributionData;
  /** ISO timestamp the attribution record was produced. */
  committedAt: string;
}

/**
 * The three computed attribution signals that compose the contribution byline
 * (parity: the reference's `getEnhancedPRAttribution` inputs): the overall
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
 * journal yet, or no memory file was touched. Parity: the reference's
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
 * the reference's enhanced PR-attribution suffix (attribution.ts L369-374): the
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
 * files / trailer). It deliberately does NOT fabricate the reference's
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
 * Build the normalized one-line commit affordance from `git log --oneline -1`
 * output ("<shortsha> <subject>") → "Committed <shortsha>: <subject>", so a
 * frontend can render a clean commit line. Parity with the reference's structured
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
 *  1. The merged config's `attribution.commit` (parity: the reference's
 *     `attribution.commit` setting): a non-empty string REPLACES the default AMI
 *     trailer; the empty string `""` suppresses it (→ `null`). When explicitly set
 *     it takes PRECEDENCE over the master toggle and the deprecated binary env
 *     opt-out below — matching the reference, whose `attribution` setting overrides
 *     the legacy `includeCoAuthoredBy` toggle.
 *  2. The merged config's `attribution.enabled === false` — the settings-driven
 *     MASTER disable toggle (parity: the reference's deprecated
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
  // Settings-driven MASTER disable toggle (parity: the reference's deprecated
  // includeCoAuthoredBy=false). Suppresses the trailer via settings.json alone,
  // no env var — but a per-surface `commit` override (above) still wins.
  if (attribution?.enabled === false) return null;
  // FUNCTIONAL CHANGE (additive): tolerant parse so `=true`/`yes`/`on` (not only
  // the strict `'1'`) suppress the co-author trailer, matching the shared convention.
  if (parseEnvBool(process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR)) return null;
  // Opt-in repo-class gate (parity: the reference's undercover auto-default). When
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
