// Split from memory.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { expandConfigDir, isDangerousStorageRoot } from './reference-journal';
import * as path from 'path';
import * as os from 'os';
import { log as coreLog } from './logger';
import { isEnvTruthy } from './analytics';
import { abbreviateHome } from './utils/path';
import { resolveConfiguredExternalMemoryImports } from './config';

/**
 * Home-abbreviated path for log/diagnostic lines. Memory diagnostics name the
 * offending file so a user can fix it, but the absolute path leaks the user's
 * home directory (a username) into logs/telemetry. Route every logged memory
 * path through the canonical {@link abbreviateHome} (`/home/alice/x` → `~/x`) —
 * a path OUTSIDE home is returned unchanged, so project-absolute diagnostics are
 * untouched. (PII-redacted logging of memory-file paths.)
 */
export function redactLogPath(p: string): string {
  return abbreviateHome(p);
}

// eslint-disable-next-line security/detect-bidi-characters -- intentional: regex must contain the bidi chars it strips
export const INVISIBLE_CHARS_RE = /[­͏؜᠎​-‏⁠-⁤⁪-⁯﻿]/g;

export const SYSTEM_TAG_RE = /<\/?(?:system-reminder|system|anthropic|claude|cc[a-z]*)[^>]*>/gi;

export function sanitizeMemoryContent(text: string): string {
  let result = text.replace(INVISIBLE_CHARS_RE, '');
  result = result.replace(SYSTEM_TAG_RE, (tag) => tag.replace(/</g, '＜').replace(/>/g, '＞'));
  return result;
}

/**
 * Memory types for persistent knowledge across sessions.
 * - user: info about the user's role, goals, preferences
 * - feedback: guidance on how to approach work (corrections + confirmations)
 * - project: ongoing work context not derivable from code/git
 * - reference: pointers to external systems and resources
 */
export const MEMORY_TYPES = ['user', 'feedback', 'project', 'reference'] as const;

export type MemoryType = (typeof MEMORY_TYPES)[number];

/**
 * Memory scope — WHERE a memory is stored, orthogonal to its {@link MemoryType}.
 * Mirrors the reference's two-directory model (teamMemPrompts.ts `## Memory scope`):
 * - `private`: known only to you and the current user; stored in the private
 *   auto-memory root. This is the default.
 * - `team`: shared with and contributed to by everyone working in this project
 *   directory; synced at session start; stored in the shared `team/` sub-directory
 *   (the same directory {@link MemoryManager.loadTeamMemories} reads).
 */
export const MEMORY_SCOPES = ['private', 'team'] as const;

export type MemoryScope = (typeof MEMORY_SCOPES)[number];

/**
 * Which on-disk memory STORE a path physically belongs to, as resolved by
 * {@link MemoryManager.memoryScopeForPath}. Distinct from the authoring
 * {@link MemoryScope} (`private`/`team`): this is the classifier used by
 * permission/telemetry/collapse logic to recognize a path, and names the
 * personal store `personal` (the auto-memory root) to mirror the reference's
 * `memoryScopeForPath` return type (utils/memoryFileDetection.ts). `null` means
 * the path is not inside any memory store.
 */
export const MEMORY_PATH_SCOPES = ['personal', 'team'] as const;

export type MemoryPathScope = (typeof MEMORY_PATH_SCOPES)[number];

/** An on-disk memory file whose size exceeds {@link MAX_MEMORY_CHARACTER_COUNT}. */
export interface LargeMemoryFile {
  /** Absolute path to the oversized memory file. */
  path: string;
  /** Character length of the file's raw content. */
  chars: number;
}

/**
 * A user-facing memory/instruction-file HEALTH diagnostic produced by
 * {@link MemoryManager.buildMemoryDiagnostics}. Parity with the reference's
 * `buildMemoryDiagnostics` (utils/status.tsx) + the doctor context warnings
 * (utils/doctorContextWarnings.ts) + the large-memory-file status notice
 * (utils/statusNoticeDefinitions.tsx): an oversized / soon-to-be-truncated /
 * unreadable memory or CLAUDE.md/AGENTS.md file is SURFACED to the user (with a
 * char count and a remediation pointer) instead of being handled silently.
 */
export interface MemoryDiagnostic {
  /** Severity — always a warning today (these are non-fatal health signals). */
  level: 'warn';
  /** Human-readable, home-redacted message (safe to show + log). */
  message: string;
  /** Absolute path of the offending file. */
  file: string;
  /** Raw character length of the file, when the diagnostic is size-driven. */
  chars?: number;
  /**
   * Compact, locale-aware "last modified" label for the offending file
   * (e.g. `3d ago`), computed via {@link formatRelativeTimeAgo} from the file's
   * mtime. Lets the `/doctor` surface show HOW STALE an oversized file is so a
   * user can triage (an untouched-for-months bloated file is a stronger prune
   * candidate than one edited today). Omitted when the mtime cannot be read.
   */
  modifiedAgo?: string;
  /**
   * `true` when the offending file is AMI-MANAGED memory (inside the auto-memory
   * tree — stored/session/team memory), `false` when it is a USER-MANAGED
   * instruction file (CLAUDE.md / AGENTS.md / `.ami/rules/*`). Mirrors the reference
   * Code's `isAutoManagedMemoryFile` classification so a UI can route a managed
   * file to `/memory` and an instruction file to a plain editor.
   */
  managed?: boolean;
}

/**
 * A machine-readable summary of everything the memory/instruction loader pulled
 * into context for a cwd, produced by {@link MemoryManager.getMemoryLoadSummary}.
 * Parity with the reference's `memory_files_completed` / `tengu_claudemd__initial_load`
 * introspection events (the reference's markdown-memory module:L1014-L1040): rather than the context cost
 * of loaded instruction/memory files being invisible, the counts (overall + per
 * source level), total content size and load duration are SURFACED — to `/doctor`
 * for the user and to the no-PII diagnostics stream for machine monitoring — so a
 * bloated or slow memory load can be noticed and triaged.
 */
export interface MemoryLoadSummary {
  /** Total number of instruction/memory sources loaded (all levels). */
  fileCount: number;
  /** Combined character length of every loaded source's content. */
  totalBytes: number;
  /** Wall-clock milliseconds the load (discovery + read) took. */
  durationMs: number;
  /** Per-source-level file counts. Every {@link MemorySourceLevel} is present
   *  (zero when none of that level loaded) so a consumer can render a stable table. */
  byLevel: Record<MemorySourceLevel, number>;
}

export interface MemoryEntry {
  path: string;
  content: string;
  type: 'instructions' | 'memory';
  name?: string;
  description?: string;
  memoryType?: MemoryType;
  ageDays?: number;
  freshness?: string;
  mtimeMs?: number;
}

/**
 * Lightweight header record for one stored memory file — its identity and
 * frontmatter metadata WITHOUT the (potentially large) body. Produced by
 * {@link MemoryManager.scanMemoryHeaders} for listing/manifest surfaces that only
 * need names/descriptions/types (e.g. the `/memory` menu) and must not pay to read
 * every body. Parity with the reference's `MemoryHeader` (memdir/memoryScan.ts).
 */
export interface MemoryHeader {
  path: string;
  name: string;
  description?: string;
  memoryType?: MemoryType;
  mtimeMs: number;
  bytes: number;
}

/**
 * How many leading lines to read when scanning a memory file for its YAML
 * frontmatter header only (parity with the reference's `FRONTMATTER_MAX_LINES`,
 * memdir/memoryScan.ts). A header scan reads a bounded prefix instead of the whole
 * file so a manifest/listing over many memories stays cheap and can never be forced
 * to slurp a runaway body.
 */
export const FRONTMATTER_MAX_LINES = 30;

/**
 * The unified memory-source taxonomy — WHERE an instruction/memory originated,
 * as a first-class label distinct from its {@link MemoryType}. Parity with the reference
 * Code's `MEMORY_TYPE_VALUES` (utils/memory/types.ts): User, Project, Local,
 * Managed, AutoMem and (feature-gated) TeamMem. AMI previously collapsed Local
 * (`*.local.md`), AutoMem (auto-captured stored memory) and Team (shared store)
 * all into `'user'`; they are now tagged distinctly so `/memory`, diagnostics and
 * provenance can tell a gitignored local override, an auto-captured memory and a
 * shared team memory apart from a hand-authored user-global instruction.
 * - `managed`: `.ami/rules/` (team-managed, scoped rules)
 * - `user`: user-global instructions (`~/.ami`, `~/.claude/CLAUDE.md`)
 * - `local`: project-local, gitignored override files (CLAUDE.local.md, SUPERINFERENCE.local.md)
 * - `project`: checked-in project instruction files (CLAUDE.md, AGENTS.md, SUPERINFERENCE.md, …)
 * - `automem`: auto-captured stored memories (the `memory/` store)
 * - `team`: shared team-memory store (only when {@link isTeamMemoryEnabled})
 */
export const MEMORY_SOURCE_LEVELS = [
  'managed',
  'user',
  'local',
  'project',
  'automem',
  'team',
] as const;

export type MemorySourceLevel = (typeof MEMORY_SOURCE_LEVELS)[number];

export interface InstructionSource {
  content: string;
  level: MemorySourceLevel;
  path: string;
}

/**
 * Files to look for as project instructions, in priority order.
 * The first one found wins for each directory level.
 */
export const INSTRUCTION_FILES = [
  'CLAUDE.md',
  'AGENTS.md',
  'SUPERINFERENCE.md',
  'GEMINI.md',
  'CRUSH.md',
  '.cursorrules',
  '.cursor/rules',
  '.github/copilot-instructions.md',
] as const;

export const LOCAL_VARIANTS = [
  'CLAUDE.local.md',
  'SUPERINFERENCE.local.md',
] as const;

/**
 * Per-source provenance/trust labels appended to each instruction-file section
 * header so the model can see WHERE an instruction came from and whether editing
 * it is shared or private. Parity with the reference's `getClaudeMds` descriptive
 * suffixes (the reference's markdown-memory module) — AMI previously emitted a bare relative-path header.
 */
export const PROVENANCE_PROJECT = 'project instructions, checked into the codebase';

export const PROVENANCE_LOCAL = 'private project instructions, not checked in';

export const PROVENANCE_USER_GLOBAL = "user's global instructions for all projects";

/** Maximum number of parent directories to scan upward. */
export const MAX_PARENT_SCAN_DEPTH = 3;

/** Maximum size for instruction files — reject oversized files to prevent context stuffing. */
export const MAX_INSTRUCTION_FILE_SIZE = 10_240;

/**
 * Hard caps on the MEMORY.md entrypoint index (mirrors the reference's
 * MAX_ENTRYPOINT_LINES / MAX_ENTRYPOINT_BYTES). The entrypoint is injected into
 * the system prompt on every turn, so an unbounded index is a context-stuffing /
 * cost vector. {@link truncateEntrypointContent} enforces both, naming the cap it
 * hit so the overflow is never silent.
 */
export const MAX_ENTRYPOINT_LINES = 200;

export const MAX_ENTRYPOINT_BYTES = 25_000;

/**
 * Hard cap on how many stored-memory topic files are loaded into context in a
 * single turn (parity: the reference's memory-scan cap). Beyond this the newest
 * files win (loaded AFTER the recency sort); the overflow is SURFACED in the
 * generated memory context (see {@link MemoryManager.getMemoryContext}) rather
 * than silently dropped, so the model learns its index outgrew the budget and
 * can consolidate. Single-sourced here so {@link MemoryManager.loadMemories},
 * {@link MemoryManager.scanMemoryHeaders}, and the context-warning share ONE cap.
 */
export const MAX_MEMORY_FILES = 200;

/** The MEMORY.md index is a table of contents, not a memory body — it is loaded
 *  separately (readMemoryIndex) and must be excluded from the memory-body scan.
 *  Exported so the single-source entrypoint filename is reused by the
 *  consolidation-prompt builder ({@link ./memory-consolidation}) rather than
 *  re-literalling `'MEMORY.md'`. */
export const MEMORY_INDEX_FILENAME = 'MEMORY.md';

/**
 * Recommended maximum character count for a single memory file (parity with
 * the reference's `MAX_MEMORY_CHARACTER_COUNT`, the reference's markdown-memory module). Files over this
 * are not silently clipped — {@link MemoryManager.getLargeMemoryFiles} surfaces
 * them and {@link MemoryManager.getMemoryContext} emits a visible performance
 * warning so the user can split/trim rather than having context silently crowded.
 */
export const MAX_MEMORY_CHARACTER_COUNT = 40_000;

/**
 * Environment variable that redirects the ENTIRE auto-memory directory to an
 * explicit absolute path, bypassing the computed `{config}/projects/{slug}/memory`
 * location. Parity with the reference's full-path memory override
 * (`CLAUDE_COWORK_MEMORY_PATH_OVERRIDE` / `CLAUDE_CODE_REMOTE_MEMORY_DIR`,
 * memdir/paths.ts): lets an SDK/host mount memory on a space-scoped volume where
 * the per-session cwd would otherwise fragment the project key. The value is
 * tilde-expanded and boundary-validated (shared {@link isDangerousStorageRoot})
 * so a near-root / relative / UNC / NUL path can never silently redirect every
 * memory write; a rejected value falls through to the default location.
 */
export const MEMORY_DIR_OVERRIDE_ENV = 'AMI_MEMORY_DIR';

/**
 * Resolve the validated full memory-dir override from the environment, or null
 * when unset/empty/dangerous. A dangerous value is logged and skipped so it is
 * never silently honored.
 */
export function resolveMemoryDirOverride(env: NodeJS.ProcessEnv = process.env): string | null {
  const raw = env[MEMORY_DIR_OVERRIDE_ENV];
  if (!raw || !raw.trim()) return null;
  const expanded = expandConfigDir(raw, env);
  if (isDangerousStorageRoot(expanded)) {
    coreLog('memory', `rejecting dangerous ${MEMORY_DIR_OVERRIDE_ENV} override: ${raw}`);
    return null;
  }
  return expanded.normalize('NFC');
}

/**
 * Glob exclude-list for instruction (CLAUDE.md / AGENTS.md / *.local.md) files.
 * Parity with the reference's `claudeMdExcludes` setting (the reference's markdown-memory module
 * `isClaudeMdExcluded`): a user can opt specific instruction files OUT of the
 * auto-load (e.g. a vendored `**\/node_modules/**\/CLAUDE.md`). Only USER /
 * PROJECT / LOCAL instruction files are eligible — team-managed `.ami/rules/`
 * are NEVER excluded, since those are the project's enforced rules. Patterns are
 * separated by comma, colon, or semicolon.
 */
export const MEMORY_EXCLUDES_ENV = 'AMI_MEMORY_EXCLUDES';

/**
 * Resolve the effective instruction-file exclude globs: the {@link MEMORY_EXCLUDES_ENV}
 * env list UNIONED with any `configExcludes` (the persisted
 * {@link ProjectConfig.memoryExcludes} resolved from the config waterfall). Both
 * sources are honored (env is a one-shot override, config is the persisted choice);
 * duplicates are collapsed while preserving first-seen order. Matched against
 * instruction-file paths by {@link isInstructionExcluded}.
 */
export function resolveMemoryExcludes(
  env: NodeJS.ProcessEnv = process.env,
  configExcludes: string[] = [],
): string[] {
  const raw = env[MEMORY_EXCLUDES_ENV];
  const envList = raw && raw.trim()
    ? raw.split(/[,:;]+/).map(s => s.trim()).filter(s => s.length > 0)
    : [];
  return [...new Set([...envList, ...configExcludes])];
}

/**
 * One-shot env override that GRANTS consent to load outside-workspace instruction
 * `@imports` without a persisted config decision, and an optional colon/comma/semicolon
 * -separated list of extra trusted root directories. Mirrors the env-vs-config waterfall
 * of {@link resolveMemoryExcludes}: the persisted {@link ProjectConfig.externalMemoryImportsApproved}
 * is the durable choice, this env var the ephemeral one (e.g. a trusted CI run), and the
 * two root lists are UNIONED. Unset ⇒ no env consent (the default hard block stands).
 */
export const EXTERNAL_IMPORTS_APPROVE_ENV = 'AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS';

export const EXTERNAL_IMPORT_ROOTS_ENV = 'AMI_EXTERNAL_MEMORY_IMPORT_ROOTS';

/**
 * Resolve the EFFECTIVE external-import consent for a workspace: the persisted
 * {@link resolveConfiguredExternalMemoryImports | config decision} combined with the
 * one-shot {@link EXTERNAL_IMPORTS_APPROVE_ENV} / {@link EXTERNAL_IMPORT_ROOTS_ENV} env
 * overrides. Consent is granted when EITHER source approves; the trusted-root allowlists
 * are UNIONED (env `~`-expanded and absolutized). When no root is listed but consent is
 * granted, ANY outside-workspace import is permitted (approve-once for a wholly trusted
 * repo); when roots ARE listed, an import is only allowed if its real path resolves
 * within one of them. Defaults to blocked ({@link resolveConfiguredExternalMemoryImports}
 * returns `approved:false`). Kept pure/injectable for testability.
 */
export function resolveExternalMemoryImportConsent(
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): { approved: boolean; roots: string[] } {
  const cfg = resolveConfiguredExternalMemoryImports(cwd);
  const approvedByEnv = isEnvTruthy(env[EXTERNAL_IMPORTS_APPROVE_ENV]);
  const rawRoots = env[EXTERNAL_IMPORT_ROOTS_ENV];
  const envRoots = rawRoots && rawRoots.trim()
    ? rawRoots.split(/[,:;]+/).map(s => s.trim()).filter(s => s.length > 0)
    : [];
  const roots = [...new Set([...cfg.roots, ...envRoots])].map(r =>
    r.startsWith('~') ? path.join(os.homedir(), r.slice(1)) : path.resolve(r),
  );
  return { approved: cfg.approved || approvedByEnv, roots };
}

/**
 * Extra memory guidelines injected verbatim into the memory-guidance prompt
 * block. Parity with the reference's `CLAUDE_COWORK_MEMORY_EXTRA_GUIDELINES`
 * (memdir.ts `extraGuidelines`): a host/SDK can append organization-specific
 * memory rules without patching the prompt builder. The value is memory-content
 * sanitized (system-tag neutralization + invisible-char stripping) so a hostile
 * env value cannot inject fake system tags into the prompt.
 */
export const MEMORY_EXTRA_GUIDELINES_ENV = 'AMI_MEMORY_EXTRA_GUIDELINES';

/** Resolve {@link MEMORY_EXTRA_GUIDELINES_ENV} (sanitized), or '' when unset. */
export function resolveMemoryExtraGuidelines(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env[MEMORY_EXTRA_GUIDELINES_ENV];
  return raw && raw.trim() ? sanitizeMemoryContent(raw.trim()) : '';
}

/**
 * Tunable parameters of the BM25 memory-relevance scorer ({@link computeRelevanceScore}),
 * the ranking {@link MemoryManager.selectRelevantMemories} uses to pick which stored
 * memories to surface for a query. Parity with the reference's relevance-selection
 * config knobs (memdir/findRelevantMemories.ts: model/max_tokens/output_format are
 * exposed so the selection step is tunable rather than hard-coded); AMI's recall is
 * a local BM25 pass, so the equivalent knobs are the BM25 constants themselves.
 * - `k1`  — term-frequency saturation (higher ⇒ raw frequency matters more).
 * - `b`   — length normalization strength, 0‥1 (1 ⇒ fully penalize long memories).
 * - `avgDl` — assumed average memory length in tokens, the normalization anchor.
 */
export interface MemoryRelevanceConfig {
  k1: number;
  b: number;
  avgDl: number;
}
