import * as fs from 'fs';
import {
  projectStateDir,
  canonicalGitRoot,
  projectSlug,
  resolveConfigDir,
  expandConfigDir,
  isDangerousStorageRoot,
} from './reference-journal';
import * as fsp from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import type { SessionFact } from './session-memory';
import type { AgentMemoryScope } from './skills';
import { log as coreLog, logForDiagnosticsNoPII } from './logger';
import { redactCredentials } from './credential-guard';
import { isEnvTruthy } from './analytics';
import { abbreviateHome, isPathWithin } from './utils/path';
import { scanForSecrets } from './tools/tool-utils';
import { resolveConfiguredMemoryExcludes } from './config';

/**
 * Home-abbreviated path for log/diagnostic lines. Memory diagnostics name the
 * offending file so a user can fix it, but the absolute path leaks the user's
 * home directory (a username) into logs/telemetry. Route every logged memory
 * path through the canonical {@link abbreviateHome} (`/home/alice/x` → `~/x`) —
 * a path OUTSIDE home is returned unchanged, so project-absolute diagnostics are
 * untouched. (PII-redacted logging of memory-file paths.)
 */
function redactLogPath(p: string): string {
  return abbreviateHome(p);
}

/**
 * Deny-by-default allowlist of file extensions that `@include` / `@path` imports
 * are permitted to inline into instructions/memory. Parity with Claude Code's
 * `TEXT_FILE_EXTENSIONS` (utils/claudemd.ts `parseMemoryFileContent`): a crafted
 * CLAUDE.md/AGENTS.md could otherwise slurp a binary blob (an image, PDF, archive,
 * shared object, `.so.1`, `.dat`, or any novel binary format) into the system
 * prompt. An allowlist is strictly safer than a finite binary DENYlist, which any
 * unlisted or multi-dotted extension silently defeats. See {@link isTextImportPath}.
 */
const TEXT_FILE_EXTENSIONS = new Set([
  // Markdown and text
  '.md', '.txt', '.text',
  // Data formats
  '.json', '.yaml', '.yml', '.toml', '.xml', '.csv',
  // Web
  '.html', '.htm', '.css', '.scss', '.sass', '.less',
  // JavaScript/TypeScript
  '.js', '.ts', '.tsx', '.jsx', '.mjs', '.cjs', '.mts', '.cts',
  // Python
  '.py', '.pyi', '.pyw',
  // Ruby
  '.rb', '.erb', '.rake',
  // Go
  '.go',
  // Rust
  '.rs',
  // Java/Kotlin/Scala
  '.java', '.kt', '.kts', '.scala',
  // C/C++
  '.c', '.cpp', '.cc', '.cxx', '.h', '.hpp', '.hxx',
  // C#
  '.cs',
  // Swift
  '.swift',
  // Shell
  '.sh', '.bash', '.zsh', '.fish', '.ps1', '.bat', '.cmd',
  // Config
  '.env', '.ini', '.cfg', '.conf', '.config', '.properties',
  // Database / query
  '.sql', '.graphql', '.gql',
  // Protocol
  '.proto',
  // Frontend frameworks
  '.vue', '.svelte', '.astro',
  // Templating
  '.ejs', '.hbs', '.pug', '.jade',
  // Other languages
  '.php', '.pl', '.pm', '.lua', '.r', '.dart', '.ex', '.exs', '.erl', '.hrl',
  '.clj', '.cljs', '.cljc', '.edn', '.hs', '.lhs', '.elm', '.ml', '.mli',
  '.f', '.f90', '.f95', '.for',
  // Build files
  '.cmake', '.make', '.makefile', '.gradle', '.sbt',
  // Documentation
  '.rst', '.adoc', '.asciidoc', '.org', '.tex', '.latex',
  // Lock files (often text-based)
  '.lock',
  // Misc
  '.log', '.diff', '.patch',
]);

/**
 * Whether an `@include`/`@path` target may be inlined, by extension. Deny by
 * default: an EXTENSIONLESS file (`Dockerfile`, `Makefile`, `LICENSE`) is allowed,
 * but a file carrying an extension that is NOT a known text type is skipped.
 * Mirrors Claude Code's `if (ext && !TEXT_FILE_EXTENSIONS.has(ext))` gate, so a
 * multi-dotted binary like `libatomic.so.1` (extname `.1`) is correctly refused.
 */
export function isTextImportPath(filePath: string): boolean {
  const ext = path.extname(filePath).toLowerCase();
  return ext === '' || TEXT_FILE_EXTENSIONS.has(ext);
}

// eslint-disable-next-line security/detect-bidi-characters -- intentional: regex must contain the bidi chars it strips
const INVISIBLE_CHARS_RE = /[­͏؜᠎​-‏⁠-⁤⁪-⁯﻿]/g;
const SYSTEM_TAG_RE = /<\/?(?:system-reminder|system|anthropic|claude|cc[a-z]*)[^>]*>/gi;

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
 * Mirrors Claude Code's two-directory model (teamMemPrompts.ts `## Memory scope`):
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
 * personal store `personal` (the auto-memory root) to mirror Claude Code's
 * `memoryScopeForPath` return type (utils/memoryFileDetection.ts). `null` means
 * the path is not inside any memory store.
 */
export type MemoryPathScope = 'personal' | 'team';

/**
 * Per-type memory taxonomy guidance surfaced in the system prompt. Mirrors Claude
 * Code's `TYPES_SECTION_INDIVIDUAL` (memdir/memoryTypes.ts): each type carries a
 * rich <description>, <when_to_save> trigger, <how_to_use> cue, a <body_structure>
 * scaffold for the save-heavy types, and two eval-validated worked examples. This
 * replaces the bare 4-line type list — the model needs to recognize WHEN to write
 * each type and what a good one looks like, not just the type names.
 */
const MEMORY_TYPES_GUIDANCE = [
  'There are several discrete types of memory that you can store in your memory system:',
  '',
  '<types>',
  '<type>',
  '    <name>user</name>',
  "    <description>Contain information about the user's role, goals, responsibilities, and knowledge. Great user memories help you tailor your future behavior to the user's preferences and perspective. Your goal in reading and writing these memories is to build up an understanding of who the user is and how you can be most helpful to them specifically. For example, you should collaborate with a senior software engineer differently than a student who is coding for the very first time. Avoid writing memories about the user that could be viewed as a negative judgement or that are not relevant to the work you're trying to accomplish together.</description>",
  "    <when_to_save>When you learn any details about the user's role, preferences, responsibilities, or knowledge</when_to_save>",
  '    <how_to_use>When your work should be informed by the user\'s profile or perspective. For example, if the user is asking you to explain a part of the code, answer in a way tailored to the details they will find most valuable or that build on domain knowledge they already have.</how_to_use>',
  '    <scope>Always private — a user profile is personal to this user and must never be written to shared/team memory.</scope>',
  '    <examples>',
  "    user: I'm a data scientist investigating what logging we have in place",
  '    assistant: [saves user memory: user is a data scientist, currently focused on observability/logging]',
  '',
  "    user: I've been writing Go for ten years but this is my first time touching the React side of this repo",
  "    assistant: [saves user memory: deep Go expertise, new to React and this project's frontend — frame frontend explanations in terms of backend analogues]",
  '    </examples>',
  '</type>',
  '<type>',
  '    <name>feedback</name>',
  '    <description>Guidance the user has given you about how to approach work — both what to avoid and what to keep doing. These let you remain coherent and responsive to how work should be done in the project. Record from failure AND success: if you only save corrections, you will avoid past mistakes but drift away from approaches the user has already validated, and may grow overly cautious.</description>',
  '    <when_to_save>Any time the user corrects your approach ("no not that", "don\'t", "stop doing X") OR confirms a non-obvious approach worked ("yes exactly", "perfect, keep doing that", accepting an unusual choice without pushback). Corrections are easy to notice; confirmations are quieter — watch for them. Include *why* so you can judge edge cases later.</when_to_save>',
  '    <how_to_use>Let these memories guide your behavior so the user does not need to offer the same guidance twice.</how_to_use>',
  '    <scope>Default private; use team scope only when the guidance is a project-wide convention everyone working here should follow. Before saving a PRIVATE feedback memory, check it does not contradict an existing TEAM feedback memory — if it does, update the team memory instead of silently diverging from it.</scope>',
  '    <body_structure>Lead with the rule itself, then a **Why:** line (the reason the user gave — often a past incident or strong preference) and a **How to apply:** line (when/where this guidance kicks in). Knowing *why* lets you judge edge cases instead of blindly following the rule.</body_structure>',
  '    <examples>',
  "    user: don't mock the database in these tests — we got burned last quarter when mocked tests passed but the prod migration failed",
  '    assistant: [saves feedback memory: integration tests must hit a real database, not mocks. Reason: prior incident where mock/prod divergence masked a broken migration]',
  '',
  '    user: stop summarizing what you just did at the end of every response, I can read the diff',
  '    assistant: [saves feedback memory: this user wants terse responses with no trailing summaries]',
  '    </examples>',
  '</type>',
  '<type>',
  '    <name>project</name>',
  '    <description>Information you learn about ongoing work, goals, initiatives, bugs, or incidents within the project that is not otherwise derivable from the code or git history. Project memories help you understand the broader context and motivation behind the work in this working directory.</description>',
  '    <when_to_save>When you learn who is doing what, why, or by when. These states change relatively quickly so keep your understanding up to date. Always convert relative dates to absolute dates when saving (e.g., "Thursday" → "2026-03-05"), so the memory remains interpretable later.</when_to_save>',
  "    <how_to_use>Use these memories to more fully understand the details and nuance behind the user's request and make better informed suggestions.</how_to_use>",
  '    <scope>Bias toward team — project context (who/what/why/when, incidents, decisions) usually benefits every collaborator in this directory. Keep it private only when it is specific to how YOU work rather than the project.</scope>',
  '    <body_structure>Lead with the fact or decision, then a **Why:** line (the motivation — often a constraint, deadline, or stakeholder ask) and a **How to apply:** line (how this should shape your suggestions). Project memories decay fast, so the why helps future-you judge whether the memory is still load-bearing.</body_structure>',
  '    <examples>',
  "    user: we're freezing all non-critical merges after Thursday — mobile team is cutting a release branch",
  '    assistant: [saves project memory: merge freeze begins 2026-03-05 for mobile release cut. Flag any non-critical PR work scheduled after that date]',
  '',
  "    user: the reason we're ripping out the old auth middleware is that legal flagged it for storing session tokens in a way that doesn't meet the new compliance requirements",
  '    assistant: [saves project memory: auth middleware rewrite is driven by legal/compliance requirements around session token storage, not tech-debt cleanup — scope decisions should favor compliance over ergonomics]',
  '    </examples>',
  '</type>',
  '<type>',
  '    <name>reference</name>',
  '    <description>Stores pointers to where information can be found in external systems, so you remember where to look for up-to-date information outside the project directory.</description>',
  '    <when_to_save>When you learn about resources in external systems and their purpose. For example, that bugs are tracked in a specific Linear project or that feedback lives in a specific Slack channel.</when_to_save>',
  '    <how_to_use>When the user references an external system or information that may be in an external system.</how_to_use>',
  '    <scope>Usually team — pointers to shared external systems (issue trackers, dashboards, channels) help the whole team. Keep a pointer private only when it is personal or carries a credential.</scope>',
  '    <examples>',
  '    user: check the Linear project "INGEST" if you want context on these tickets, that\'s where we track all pipeline bugs',
  '    assistant: [saves reference memory: pipeline bugs are tracked in Linear project "INGEST"]',
  '',
  "    user: the Grafana board at grafana.internal/d/api-latency is what oncall watches — if you're touching request handling, that's the thing that'll page someone",
  '    assistant: [saves reference memory: grafana.internal/d/api-latency is the oncall latency dashboard — check it when editing request-path code]',
  '    </examples>',
  '</type>',
  '</types>',
].join('\n');

/**
 * `### What NOT to save` list with per-item reasons + the eval-validated
 * explicit-save gate (Claude Code `WHAT_NOT_TO_SAVE_SECTION`). The gate prevents
 * "save this week's PR list" turning memory into an activity log even when the
 * user explicitly asks.
 */
const WHAT_NOT_TO_SAVE_GUIDANCE = [
  '### What NOT to save',
  '- Code patterns, conventions, architecture, file paths, or project structure — these can be derived by reading the current project state.',
  '- Git history, recent changes, or who-changed-what — `git log` / `git blame` are authoritative.',
  '- Debugging solutions or fix recipes — the fix is in the code; the commit message has the context.',
  '- Anything already documented in instruction files (CLAUDE.md / AGENTS.md / SUPERINFERENCE.md).',
  '- Ephemeral task details: in-progress work, temporary state, current conversation context.',
  '',
  'These exclusions apply even when the user explicitly asks you to save. If they ask you to save a PR list or activity summary, ask what was *surprising* or *non-obvious* about it — that is the part worth keeping.',
].join('\n');

/**
 * `### Memory scope` block distinguishing the two storage scopes (Claude Code
 * teamMemPrompts.ts `## Memory scope`). The model must DECIDE, per memory, whether
 * it is private (only this user) or team (shared with everyone in this project dir,
 * synced at session start) — and it must never put secrets in a shared team memory.
 */
const MEMORY_SCOPE_GUIDANCE = [
  '### Memory scope',
  'Every memory you save has a `scope` — WHERE it is stored — in addition to its type:',
  '- **private** (the default): known only to you and the current user; persists across this user\'s sessions. Use for user-specific preferences and anything personal to this user.',
  '- **team**: shared with and contributed to by EVERYONE working in this project directory, and synced at the start of every session. Use for durable project/reference context that benefits every collaborator.',
  'Choose the scope per the memory\'s nature. You MUST NOT save sensitive data in a team memory — never save API keys or user credentials to shared/team memory, since team memories are visible to all collaborators.',
  'Tailor what you write to the scope: keep **private** memories general to you and this user (they apply across your work here); make **team** memories durable and specific to THIS project (they are checked into the shared project directory and read by everyone). Before saving a private memory that restates project guidance, check it does not contradict an existing team memory — reconcile with the team memory rather than keeping a divergent private copy.',
].join('\n');

/**
 * `### When to access memories` block including the eval-validated "ignore
 * memory" anti-pattern bullet (Claude Code `WHEN_TO_ACCESS_SECTION`): when the
 * user says to ignore memory, proceed as if the index were empty rather than
 * acknowledging-then-overriding.
 */
const WHEN_TO_ACCESS_GUIDANCE = [
  '### When to access memories',
  '- When memories seem relevant, or the user references prior-conversation work.',
  '- You MUST access memory when the user explicitly asks you to check, recall, or remember.',
  '- If the user says to *ignore* or *not use* memory: proceed as if MEMORY.md were empty. Do not apply remembered facts, cite, compare against, or mention memory content.',
].join('\n');

/**
 * `### Before recommending from memory` verification checklist (Claude Code
 * `TRUSTING_RECALL_SECTION`): a memory naming a function/file/flag is a claim
 * about when it was written, not proof it exists now — verify before acting.
 */
const BEFORE_RECOMMENDING_GUIDANCE = [
  '### Before recommending from memory',
  'A memory that names a specific function, file, or flag is a claim that it existed *when the memory was written*. It may have been renamed, removed, or never merged. Before recommending it:',
  '- If the memory names a file path: check the file exists.',
  '- If the memory names a function or flag: grep for it.',
  '- If the user is about to act on your recommendation (not just asking about history), verify first.',
  '',
  '"The memory says X exists" is not the same as "X exists now."',
  '',
  'A memory that summarizes repo state (activity logs, architecture snapshots) is frozen in time. If the user asks about *recent* or *current* state, prefer `git log` or reading the code over recalling the snapshot.',
].join('\n');

/**
 * `### Memory and other forms of persistence` block (Claude Code
 * memdir/memdir.ts `buildMemoryLines` + teamMemPrompts.ts). Memory is for
 * information useful in FUTURE conversations; a Plan (reaching alignment on a
 * non-trivial approach) and Tasks (breaking the current conversation's work into
 * steps / tracking progress) are for the CURRENT conversation. This steers the
 * model away from polluting durable memory with ephemeral approach/progress
 * state and toward AMI's plan-mode / task surfaces instead.
 */
const PERSISTENCE_GUIDANCE = [
  '### Memory and other forms of persistence',
  'Memory is one of several persistence mechanisms. The distinction: memory is recalled in FUTURE conversations and must not hold information only useful within the CURRENT conversation.',
  '- Use (or update) a **plan** instead of memory when you are about to start a non-trivial implementation and want to align on the approach, or when your approach changes mid-task — update the plan, do not save the change to memory.',
  '- Use (or update) **tasks** instead of memory to break the current conversation into discrete steps and track progress. Tasks persist work for THIS conversation; memory is reserved for what will still be useful in future ones.',
].join('\n');

/**
 * Frontmatter example with the body-structure scaffold (Claude Code
 * `MEMORY_FRONTMATTER_EXAMPLE`) — shows the model the exact shape of a good
 * saved memory, including the **Why:** / **How to apply:** body scaffolding for
 * feedback/project types.
 */
const MEMORY_FRONTMATTER_EXAMPLE = [
  '```markdown',
  '---',
  'name: {{memory name}}',
  'description: {{one-line description — used to decide relevance in future conversations, so be specific}}',
  `type: {{${MEMORY_TYPES.join(', ')}}}`,
  '---',
  '',
  '{{memory content — for feedback/project types, structure as: rule/fact, then **Why:** and **How to apply:** lines}}',
  '```',
].join('\n');

/** An on-disk memory file whose size exceeds {@link MAX_MEMORY_CHARACTER_COUNT}. */
export interface LargeMemoryFile {
  /** Absolute path to the oversized memory file. */
  path: string;
  /** Character length of the file's raw content. */
  chars: number;
}

/**
 * A user-facing memory/instruction-file HEALTH diagnostic produced by
 * {@link MemoryManager.buildMemoryDiagnostics}. Parity with Claude Code's
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
   * instruction file (CLAUDE.md / AGENTS.md / `.ami/rules/*`). Mirrors Claude
   * Code's `isAutoManagedMemoryFile` classification so a UI can route a managed
   * file to `/memory` and an instruction file to a plain editor.
   */
  managed?: boolean;
}

/**
 * A machine-readable summary of everything the memory/instruction loader pulled
 * into context for a cwd, produced by {@link MemoryManager.getMemoryLoadSummary}.
 * Parity with Claude Code's `memory_files_completed` / `tengu_claudemd__initial_load`
 * introspection events (utils/claudemd.ts:L1014-L1040): rather than the context cost
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
 * The unified memory-source taxonomy — WHERE an instruction/memory originated,
 * as a first-class label distinct from its {@link MemoryType}. Parity with Claude
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
const INSTRUCTION_FILES = [
  'CLAUDE.md',
  'AGENTS.md',
  'SUPERINFERENCE.md',
  'GEMINI.md',
  'CRUSH.md',
  '.cursorrules',
  '.cursor/rules',
  '.github/copilot-instructions.md',
] as const;

const LOCAL_VARIANTS = [
  'CLAUDE.local.md',
  'SUPERINFERENCE.local.md',
] as const;

/**
 * Per-source provenance/trust labels appended to each instruction-file section
 * header so the model can see WHERE an instruction came from and whether editing
 * it is shared or private. Parity with Claude Code's `getClaudeMds` descriptive
 * suffixes (utils/claudemd.ts) — AMI previously emitted a bare relative-path header.
 */
const PROVENANCE_PROJECT = 'project instructions, checked into the codebase';
const PROVENANCE_LOCAL = 'private project instructions, not checked in';
const PROVENANCE_USER_GLOBAL = "user's global instructions for all projects";

/** Maximum number of parent directories to scan upward. */
const MAX_PARENT_SCAN_DEPTH = 3;

/** Maximum size for instruction files — reject oversized files to prevent context stuffing. */
const MAX_INSTRUCTION_FILE_SIZE = 10_240;

/**
 * Hard caps on the MEMORY.md entrypoint index (mirrors Claude Code's
 * MAX_ENTRYPOINT_LINES / MAX_ENTRYPOINT_BYTES). The entrypoint is injected into
 * the system prompt on every turn, so an unbounded index is a context-stuffing /
 * cost vector. {@link truncateEntrypointContent} enforces both, naming the cap it
 * hit so the overflow is never silent.
 */
export const MAX_ENTRYPOINT_LINES = 200;
export const MAX_ENTRYPOINT_BYTES = 25_000;

/** The MEMORY.md index is a table of contents, not a memory body — it is loaded
 *  separately (readMemoryIndex) and must be excluded from the memory-body scan.
 *  Exported so the single-source entrypoint filename is reused by the
 *  consolidation-prompt builder ({@link ./memory-consolidation}) rather than
 *  re-literalling `'MEMORY.md'`. */
export const MEMORY_INDEX_FILENAME = 'MEMORY.md';

/**
 * Two-step save guidance teaching the MEMORY.md index pattern (Claude Code
 * memdir/memdir.ts `## How to save memories`). A memory is NOT a single dump: it
 * is a topic file PLUS a one-line pointer in the always-loaded MEMORY.md index.
 * Without this the model writes memory bodies straight into the index (which is
 * line/byte-capped — see {@link MAX_ENTRYPOINT_LINES}) and never learns the
 * one-line-hook entry format {@link MemoryManager.addToMemoryIndex} expects.
 */
const TWO_STEP_SAVE_GUIDANCE = [
  '### How to save a memory (two steps)',
  'Saving a memory is a two-step process:',
  '',
  '**Step 1** — write the memory to its own topic file (e.g. `user_role.md`, `feedback_testing.md`) using the frontmatter format above. Organize by topic, not chronologically.',
  `**Step 2** — add a one-line pointer to that file in \`${MEMORY_INDEX_FILENAME}\`. \`${MEMORY_INDEX_FILENAME}\` is an INDEX, not a memory: each entry is a single line under ~150 characters — \`- [Title](file.md) — one-line hook\` — and it has no frontmatter. Never write memory content directly into \`${MEMORY_INDEX_FILENAME}\`.`,
  '',
  `- \`${MEMORY_INDEX_FILENAME}\` is loaded into context every turn; entries past ${MAX_ENTRYPOINT_LINES} lines are truncated, so keep the index concise.`,
  '- Keep each memory file\'s name / description / type fields in sync with its content.',
  '- Update or remove memories that turn out to be wrong or outdated.',
  '- Do not write duplicate memories — check for an existing memory to update before writing a new one.',
  '- If the user asks you to forget something, use `forget_memory` to find and remove the relevant entry.',
].join('\n');

/**
 * Recommended maximum character count for a single memory file (parity with
 * Claude Code's `MAX_MEMORY_CHARACTER_COUNT`, utils/claudemd.ts). Files over this
 * are not silently clipped — {@link MemoryManager.getLargeMemoryFiles} surfaces
 * them and {@link MemoryManager.getMemoryContext} emits a visible performance
 * warning so the user can split/trim rather than having context silently crowded.
 */
export const MAX_MEMORY_CHARACTER_COUNT = 40_000;

/**
 * Environment variable that redirects the ENTIRE auto-memory directory to an
 * explicit absolute path, bypassing the computed `{config}/projects/{slug}/memory`
 * location. Parity with Claude Code's full-path memory override
 * (`CLAUDE_COWORK_MEMORY_PATH_OVERRIDE` / `CLAUDE_CODE_REMOTE_MEMORY_DIR`,
 * memdir/paths.ts): lets an SDK/host mount memory on a space-scoped volume where
 * the per-session cwd would otherwise fragment the project key. The value is
 * tilde-expanded and boundary-validated (shared {@link isDangerousStorageRoot})
 * so a near-root / relative / UNC / NUL path can never silently redirect every
 * memory write; a rejected value falls through to the default location.
 */
const MEMORY_DIR_OVERRIDE_ENV = 'AMI_MEMORY_DIR';

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
 * Parity with Claude Code's `claudeMdExcludes` setting (utils/claudemd.ts
 * `isClaudeMdExcluded`): a user can opt specific instruction files OUT of the
 * auto-load (e.g. a vendored `**\/node_modules/**\/CLAUDE.md`). Only USER /
 * PROJECT / LOCAL instruction files are eligible — team-managed `.ami/rules/`
 * are NEVER excluded, since those are the project's enforced rules. Patterns are
 * separated by comma, colon, or semicolon.
 */
const MEMORY_EXCLUDES_ENV = 'AMI_MEMORY_EXCLUDES';

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
 * Extra memory guidelines injected verbatim into the memory-guidance prompt
 * block. Parity with Claude Code's `CLAUDE_COWORK_MEMORY_EXTRA_GUIDELINES`
 * (memdir.ts `extraGuidelines`): a host/SDK can append organization-specific
 * memory rules without patching the prompt builder. The value is memory-content
 * sanitized (system-tag neutralization + invisible-char stripping) so a hostile
 * env value cannot inject fake system tags into the prompt.
 */
const MEMORY_EXTRA_GUIDELINES_ENV = 'AMI_MEMORY_EXTRA_GUIDELINES';

/** Resolve {@link MEMORY_EXTRA_GUIDELINES_ENV} (sanitized), or '' when unset. */
export function resolveMemoryExtraGuidelines(env: NodeJS.ProcessEnv = process.env): string {
  const raw = env[MEMORY_EXTRA_GUIDELINES_ENV];
  return raw && raw.trim() ? sanitizeMemoryContent(raw.trim()) : '';
}

/**
 * Opt-in env flag gating whether instruction files (CLAUDE.md / AGENTS.md /
 * SUPERINFERENCE.md …) from ADDITIONAL directories — the extra roots a user grants
 * read access to via `--add-dir` / `--context-file` (the engine's
 * `allowedReadRoots`), beyond cwd and its parents — also contribute their
 * instructions to the system prompt. Parity with Claude Code's
 * `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD` (utils/claudemd.ts): a granted
 * read-access dir does NOT silently inject its instruction file by default — the
 * user must opt in with a truthy value. When OFF, the additional directories still
 * grant tool read-access (unchanged) but add nothing to the prompt.
 */
const ADDITIONAL_INSTRUCTION_DIRS_ENV = 'AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD';

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
 * Claude Code's errno-discriminated memory reads (SessionMemory/sessionMemoryUtils).
 */
export function isExpectedMissing(err: unknown): boolean {
  const code = errnoCode(err);
  return code === 'ENOENT' || code === 'ENOTDIR';
}

/** Extract a NodeJS errno string from an unknown thrown value ('' when absent). */
function errnoCode(err: unknown): string {
  return err && typeof err === 'object' && 'code' in err
    ? String((err as NodeJS.ErrnoException).code)
    : '';
}

/**
 * Log a memory file-read failure that is NOT an expected "missing" case, so a
 * corrupt / permission-denied memory file is never silently equated with an
 * empty one. A no-op for ENOENT/ENOTDIR (the silent fallback).
 */
function logUnexpectedReadError(err: unknown, filePath: string): void {
  if (isExpectedMissing(err)) return;
  coreLog('memory', `memory read failed for ${redactLogPath(filePath)}`, { code: errnoCode(err) || 'unknown' }, 'warn');
}

/**
 * Owner-only permissions for the auto-memory tree. Parity with Claude Code's
 * session-memory hardening (0o700 dir / 0o600 file): memory can hold personal
 * context (user profile, preferences, project state) and must not be group- or
 * world-readable. The mode is passed to mkdir/writeFile AND re-applied with an
 * explicit chmod — mkdir's mode is masked by the umask and ignored entirely when
 * the directory already exists, so the chmod is what actually defeats a loose
 * umask on a pre-existing memory dir/file.
 */
const MEMORY_DIR_MODE = 0o700;
const MEMORY_FILE_MODE = 0o600;

/** mkdir -p the memory dir with owner-only mode, then chmod to defeat the umask
 *  / tighten an existing dir. Best-effort chmod: a filesystem that rejects the
 *  mode (e.g. a mounted volume) must not abort the memory write. */
function ensureMemoryDirMode(dir: string): void {
  try {
    fs.mkdirSync(dir, { recursive: true, mode: MEMORY_DIR_MODE });
  } catch (err) {
    // Defensive mkdir with errno classification (parity with Claude Code's
    // error-code-discriminated memory write path): a read-only mount (EROFS),
    // permission-denied (EACCES/EPERM), or a path colliding with an existing
    // FILE (EEXIST/ENOTDIR) is diagnosed — naming the (home-redacted) dir and
    // the errno — rather than surfacing as an opaque throw to the caller. The
    // error is re-thrown so a failed memory write is never silently swallowed.
    coreLog('memory', `failed to create memory directory ${redactLogPath(dir)}`, { code: errnoCode(err) || 'unknown' }, 'warn');
    throw err;
  }
  try { fs.chmodSync(dir, MEMORY_DIR_MODE); } catch { /* best effort — mode not enforceable here */ }
}

/**
 * Create `filePath` as an empty, owner-only file IF it does not already exist,
 * PRESERVING the content of an existing file. Uses the exclusive-create `wx`
 * flag (parity with Claude Code's `writeFile(path, '', { flag: 'wx' })` in
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
function createFileIfAbsent(filePath: string): boolean {
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
 * the memory save/recall guidance block). Parity with Claude Code's
 * `CLAUDE_CODE_DISABLE_AUTO_MEMORY` (memdir/paths.ts `isAutoMemoryEnabled`):
 * project/user INSTRUCTION files (CLAUDE.md / AGENTS.md) are NOT auto-memory and
 * always load; only the persistent memory system is gated.
 */
const DISABLE_AUTO_MEMORY_ENV = 'AMI_DISABLE_AUTO_MEMORY';

/**
 * Whether the auto-memory system is enabled. Priority chain (first that applies
 * wins), mirroring Claude Code's `isAutoMemoryEnabled`:
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
 * The independently-gateable instruction/memory SOURCE tiers. Parity with Claude
 * Code's per-source `isSettingSourceEnabled('userSettings' | 'projectSettings' |
 * 'localSettings')` gate (utils/claudemd.ts): a host/SDK can disable whole
 * instruction sources (e.g. a sandboxed run that must ignore checked-in project
 * instructions, or a run that loads nothing user-global).
 *  - `user`: user-global instructions (`~/.ami`, `~/.claude/CLAUDE.md`).
 *  - `project`: checked-in project instruction files (CLAUDE.md / AGENTS.md / …).
 *  - `local`: gitignored project-local override variants (`*.local.md`).
 * Team-managed `.ami/rules/` are NOT a gateable source here — those are the
 * project's enforced rules and always load (mirroring the exclude-list rule that
 * never excludes managed rules).
 */
export type MemorySource = 'user' | 'project' | 'local';

/** Per-tier disable env var, mirroring {@link DISABLE_AUTO_MEMORY_ENV}. */
const MEMORY_SOURCE_DISABLE_ENV: Record<MemorySource, string> = {
  user: 'AMI_DISABLE_USER_MEMORY',
  project: 'AMI_DISABLE_PROJECT_MEMORY',
  local: 'AMI_DISABLE_LOCAL_MEMORY',
};

/**
 * Whether a given instruction/memory SOURCE tier should load. Priority chain
 * (first that applies wins), mirroring {@link isAutoMemoryEnabled} and Claude
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

const ENABLE_TEAM_MEMORY_ENV = 'AMI_ENABLE_TEAM_MEMORY';

/**
 * Whether the shared TEAM-memory capability is enabled. Parity with Claude
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
function realpathDeepest(p: string): string | null {
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
function realpathWithin(target: string, root: string): string | null {
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
function isGitRoot(dir: string): boolean {
  try {
    const st = fs.statSync(path.join(dir, '.git'));
    return st.isDirectory() || st.isFile();
  } catch {
    return false;
  }
}

const INJECTION_PATTERNS = [
  /ignore .{0,10}previous instructions/i,
  /you are now /i,
  /new instructions?:/i,
  /\[SYSTEM\]/i,
  /override safety/i,
  /disregard .{0,10}(safety|rules|instructions)/i,
  /pretend you are/i,
  /from now on.{0,3}you/i,
];

/** Strip zero-width characters and apply NFKD normalization to defeat homoglyph/invisible-char bypasses. */
function normalizeForScanning(text: string): string {
  // Strip all Unicode format (Cf), control (Cc), and nonspacing mark (Mn) characters
  // This catches RTL overrides, null bytes, zero-width chars, combining marks
  // eslint-disable-next-line security/detect-bidi-characters
  let normalized = text.replace(/[--­؀-؅؜۝܏࣢᠎​-‏‪-‮⁠-⁤⁦-⁯︀-️﻿￰-￸￾￿]/g, '');
  // NFKD normalization — decomposes ligatures and compatibility characters
  normalized = normalized.normalize('NFKD');
  return normalized;
}

/**
 * Strip block-level HTML comments (`<!-- … -->`) from instruction/memory
 * content before it is injected into the prompt. Parity with Claude Code's
 * `stripHtmlComments` (utils/claudemd.ts): authorial notes written as HTML
 * comments are editorial, not instructions, and should not reach the model.
 *
 * Preserved verbatim (never stripped):
 *  - comments inside FENCED code blocks (```… ```), which are documentation of
 *    the comment syntax itself;
 *  - comments inside INLINE code spans (`` `…` ``);
 *  - an UNCLOSED `<!--` (no matching `-->`) — a typo must not silently swallow
 *    the rest of the file, so only well-formed spans are removed.
 */
export function stripHtmlComments(content: string): string {
  if (!content.includes('<!--')) return content;
  // Mask code so comment-like text inside it is untouchable, strip the real
  // comment spans, then restore the code. NUL sentinels can't occur in the masks
  // (fenced/inline code), and are fully restored before returning.
  const fenced: string[] = [];
  let masked = content.replace(/```[\s\S]*?```/g, (m) => {
    fenced.push(m);
    return `\u0000F${fenced.length - 1}\u0000`;
  });
  const inline: string[] = [];
  masked = masked.replace(/`[^`\n]*`/g, (m) => {
    inline.push(m);
    return `\u0000I${inline.length - 1}\u0000`;
  });
  masked = masked.replace(/<!--[\s\S]*?-->/g, '');
  masked = masked.replace(/\u0000I(\d+)\u0000/g, (_m, n) => inline[Number(n)]);
  masked = masked.replace(/\u0000F(\d+)\u0000/g, (_m, n) => fenced[Number(n)]);
  return masked;
}

function sanitizeContent(content: string, filePath: string): string {
  // Drop editorial HTML comments FIRST (before the size cap) so comment bulk
  // doesn't count toward the truncation budget and comment text never reaches
  // the injection scanner / the model.
  content = stripHtmlComments(content);
  if (content.length > MAX_INSTRUCTION_FILE_SIZE) {
    coreLog('memory', `Instruction file ${redactLogPath(filePath)} exceeds ${MAX_INSTRUCTION_FILE_SIZE} bytes — truncating`);
    let cut = content.slice(0, MAX_INSTRUCTION_FILE_SIZE);
    // Cut back to the last newline inside the cap so an instruction/memory file is
    // never sliced mid-line (which can split a markdown construct or a sentence and
    // feed the model a corrupt half-line). Fall back to the raw slice only when the
    // kept prefix has no newline at all. Append a visible marker so the truncation
    // is never silent — mirrors the entrypoint cap's named-warning behavior.
    const nl = cut.lastIndexOf('\n');
    if (nl > 0) cut = cut.slice(0, nl);
    content = `${cut}\n[truncated — ${path.basename(filePath)} exceeded the ${MAX_INSTRUCTION_FILE_SIZE}-byte cap]`;
  }

  const lines = content.split('\n');
  const clean: string[] = [];
  let injectionFound = false;
  for (const line of lines) {
    const normalized = normalizeForScanning(line);
    if (INJECTION_PATTERNS.some(p => p.test(normalized))) {
      injectionFound = true;
      clean.push(`[BLOCKED: potential prompt injection removed]`);
    } else {
      clean.push(line);
    }
  }
  if (injectionFound) {
    coreLog('memory', `Prompt injection patterns detected in ${redactLogPath(filePath)}`);
  }
  return clean.join('\n');
}

/**
 * Check whether a file path matches any of the given glob patterns from a rule's
 * `paths:` frontmatter. Supports `*` (any characters) and `?` (single character).
 */
export function matchesPaths(pathGlobs: string[], currentFile: string): boolean {
  for (const glob of pathGlobs) {
    // Escape regex special chars except * and ?, then convert glob wildcards
    let escaped = glob.replace(/[.+^${}()|[\]\\]/g, '\\$&');
    escaped = escaped.replace(/\*\*/g, '⧫⧫');
    const pattern = escaped.replace(/\*/g, '[^/]*').replace(/⧫⧫/g, '.*').replace(/\?/g, '[^/]');
    if (new RegExp(`^${pattern}$`).test(currentFile)) return true;
  }
  return false;
}

/**
 * Whether an instruction file is excluded by the {@link MEMORY_EXCLUDES_ENV}
 * glob list. Matched against the raw path, its forward-slash form, AND the
 * symlink-resolved realpath (both forms) so an exclude written against a logical
 * path still matches when the cwd resolves through a symlink (e.g. `/tmp` →
 * `/private/tmp`). Parity with Claude Code's `isClaudeMdExcluded`
 * realpath-expanded matching.
 */
function isInstructionExcluded(filePath: string, excludes: string[]): boolean {
  if (excludes.length === 0) return false;
  const candidates = new Set<string>([filePath, filePath.replace(/\\/g, '/')]);
  const real = realpathDeepest(filePath);
  if (real) {
    candidates.add(real);
    candidates.add(real.replace(/\\/g, '/'));
  }
  for (const c of candidates) {
    if (matchesPaths(excludes, c)) return true;
  }
  return false;
}

/**
 * Parse a multi-value frontmatter field (comma-separated or YAML list).
 * Handles both `paths: src/*.ts, lib/*.ts` and multi-line YAML arrays.
 */
export function parseFrontmatterList(value: string): string[] {
  if (!value) return [];
  return value.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

/**
 * Normalize a rule's `paths:` globs the way Claude Code's `parseFrontmatterPaths`
 * does (utils/claudemd.ts): if EVERY pattern is the match-all `**` (after ignoring a
 * trailing `/**`, since `path` and `path/**` denote the same membership), the rule is
 * UNCONDITIONAL — `null` is returned so the caller loads it eagerly instead of routing
 * it through the per-file conditional filter.
 *
 * The *matching* globs are returned UNCHANGED (trailing `/**` preserved): AMI's
 * {@link matchesPaths} does a full-string regex match where `src/**` already matches
 * `src/foo.ts` via `**`→`.*`, so stripping `/**` to `src` (which would full-match only
 * the literal string `src`) would break subtree matching. The `/**` strip is therefore
 * applied ONLY to the unconditional decision, not to the patterns handed to the matcher.
 *
 * @returns the glob list to match against, or `null` when the rule applies to all files
 *   (empty after filtering, or every pattern is a match-all double-star).
 */
export function normalizeFrontmatterPaths(globs: string[]): string[] | null {
  const kept = globs.filter(p => p.length > 0);
  const stripped = kept.map(p => (p.endsWith('/**') ? p.slice(0, -3) : p)).filter(p => p.length > 0);
  if (stripped.length === 0 || stripped.every(p => p === '**')) return null;
  return kept;
}

/** Directory name for SuperInference memory storage. */

const STALE_MEMORY_DAYS = 30;

export function memoryAgeDays(mtimeMs: number): number {
  // Clamp to 0: a memory is never "from the future". A file mtime slightly ahead
  // of Date.now() (clock skew, NTP correction, or coarse filesystem mtime
  // granularity) would otherwise yield a negative age and render as the
  // nonsensical "-1 days ago" instead of "today".
  return Math.max(0, Math.floor((Date.now() - mtimeMs) / (1000 * 60 * 60 * 24)));
}

/**
 * Per-read staleness caveat for a memory FILE surfaced through a caller that
 * does NOT add its own system-reminder wrapper (e.g. the file_read tool, which
 * line-numbers and returns raw file content). The note is wrapped in
 * `<system-reminder>…</system-reminder>\n` so the caveat carries system-level
 * trust weight into the transcript rather than reading as ordinary file text a
 * planted memory could spoof. Returns '' for files ≤ 1 day old (not stale).
 *
 * Distinct from read-memory.ts's `memoryFreshnessNote(ageDays)`, which returns a
 * plain-markdown note for a surface that emits the body directly to the model;
 * this variant is for the file_read code path and takes the file mtime so the
 * age is computed via the single canonical {@link memoryAgeDays} clamp. Mirrors
 * Claude Code's memdir/memoryAge.memoryFreshnessNote(mtimeMs).
 */
export function memoryFreshnessReminder(mtimeMs: number): string {
  const ageDays = memoryAgeDays(mtimeMs);
  if (ageDays <= 1) return '';
  return `<system-reminder>Memory file — last modified ${ageDays} days ago. Verify before relying on this data.</system-reminder>\n`;
}

function memoryFreshness(ageDays: number): string {
  if (ageDays === 0) return 'today';
  if (ageDays === 1) return 'yesterday';
  if (ageDays <= 7) return `${ageDays} days ago`;
  if (ageDays <= 30) return `${Math.floor(ageDays / 7)} weeks ago`;
  return `${Math.floor(ageDays / 30)} months ago`;
}

/** Rendering style for {@link formatRelativeTime}. `narrow` uses AMI's compact
 *  custom units (`3d ago`, `in 2h`); `short`/`long` defer to the localized
 *  `Intl.RelativeTimeFormat` for full-word output (`3 days ago`). */
export type RelativeTimeStyle = 'narrow' | 'short' | 'long';

/** Options for {@link formatRelativeTime} / {@link formatRelativeTimeAgo}
 *  (parity with Claude Code's `RelativeTimeOptions`, utils/format.ts). */
export interface RelativeTimeOptions {
  /** @default 'narrow' */
  style?: RelativeTimeStyle;
  /** Whether the localized formatter always emits a number or may say
   *  "yesterday"/"now". @default 'always' */
  numeric?: 'always' | 'auto';
}

/** Interval table (largest → smallest) driving the unit selection in
 *  {@link formatRelativeTime}. Each row carries the full unit (for the localized
 *  `Intl.RelativeTimeFormat`) and the compact `narrow` suffix. */
const RELATIVE_TIME_INTERVALS = [
  { unit: 'year', seconds: 31536000, shortUnit: 'y' },
  { unit: 'month', seconds: 2592000, shortUnit: 'mo' },
  { unit: 'week', seconds: 604800, shortUnit: 'w' },
  { unit: 'day', seconds: 86400, shortUnit: 'd' },
  { unit: 'hour', seconds: 3600, shortUnit: 'h' },
  { unit: 'minute', seconds: 60, shortUnit: 'm' },
  { unit: 'second', seconds: 1, shortUnit: 's' },
] as const;

/** Memoized `Intl.RelativeTimeFormat` instances — constructing one per call is
 *  measurably expensive, and the (style, numeric) space is tiny. Parity with
 *  Claude Code's cached `getRelativeTimeFormat` (utils/intl.ts). */
const relativeTimeFormatCache = new Map<string, Intl.RelativeTimeFormat>();
function getRelativeTimeFormat(
  style: 'short' | 'long' | 'narrow',
  numeric: 'always' | 'auto',
): Intl.RelativeTimeFormat {
  const key = `${style}\u0000${numeric}`;
  let fmt = relativeTimeFormatCache.get(key);
  if (fmt === undefined) {
    fmt = new Intl.RelativeTimeFormat('en-US', { style, numeric });
    relativeTimeFormatCache.set(key, fmt);
  }
  return fmt;
}

/**
 * Format `date` relative to `now` (default: the current time) using an
 * interval-table algorithm. Parity with Claude Code's `formatRelativeTime`
 * (utils/format.ts) + its localized `Intl.RelativeTimeFormat` wrapper
 * (utils/intl.ts): the largest interval whose magnitude the gap meets or exceeds
 * selects the unit (truncating toward zero), so a 90-minute gap renders as the
 * hour unit, not 90 minutes.
 *
 * `narrow` (the default) emits AMI's compact `${value}${shortUnit} ago` / `in
 * ${value}${shortUnit}` form; `short`/`long` defer to the memoized localized
 * `Intl.RelativeTimeFormat` for full-word, locale-aware output. A matched
 * interval ≥ 1 day always renders with the long localized form regardless of
 * `style` (matching the reference), and a sub-second gap degrades to `0s ago` /
 * `in 0s` (narrow) or the localized zero-second string.
 */
export function formatRelativeTime(
  date: Date,
  options: RelativeTimeOptions & { now?: Date } = {},
): string {
  const { style = 'narrow', numeric = 'always', now = new Date() } = options;
  const diffInMs = date.getTime() - now.getTime();
  // Truncate toward zero so a 1.9s future gap is 1s (not rounded up to 2s) and a
  // past gap is handled symmetrically.
  const diffInSeconds = Math.trunc(diffInMs / 1000);

  for (const { unit, seconds, shortUnit } of RELATIVE_TIME_INTERVALS) {
    if (Math.abs(diffInSeconds) >= seconds) {
      const value = Math.trunc(diffInSeconds / seconds);
      if (style === 'narrow') {
        return diffInSeconds < 0
          ? `${Math.abs(value)}${shortUnit} ago`
          : `in ${value}${shortUnit}`;
      }
      // Days-and-longer always read best in the long localized form.
      return getRelativeTimeFormat('long', numeric).format(value, unit);
    }
  }

  // Less than one second in either direction.
  if (style === 'narrow') {
    return diffInSeconds <= 0 ? '0s ago' : 'in 0s';
  }
  return getRelativeTimeFormat(style, numeric).format(0, 'second');
}

/**
 * {@link formatRelativeTime} specialized for a PAST timestamp: forces
 * `numeric: 'always'` for past dates so the output is a definite "X units ago"
 * rather than a localized "yesterday", while a future date falls through to the
 * caller's options unchanged. Parity with Claude Code's `formatRelativeTimeAgo`
 * (utils/format.ts). Used by {@link MemoryManager.buildMemoryDiagnostics} to
 * stamp each health warning with how recently the offending file changed.
 */
export function formatRelativeTimeAgo(
  date: Date,
  options: RelativeTimeOptions & { now?: Date } = {},
): string {
  const { now = new Date(), ...rest } = options;
  if (date > now) {
    return formatRelativeTime(date, { ...rest, now });
  }
  return formatRelativeTime(date, { ...rest, numeric: 'always', now });
}

/**
 * Parse YAML-like frontmatter from a markdown string.
 * Returns the extracted fields and the body after the frontmatter block.
 *
 * Delimiter matching is LINE-ANCHORED (parity with + superset of Claude Code's
 * `FRONTMATTER_REGEX` `/^---\s*\n([\s\S]*?)---\s*\n?/`): both the opening and the
 * closing `---` must occupy a line by themselves (trailing whitespace allowed).
 * This fixes two defects of a naive `startsWith('---')` + `indexOf('---', 3)`:
 *   (a) a run-together `---foo` is NOT a valid opener (needs an end-of-line after
 *       the dashes), and
 *   (b) a `---` substring INSIDE a value (e.g. `description: a --- b`) no longer
 *       prematurely closes the block, so later keys (`type:` …) survive.
 *
 * @param sourcePath optional path of the file being parsed; when a block opens
 *   with `---` but is never closed (malformed frontmatter) a `warn` diagnostic is
 *   logged naming the file, mirroring Claude Code's `logForDebugging(...Failed to
 *   parse YAML frontmatter in ${sourcePath})`, instead of silently swallowing it.
 */
/**
 * Strip a single layer of matching surrounding quotes from a scalar YAML value,
 * unescaping that quote style's escapes. Parity with a real YAML parser's scalar
 * handling (Claude Code `frontmatterParser`): a value serialized quoted to defeat
 * YAML injection (see {@link sanitizeFrontmatterValue}, e.g. `"a: b"`) must read
 * back as `a: b`, not `"a: b"`. Double-quoted values unescape `\"` and `\\`;
 * single-quoted values unescape the YAML `''` → `'`. A value without matching
 * surrounding quotes (or shorter than two chars) is returned unchanged.
 */
export function stripSurroundingQuotes(value: string): string {
  if (value.length >= 2) {
    const q = value[0];
    if ((q === '"' || q === "'") && value[value.length - 1] === q) {
      const inner = value.slice(1, -1);
      return q === '"'
        ? inner.replace(/\\(["\\])/g, '$1')
        : inner.replace(/''/g, "'");
    }
  }
  return value;
}

/**
 * Parse an inline YAML flow sequence (`[a, b, "c, d"]`) into its items, honoring
 * quoted items so a comma INSIDE quotes does not split the list. Each item is
 * unquoted via {@link stripSurroundingQuotes} and empty items are dropped.
 * Returns `null` when `value` is not a well-formed (closed) `[...]` flow sequence
 * — the caller surfaces that as a malformed-frontmatter diagnostic. Parity with a
 * real YAML parser's flow-sequence handling.
 */
export function parseInlineFlowSequence(value: string): string[] | null {
  if (!(value.startsWith('[') && value.endsWith(']'))) return null;
  const inner = value.slice(1, -1).trim();
  if (inner === '') return [];
  const items: string[] = [];
  let buf = '';
  let quote: '"' | "'" | null = null;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (quote) {
      buf += ch;
      if (ch === quote) quote = null;
    } else if (ch === '"' || ch === "'") {
      quote = ch;
      buf += ch;
    } else if (ch === ',') {
      items.push(stripSurroundingQuotes(buf.trim()));
      buf = '';
    } else {
      buf += ch;
    }
  }
  items.push(stripSurroundingQuotes(buf.trim()));
  return items.filter(s => s.length > 0);
}

export function parseFrontmatter(raw: string, sourcePath?: string): {
  frontmatter: Record<string, string>;
  body: string;
} {
  const rawLines = raw.split('\n');
  // Allow leading blank lines before the opener (mirrors the previous trimStart).
  let openIdx = 0;
  while (openIdx < rawLines.length && rawLines[openIdx].trim() === '') openIdx++;
  // Opener must be a line that is EXACTLY `---` (trailing whitespace / CR ok).
  if (openIdx >= rawLines.length || rawLines[openIdx].replace(/\r$/, '').trim() !== '---') {
    return { frontmatter: {}, body: raw };
  }

  // Closer: the first SUBSEQUENT line that is exactly `---` (line-anchored).
  let closeIdx = -1;
  for (let i = openIdx + 1; i < rawLines.length; i++) {
    if (rawLines[i].replace(/\r$/, '').trim() === '---') { closeIdx = i; break; }
  }
  if (closeIdx === -1) {
    // Opened but never closed → malformed. Surface it (named) rather than silently
    // treating the whole file as a frontmatter-less body.
    coreLog(
      'memory',
      `Failed to parse frontmatter${sourcePath ? ` in ${redactLogPath(sourcePath)}` : ''}: unterminated '---' block`,
      undefined,
      'warn',
    );
    return { frontmatter: {}, body: raw };
  }

  const fmBlock = rawLines.slice(openIdx + 1, closeIdx).join('\n').trim();
  const body = rawLines.slice(closeIdx + 1).join('\n').replace(/^\r?\n/, '');

  const frontmatter: Record<string, string> = {};
  const lines = fmBlock.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const colonIdx = line.indexOf(':');
    if (colonIdx === -1) {
      // A line that is neither a `key: value` pair nor an empty/comment line is
      // malformed YAML. AMI previously skipped it SILENTLY, so a typo'd header
      // (e.g. a bare `description` with no colon) vanished with no signal. Surface
      // it (named) rather than dropping it quietly — parity with Claude Code's
      // yaml.ts error surfacing on malformed input. Blank lines, `# comments` and
      // stray `- list items` (which the block-sequence loop consumes) are benign.
      const probe = line.trim();
      if (probe !== '' && !probe.startsWith('#') && !/^-(\s|$)/.test(probe)) {
        coreLog(
          'memory',
          `Failed to parse frontmatter${sourcePath ? ` in ${redactLogPath(sourcePath)}` : ''}: malformed line (no key) '${probe.slice(0, 80)}'`,
          undefined,
          'warn',
        );
      }
      continue;
    }
    const key = line.slice(0, colonIdx).trim();
    if (!key) continue;
    let value = line.slice(colonIdx + 1).trim();

    // YAML block scalars: `key: >` (folded → join with spaces) or `key: |`
    // (literal → keep newlines), with optional chomping indicator (-/+). The
    // value is the subsequent more-indented (or blank) lines. Without this a
    // folded description collapses to a bare ">" sentinel.
    const block = value.match(/^([>|])[-+]?$/);
    if (block) {
      const folded = block[1] === '>';
      const collected: string[] = [];
      while (i + 1 < lines.length && (lines[i + 1].trim() === '' || /^\s/.test(lines[i + 1]))) {
        i++;
        collected.push(lines[i].trim());
      }
      value = folded
        ? collected.join(' ').replace(/\s+/g, ' ').trim()
        : collected.join('\n').trim();
    }

    // YAML block SEQUENCE: `key:` on its own line followed by indented `- item`
    // entries (parity with Claude Code block-list frontmatter values such as
    // `allowed-tools:` / `arguments:`). Collected into a comma-joined scalar so
    // the existing {@link parseFrontmatterList} consumer yields the items. Only
    // triggers when the inline value is empty (a `key: value` scalar wins).
    if (value === '') {
      const items: string[] = [];
      while (i + 1 < lines.length && /^\s+-\s+/.test(lines[i + 1])) {
        i++;
        items.push(lines[i].replace(/^\s+-\s+/, '').trim());
      }
      if (items.length > 0) value = items.join(', ');
    }

    // Inline YAML FLOW SEQUENCE: `key: [a, b, "c, d"]` (parity with a real YAML
    // parser's flow syntax — AMI's hand-rolled parser previously understood only
    // block `- item` sequences, so `paths: [src/*.ts, lib/*.ts]` was read as the
    // literal string `[src/*.ts, lib/*.ts]`). A well-formed `[...]` is normalized
    // into a comma-joined scalar the existing parseFrontmatterList consumer reads;
    // an UNTERMINATED `[` is malformed and surfaced (named) via the diagnostic.
    if (value.startsWith('[')) {
      const seq = parseInlineFlowSequence(value);
      if (seq !== null) {
        value = seq.join(', ');
      } else {
        coreLog(
          'memory',
          `Failed to parse frontmatter${sourcePath ? ` in ${redactLogPath(sourcePath)}` : ''}: unterminated flow sequence for key '${key}'`,
          undefined,
          'warn',
        );
      }
    } else {
      // Scalar: strip a single layer of matching surrounding quotes (a real YAML
      // parser unquotes scalars; AMI previously kept them), so a value serialized
      // quoted to defeat YAML injection (sanitizeFrontmatterValue) round-trips.
      value = stripSurroundingQuotes(value);
    }

    frontmatter[key] = value;
  }

  return { frontmatter, body };
}

/**
 * Validate a raw string as a MemoryType. Returns undefined for invalid values.
 */
function parseMemoryType(raw: unknown): MemoryType | undefined {
  if (typeof raw !== 'string') return undefined;
  return MEMORY_TYPES.find(t => t === raw);
}

/**
 * Collapse a value destined for a single-line YAML frontmatter field so it cannot
 * inject new keys or close the block early. Newlines (the only way to add a YAML
 * line) are flattened to spaces, and a leading char that YAML reads specially is
 * neutralized — so a crafted name/description cannot smuggle `type:`/`---` lines
 * into the serialized metadata. (YAML frontmatter injection defense)
 */
function sanitizeFrontmatterValue(value: string): string {
  let v = value.replace(/\s+/g, ' ').trim();
  // A value opening with a YAML structural/indicator char is quoted so it is read
  // as a plain scalar (not a block/alias/anchor/tag/flow collection).
  if (/^[-?:,[\]{}#&*!|>'"%@`]/.test(v)) {
    v = `"${v.replace(/"/g, '\\"')}"`;
  }
  return v;
}

/**
 * Generate frontmatter markdown for a memory file. Name/description are
 * injection-sanitized so a hostile value cannot forge extra YAML keys.
 */
function buildFrontmatter(
  name: string,
  description: string,
  memoryType: MemoryType,
): string {
  const modified = new Date().toISOString();
  return `---\nname: ${sanitizeFrontmatterValue(name)}\ndescription: ${sanitizeFrontmatterValue(description)}\ntype: ${memoryType}\nmodified: ${modified}\n---\n`;
}

/**
 * Slugify a memory name into its on-disk file-stem. Lower-cased, non-alnum
 * collapsed to a single `_`, leading/trailing `_` trimmed. Shared by
 * {@link MemoryManager.saveMemory} (which derives the write filename) and
 * {@link MemoryManager.deleteMemory} (which resolves which file to remove) so the
 * two never drift into subtly different normalisations (No-Duplication Rule).
 */
export function memoryFileSlug(name: string): string {
  return name
    .replace(/[^a-zA-Z0-9_-]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toLowerCase();
}

/**
 * Resolve a non-clobbering memory file path. Two DISTINCT memory names can
 * normalize to the same `safeName` (e.g. `Foo Bar` and `Foo:Bar` both →
 * `foo_bar`), and {@link MemoryManager.saveMemory} overwrites by filename — so
 * without a guard, saving the second silently destroys the first. Overwriting is
 * correct ONLY when this is an UPDATE of the same memory (the existing file's
 * frontmatter `name` matches `incomingName`, case-insensitively); otherwise a
 * numeric suffix (`foo_bar-2.md`, `-3`, …) is chosen so the unrelated memory is
 * preserved. Mirrors Claude Code's non-destructive (wx + EEXIST) memory write.
 */
function resolveNonClobberingPath(dir: string, filename: string, incomingName: string): string {
  // Normalize the same way the on-disk name is serialized (whitespace-collapse,
  // see sanitizeFrontmatterValue) + case-fold, so an UPDATE with the same memory
  // name reliably overwrites in place even when the name carries collapsible
  // whitespace or was quoted on write and unquoted on read.
  const norm = (s: string): string => s.replace(/\s+/g, ' ').trim().toLowerCase();
  const base = filename.replace(/\.md$/, '');
  const target = norm(incomingName);
  let candidate = path.join(dir, filename);
  let n = 1;
  for (;;) {
    let existing: string;
    try {
      existing = fs.readFileSync(candidate, 'utf-8');
    } catch (err) {
      // Not present (ENOENT) → free to use. Any other read error is logged (so an
      // inaccessible-but-present file is not silently clobbered) and still used,
      // preserving the pre-guard write behavior rather than failing the save.
      logUnexpectedReadError(err, candidate);
      return candidate;
    }
    const { frontmatter } = parseFrontmatter(existing, candidate);
    if (norm(frontmatter.name || '') === target) return candidate;
    n++;
    candidate = path.join(dir, `${base}-${n}.md`);
  }
}

/**
 * Pure index-append rule shared by every MEMORY.md writer (the two-step
 * {@link MemoryManager.saveMemory} and {@link MemoryManager.addToMemoryIndex}).
 * Given the existing index text and a one-line pointer entry, returns the new
 * index content — or `null` when the entry is already present (dedup: never
 * double-write the same pointer). Keeping this as a single canonical rule
 * prevents the two writers from drifting into two subtly different append
 * behaviors.
 */
export function computeIndexAppend(existing: string, entry: string): string | null {
  const clamped = clampIndexEntry(entry);
  if (existing.includes(clamped)) return null;
  return existing.trimEnd() + '\n' + clamped + '\n';
}

/**
 * Recommended maximum length of a single MEMORY.md index pointer line. Parity
 * with Claude Code's consolidation index rule ("if an index line is over ~200
 * chars, it's carrying content that belongs in the topic file"). The whole-index
 * line/byte caps still apply on load ({@link truncateEntrypointContent}).
 */
export const MAX_INDEX_ENTRY_CHARS = 200;

/**
 * Demote an over-long index pointer: a MEMORY.md entry longer than
 * {@link MAX_INDEX_ENTRY_CHARS} is carrying body content that belongs in the
 * topic file, so it is truncated (with an ellipsis) on write rather than bloating
 * the always-loaded index. Shorter entries pass through unchanged. Applied by
 * {@link computeIndexAppend} so every index writer enforces the same per-entry
 * bound (resource-exhaustion defense on untrusted memory writes).
 */
export function clampIndexEntry(entry: string): string {
  if (entry.length <= MAX_INDEX_ENTRY_CHARS) return entry;
  return entry.slice(0, MAX_INDEX_ENTRY_CHARS - 1).trimEnd() + '…';
}

/** Per-file upper bound when scanning the memory dir: a single runaway file must
 *  not be slurped whole into the context/scan. */
const MAX_MEMORY_FILE_BYTES = 256 * 1024;

/**
 * Aggregate file-count ceiling for the shared TEAM memory store. Team memories are
 * committed to version control and contributed by everyone in the project, so —
 * unlike the private store — there is no per-author limit on how many accumulate.
 * Without an aggregate cap a repo carrying many team-memory files could stuff the
 * context unbounded (each body is already {@link MAX_MEMORY_FILE_BYTES}-capped, but
 * count was not). Parity with Claude Code's memdir hard caps on untrusted memory
 * content reaching the prompt; mirrors the primary store's 200-file ceiling in
 * {@link MemoryManager.loadMemories}. */
const MAX_TEAM_MEMORY_FILES = 200;

function tokenize(text: string): string[] {
  return text.toLowerCase()
    .replace(/[^a-z0-9\s_-]/g, ' ')
    .split(/\s+/)
    .filter(t => t.length > 2);
}

/**
 * Signals that a `reference`-type memory is a WARNING/gotcha/known-issue rather
 * than plain usage/API documentation. When a tool is in active use, its usage
 * reference is noise (the conversation already demonstrates it) — but warnings
 * matter MOST precisely then, so a reference memory matching this is never
 * suppressed. Mirrors Claude Code's findRelevantMemories recentTools rule.
 */
const TOOL_WARNING_SIGNAL_RE =
  /\b(warn(?:ing)?s?|gotchas?|caveats?|pitfalls?|cautions?|bugs?|broken|fails?|failure|limitations?|avoid|deprecat\w*|known[ -]issues?|do ?n['’]?t)\b/i;

/**
 * Whether a memory is a pure usage/API reference for a tool that is already in
 * active use, and should therefore be suppressed from relevance selection. Only
 * `reference`-type memories qualify; a memory carrying a warning/gotcha/known-issue
 * signal is preserved even for an active tool (that is exactly when it matters).
 */
function isSuppressedToolReference(mem: MemoryEntry, recentTools: readonly string[]): boolean {
  if (mem.memoryType !== 'reference') return false;
  const hay = `${mem.name || ''} ${mem.description || ''} ${mem.content}`.toLowerCase();
  const mentionsRecentTool = recentTools.some(t => {
    const name = (t || '').trim().toLowerCase();
    return name.length > 0 && hay.includes(name);
  });
  if (!mentionsRecentTool) return false;
  // Keep warnings/gotchas/known-issues about an active tool.
  return !TOOL_WARNING_SIGNAL_RE.test(hay);
}

function computeRelevanceScore(memoryText: string, queryTokens: string[], idfMap: Map<string, number>): number {
  const memTokens = tokenize(memoryText);
  if (memTokens.length === 0) return 0;

  const tf = new Map<string, number>();
  for (const t of memTokens) {
    tf.set(t, (tf.get(t) || 0) + 1);
  }

  let score = 0;
  const k1 = 1.2;
  const b = 0.75;
  const avgDl = 50;

  for (const qt of queryTokens) {
    const freq = tf.get(qt) || 0;
    if (freq === 0) continue;
    const idf = idfMap.get(qt) || 1;
    const tfNorm = (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * memTokens.length / avgDl));
    score += idf * tfNorm;
  }

  return score;
}

// ---------------------------------------------------------------------------
// Per-agent persistent memory (parity: Claude Code AgentTool/agentMemory.ts).
// A spawned agent that declares a `memory` scope in its definition gets its OWN
// file-based memory store, namespaced by agent type and separate from the global
// auto-memory. Scopes: `user` (cross-project, global config dir), `project`
// (committed to VCS under <repo>/.ami/agent-memory/), `local` (project+machine,
// NOT checked in — <repo>/.ami/agent-memory-local/, redirectable to a mount).
// ---------------------------------------------------------------------------

/**
 * Environment variable redirecting LOCAL-scope agent memory to an external mount
 * (parity: Claude Code's `CLAUDE_CODE_REMOTE_MEMORY_DIR`). When set, local agent
 * memory persists to `<dir>/projects/<project-slug>/agent-memory-local/<agentType>/`
 * — namespaced per project so one mount can serve many repos — instead of the
 * in-repo `.ami/agent-memory-local/`. Only local scope is redirected; `user` and
 * `project` scopes keep their fixed homes.
 */
export const AGENT_REMOTE_MEMORY_DIR_ENV = 'AMI_REMOTE_MEMORY_DIR';

const AGENT_MEMORY_DIRNAME = 'agent-memory';
const AGENT_MEMORY_LOCAL_DIRNAME = 'agent-memory-local';
const AGENT_MEMORY_DISPLAY_NAME = 'Persistent Agent Memory';

/**
 * Sanitize an agent type name for use as a single on-disk directory component.
 * Parity with Claude Code's `sanitizeAgentTypeForPath`: colons (invalid on
 * Windows, used by plugin-namespaced agent types like `my-plugin:my-agent`) map
 * to dashes. AMI additionally neutralizes path separators and `..` traversal runs
 * so an attacker-controlled agent type can never escape its memory directory.
 */
export function sanitizeAgentTypeForPath(agentType: string): string {
  return agentType
    .replace(/:/g, '-')
    .replace(/[/\\]/g, '-')
    .replace(/\.{2,}/g, '-');
}

/** Resolve the LOCAL-scope agent memory dir, honoring {@link AGENT_REMOTE_MEMORY_DIR_ENV}. */
function localAgentMemoryDir(dirName: string, gitRoot: string, env: NodeJS.ProcessEnv): string {
  const remote = env[AGENT_REMOTE_MEMORY_DIR_ENV];
  if (remote && remote.trim() !== '') {
    return path.join(remote.trim(), 'projects', projectSlug(gitRoot), AGENT_MEMORY_LOCAL_DIRNAME, dirName);
  }
  return path.join(gitRoot, '.ami', AGENT_MEMORY_LOCAL_DIRNAME, dirName);
}

/**
 * Absolute directory holding a given agent type's persistent memory for `scope`.
 * Parity with Claude Code's `getAgentMemoryDir`. `project`/`local` scopes key on
 * the CANONICAL git root (not raw cwd) so every worktree of a repo shares one
 * agent-memory store rather than fragmenting it per-worktree (AMI's memory-keying
 * convention; a superset over Claude's raw-cwd keying).
 */
export function getAgentMemoryDir(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  const dirName = sanitizeAgentTypeForPath(agentType);
  const gitRoot = canonicalGitRoot(cwd);
  switch (scope) {
    case 'user':
      return path.join(resolveConfigDir(env), AGENT_MEMORY_DIRNAME, dirName);
    case 'project':
      return path.join(gitRoot, '.ami', AGENT_MEMORY_DIRNAME, dirName);
    case 'local':
      return localAgentMemoryDir(dirName, gitRoot, env);
  }
}

/** Absolute path to an agent type's MEMORY.md entrypoint for `scope`. */
export function getAgentMemoryEntrypoint(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  return path.join(getAgentMemoryDir(agentType, scope, cwd, env), MEMORY_INDEX_FILENAME);
}

const AGENT_MEMORY_SCOPE_NOTE: Record<AgentMemoryScope, string> = {
  user: '- This memory is user-scope and applies across every project — keep learnings general, not project-specific.',
  project: '- This memory is project-scope and shared with your team via version control — tailor learnings to this project.',
  local: '- This memory is local-scope (NOT checked into version control) — tailor learnings to this project and this machine.',
};

/**
 * Build the "Persistent Agent Memory" prompt block for a memory-enabled agent
 * (parity: Claude Code's `loadAgentMemoryPrompt` + `buildMemoryPrompt`). Seeds
 * the agent-memory directory (best-effort mkdir), reads and size-caps the existing
 * MEMORY.md entrypoint, and assembles the save/type/persistence guidance (reusing
 * the SAME guidance constants as the global auto-memory prompt — no duplicate
 * copy) plus a scope-specific note. Synchronous: it runs at agent-spawn time while
 * the subagent's system prompt is assembled.
 */
export function buildAgentMemoryPrompt(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  const memoryDir = getAgentMemoryDir(agentType, scope, cwd, env);
  // Seed the directory so the agent's first write_memory finds it; a read-only /
  // unwritable mount is logged by ensureMemoryDirMode and tolerated here (the
  // write tool recreates parent dirs), so a bad mount never strands the spawn.
  try { ensureMemoryDirMode(memoryDir); } catch { /* best effort — tolerated */ }

  let entrypointContent = '';
  try {
    entrypointContent = fs.readFileSync(path.join(memoryDir, MEMORY_INDEX_FILENAME), 'utf-8');
  } catch { /* no entrypoint yet */ }

  const lines: string[] = [
    `# ${AGENT_MEMORY_DISPLAY_NAME}`,
    '',
    `You have a persistent, file-based memory system for the \`${agentType}\` agent at \`${abbreviateHome(memoryDir)}\`. Build it up across runs so future spawns start already carrying the context they need.`,
    AGENT_MEMORY_SCOPE_NOTE[scope],
    '',
    MEMORY_TYPES_GUIDANCE,
    '',
    WHAT_NOT_TO_SAVE_GUIDANCE,
    '',
    '### Saving a memory',
    'Lead feedback/project memories with the rule/fact, then a **Why:** line and a **How to apply:** line. Use this frontmatter:',
    MEMORY_FRONTMATTER_EXAMPLE,
    '',
    TWO_STEP_SAVE_GUIDANCE,
    '',
    PERSISTENCE_GUIDANCE,
    '',
    `## ${MEMORY_INDEX_FILENAME}`,
    '',
    entrypointContent.trim()
      ? truncateEntrypointContent(entrypointContent)
      : `Your ${MEMORY_INDEX_FILENAME} is currently empty. When you save new memories, they will appear here.`,
  ];
  return lines.join('\n');
}

export class MemoryManager {
  private cwd: string;
  private _hookCallback: ((event: string, data: any) => void) | null = null;
  /** Test seam: filesystem stat used when loading memory files. Defaults to the
   *  real `fs.statSync`; tests may replace it to exercise defensive fallbacks. */
  _statSync: typeof fs.statSync = fs.statSync;
  /** Test seam: recursive directory read used when scanning the rules dir.
   *  Defaults to the real `fsp.readdir`; tests may replace it to drive Dirent
   *  shape fallbacks (e.g. missing `parentPath`). */
  _readdir: typeof fsp.readdir = fsp.readdir;
  /** Lazily-resolved, per-instance cache of the config-sourced instruction-file
   *  exclude globs ({@link ProjectConfig.memoryExcludes}) for {@link cwd}. */
  private _configMemoryExcludes: string[] | null = null;
  /** Absolute ADDITIONAL instruction directories (the engine's `allowedReadRoots`
   *  — dirs granted via `--add-dir` / `--context-file`). Loaded into the prompt only
   *  when {@link additionalInstructionDirsEnabled}. Empty when none supplied. */
  private _additionalInstructionDirs: string[];

  constructor(cwd: string, opts?: { additionalInstructionDirs?: readonly string[] }) {
    this.cwd = cwd;
    // Absolutize + de-duplicate once; a relative entry is resolved against cwd so
    // the later discovery/exclude passes compare canonical paths.
    this._additionalInstructionDirs = [
      ...new Set((opts?.additionalInstructionDirs ?? []).map(d => path.resolve(d))),
    ];
  }

  setHookCallback(cb: (event: string, data: any) => void): void { this._hookCallback = cb; }

  /**
   * The effective instruction-file exclude globs for this workspace: the config
   * waterfall's {@link ProjectConfig.memoryExcludes} UNIONED with the
   * `AMI_MEMORY_EXCLUDES` env list. Resolved once per instance (the config read is
   * memoized by {@link resolveConfiguredMemoryExcludes}'s loaders) and reused across
   * the three instruction-discovery passes. */
  private memoryExcludes(): string[] {
    if (this._configMemoryExcludes === null) {
      this._configMemoryExcludes = resolveConfiguredMemoryExcludes(this.cwd);
    }
    return resolveMemoryExcludes(process.env, this._configMemoryExcludes);
  }

  /**
   * Load project instructions from the workspace root and parent directories.
   *
   * Searches for instruction files (CLAUDE.md, SUPERINFERENCE.md, .cursorrules,
   * .github/copilot-instructions.md) starting from the workspace root and
   * scanning up to MAX_PARENT_SCAN_DEPTH parent directories.
   *
   * For each directory level, only the highest-priority file found is used.
   * Files are concatenated with headers indicating their source path.
   */
  /**
   * Inline Claude-Code-style `@path` imports in instruction content. A bare
   * `@relative/or/~/absolute/path` (at line start or after whitespace, outside
   * fenced code) is replaced with the referenced file's content, recursively.
   * Tokens that do not resolve to a real file (e.g. `@param`, `@scope/pkg`) are
   * left untouched. Cycles and depth>5 are guarded.
   */
  resolveAtImports(content: string, basePath: string, depth = 0, seen: Set<string> = new Set()): string {
    if (depth > 5) return content;
    const lines = content.split('\n');
    let inFence = false;
    for (let i = 0; i < lines.length; i++) {
      if (/^\s*```/.test(lines[i])) { inFence = !inFence; continue; }
      if (inFence) continue;
      // Skip INDENTED CODE BLOCKS (≥4 leading spaces or a leading tab). In Markdown
      // these are code, not prose, so an `@path` written inside one is documentation
      // of the import syntax, not an import to resolve — inlining it would be wrong.
      // Parity with Claude Code's lexer-token `@include` extraction, which skips
      // `code` tokens (indented code blocks AND inline code spans alike); AMI's
      // line-based resolver previously tracked only triple-backtick fences.
      if (/^(?: {4}|\t)/.test(lines[i])) continue;
      // Mask INLINE CODE SPANS (`` `…` ``) so an `@path` inside backticks is not
      // inlined (parity with Claude Code's markdown-lexer import extraction, which
      // skips code spans as well as fenced blocks). NUL sentinels can't occur in a
      // code span and are fully restored after the @-replace.
      const spans: string[] = [];
      let line = lines[i].replace(/`[^`\n]*`/g, (m) => {
        spans.push(m);
        return `\u0000C${spans.length - 1}\u0000`;
      });
      // The @-path char class allows an ESCAPED SPACE (`\ `) and a `#fragment`
      // suffix so `@my\ file.md` and `@notes.md#section` are captured whole; the
      // escape is unescaped and the fragment stripped before resolution (parity
      // with Claude Code's safeResolvePath).
      line = line.replace(/(^|\s)@(~?(?:\\ |[\w./~#-])+)/g, (full, pre, p) => {
        // Unescape `\ ` → ` ` and drop a trailing `#fragment` (anchors are not
        // part of the filesystem path).
        let atPath = p.replace(/\\ /g, ' ');
        const hashIdx = atPath.indexOf('#');
        if (hashIdx >= 0) atPath = atPath.slice(0, hashIdx);
        if (atPath === '') return full;
        let resolved: string;
        try {
          resolved = atPath.startsWith('~')
            ? path.join(os.homedir(), atPath.slice(1))
            : path.resolve(path.dirname(basePath), atPath);
        } catch { return full; }
        // WORKSPACE CONFINEMENT: never inline a file outside the launch cwd. A crafted
        // CLAUDE.md/AGENTS.md could otherwise exfiltrate arbitrary files into the system
        // prompt via `@~/.ssh/id_rsa`, `@/etc/passwd`, or `@../../other/secret`. Resolve
        // symlinks FIRST (realpath) so a link that sits inside the workspace but points
        // out of it is also caught — a lexical prefix check misses that. Block it, leave a
        // visible marker (so it is never silent), and notify via the hook.
        const real = realpathWithin(resolved, this.cwd);
        if (real === null) {
          if (this._hookCallback) this._hookCallback('instructionsLoaded', { filePath: resolved, loadReason: 'import-blocked-outside-workspace' });
          return `${pre}[import blocked — outside workspace: ${path.basename(resolved)}]`;
        }
        // Cycle detection keys on the REAL path so two aliases of one file (one via
        // a symlink) can't slip past `seen` and re-enter the recursion.
        if (seen.has(real)) return `${pre}[circular import: ${path.basename(resolved)}]`;
        try {
          if (!fs.statSync(real).isFile()) return full;
          // Deny-by-default extension gate (parity with @include + Claude Code):
          // never inline a binary blob (image/PDF/archive/.so.1) into instructions
          // via `@logo.png` / `@dump.bin`. Extensionless files stay allowed.
          if (!isTextImportPath(real)) {
            return `${pre}[non-text file skipped: ${path.basename(resolved)}]`;
          }
          seen.add(real);
          let imported = sanitizeContent(fs.readFileSync(real, 'utf-8').trim(), real);
          imported = this.resolveAtImports(imported, real, depth + 1, seen);
          seen.delete(real);
          // Fire the InstructionsLoaded hook PER successfully-inlined @-import with
          // loadReason 'include' (parity: Claude Code reports a distinct 'include'
          // load reason for @-included children, not just the top-level files), so a
          // hook can see which files were pulled in via an import vs. loaded directly.
          if (this._hookCallback) this._hookCallback('instructionsLoaded', { filePath: real, loadReason: 'include' });
          return `${pre}${imported}`;
        } catch {
          return full;
        }
      });
      // Restore the masked inline-code spans untouched.
      lines[i] = line.replace(/\u0000C(\d+)\u0000/g, (_m, n) => spans[Number(n)]);
    }
    return lines.join('\n');
  }

  loadProjectInstructions(): string {
    const sections: string[] = [];
    const excludes = this.memoryExcludes();
    // Instruction files already pushed — so an ADDITIONAL directory that happens to
    // equal cwd or one of its scanned parents never double-loads the same file.
    const loaded = new Set<string>();
    let currentDir = path.resolve(this.cwd);
    const root = path.parse(currentDir).root;
    let depth = 0;

    // Per-tier enable gates (parity: Claude Code's per-source isSettingSourceEnabled).
    const projectEnabled = isMemorySourceEnabled('project');
    const localEnabled = isMemorySourceEnabled('local');

    while (depth <= MAX_PARENT_SCAN_DEPTH) {
      const found = projectEnabled ? this.findInstructionFile(currentDir) : null;
      if (found && !isInstructionExcluded(found, excludes)) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            loaded.add(found);
            const content = this.resolveAtImports(sanitizeContent(raw, found), found);
            const relativePath = path.relative(this.cwd, found) || path.basename(found);
            // Per-source provenance/trust label (parity: Claude Code's getClaudeMds
            // descriptive suffixes) — tells the model whether editing this file is
            // version-controlled/shared vs. local, surfacing WHERE the instruction
            // came from rather than a bare path header.
            const prefix = depth === 0 ? '' : '(parent) ';
            const label = `${prefix}${relativePath} (${PROVENANCE_PROJECT})`;
            sections.push(`## ${label}\n\n${content}`);
            // Differentiated InstructionsLoaded load reason + memory_type (parity:
            // Claude Code reports session_start for the first/own load and
            // nested_traversal for a parent-dir file, each tagged memory_type Project)
            // so a hook can tell WHY/HOW each instruction file was pulled in.
            if (this._hookCallback) this._hookCallback('instructionsLoaded', {
              filePath: found,
              loadReason: depth === 0 ? 'session_start' : 'nested_traversal',
              memoryType: 'Project',
            });
          }
        } catch (err) {
          // ENOENT/ENOTDIR (absent) is the silent fallback; a genuine error
          // (EACCES/EIO on a file the user expects loaded) is surfaced via the
          // shared errno classifier instead of vanishing (parity: Claude Code's
          // handleMemoryFileReadError).
          logUnexpectedReadError(err, found);
        }
      }

      if (localEnabled) for (const localFile of LOCAL_VARIANTS) {
        const localPath = path.join(currentDir, localFile);
        if (isInstructionExcluded(localPath, excludes)) continue;
        try {
          if (fs.statSync(localPath).isFile()) {
            const raw = fs.readFileSync(localPath, 'utf-8').trim();
            if (raw) {
              loaded.add(localPath);
              const content = this.resolveAtImports(sanitizeContent(raw, localPath), localPath);
              const relativePath = path.relative(this.cwd, localPath) || path.basename(localPath);
              // Local variants (CLAUDE.local.md / SUPERINFERENCE.local.md) are the
              // private, git-ignored project instructions — label them as such so the
              // model knows edits here are NOT shared with the team.
              const prefix = depth === 0 ? '' : '(parent) ';
              const label = `${prefix}${relativePath} (${PROVENANCE_LOCAL})`;
              sections.push(`## ${label}\n\n${content}`);
              if (this._hookCallback) this._hookCallback('instructionsLoaded', {
                filePath: localPath,
                loadReason: depth === 0 ? 'session_start' : 'nested_traversal',
                memoryType: 'Local',
              });
            }
          }
        } catch (err) {
          // ENOENT/ENOTDIR (absent) silent; EACCES/other surfaced (gap: errno-aware
          // memory-file read error handling).
          logUnexpectedReadError(err, localPath);
        }
      }

      const parentDir = path.dirname(currentDir);
      if (parentDir === currentDir || currentDir === root) {
        break;
      }
      // Stop at the git repository root to avoid loading instruction files from
      // parent projects. `.git` is a DIRECTORY in a normal clone but a FILE in a
      // linked worktree / submodule — stop at either.
      if (isGitRoot(currentDir)) break;
      currentDir = parentDir;
      depth++;
    }

    // ADDITIONAL directories (parity: Claude Code's CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD
    // branch). When opted in and the project tier is enabled, each extra read-root the
    // user granted via `--add-dir` / `--context-file` contributes its highest-priority
    // instruction file too — deduped against what the cwd/parent walk already loaded,
    // exclude-list honored, @-imports resolved. A granted dir that adds no instruction
    // file (or is already loaded) contributes nothing. Off by default.
    if (projectEnabled && this._additionalInstructionDirs.length > 0
        && additionalInstructionDirsEnabled()) {
      for (const dir of this._additionalInstructionDirs) {
        const found = this.findInstructionFile(dir);
        if (!found || loaded.has(found) || isInstructionExcluded(found, excludes)) continue;
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (!raw) continue;
          loaded.add(found);
          const content = this.resolveAtImports(sanitizeContent(raw, found), found);
          const relativePath = path.relative(this.cwd, found) || found;
          const label = `(added dir) ${relativePath} (${PROVENANCE_PROJECT})`;
          sections.push(`## ${label}\n\n${content}`);
          // Distinct load reason so a hook can tell an --add-dir instruction file
          // apart from a cwd/parent one (parity: Claude's additional-directory branch).
          if (this._hookCallback) this._hookCallback('instructionsLoaded', {
            filePath: found,
            loadReason: 'additional_directory',
            memoryType: 'Project',
          });
        } catch (err) {
          logUnexpectedReadError(err, found);
        }
      }
    }

    return sections.join('\n\n');
  }

  /**
   * Find the highest-priority instruction file in a directory.
   * Returns the absolute path if found, null otherwise.
   */
  private findInstructionFile(dir: string): string | null {
    for (const filename of INSTRUCTION_FILES) {
      const filePath = path.join(dir, filename);
      try {
        if (fs.statSync(filePath).isFile()) {
          return filePath;
        }
      } catch (err) {
        // ENOENT/ENOTDIR (this candidate just isn't present) is the normal case;
        // an EACCES/EIO on a candidate that DOES exist is surfaced rather than
        // silently making the file undiscoverable (parity: errno-aware reads).
        logUnexpectedReadError(err, filePath);
      }
    }
    return null;
  }

  /**
   * Resolve instruction files for a specific file path by walking up from the
   * file's directory to the workspace root. Each instruction file is only
   * included once (tracked via `claimedPaths`).
   */
  resolveFileInstructions(filePath: string, claimedPaths: Set<string>): string {
    const sections: string[] = [];
    let dir = path.dirname(path.resolve(this.cwd, filePath));
    const root = path.resolve(this.cwd);

    while (dir === root || dir.startsWith(root + path.sep)) {
      const found = this.findInstructionFile(dir);
      if (found && !claimedPaths.has(found)) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            claimedPaths.add(found);
            const content = this.resolveAtImports(sanitizeContent(raw, found), found);
            sections.push(content);
          }
        } catch (err) {
          // Absent is silent; a genuine read error is surfaced (errno-aware reads).
          logUnexpectedReadError(err, found);
        }
      }
      const parent = path.dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }

    return sections.join('\n\n');
  }

  /**
   * Load all memory entries from the .ami/memory/ directory.
   *
   * Scans for .md files, parses their frontmatter, and returns an array of
   * MemoryEntry objects sorted by modification time (newest first).
   */
  loadMemories(): MemoryEntry[] {
    const memoryDir = this.getMemoryDir();
    let files: string[];

    try {
      // RECURSIVE scan (parity: Claude Code memdir/memoryScan.ts `readdir(dir,
      // { recursive: true })`) so memories organized into topic SUBDIRECTORIES are
      // discovered, not just the flat top level. Returned paths are relative to
      // memoryDir and may contain a subdir segment. Exclusions:
      //  - the MEMORY.md index (ANY level) — it is a table of contents loaded via
      //    readMemoryIndex, never a memory body, so it isn't double-counted;
      //  - the `team/` subtree — team memories are loaded + rendered separately by
      //    loadTeamMemories(), so recursing into them here would duplicate them.
      files = (fs.readdirSync(memoryDir, { recursive: true }) as string[])
        .filter(f =>
          typeof f === 'string' &&
          f.endsWith('.md') &&
          path.basename(f) !== MEMORY_INDEX_FILENAME &&
          f.split(/[\\/]/)[0] !== 'team');
    } catch (err) {
      // ENOENT (dir not created yet) is the expected empty case; a real error
      // (permissions, I/O) is logged rather than silently treated as "no memories".
      logUnexpectedReadError(err, memoryDir);
      return [];
    }

    const MAX_MEMORY_FILES = 200;

    const entries: MemoryEntry[] = [];

    for (const file of files) {
      const filePath = path.join(memoryDir, file);
      try {
        const stat = this._statSync(filePath);
        // Bounded read: skip a runaway memory file rather than slurping it whole.
        if (stat.size > MAX_MEMORY_FILE_BYTES) continue;
        const raw = fs.readFileSync(filePath, 'utf-8');
        const { frontmatter, body } = parseFrontmatter(raw, filePath);
        const sanitizedBody = sanitizeContent(body.trim(), filePath);
        const ageDays = memoryAgeDays(stat.mtimeMs);
        const freshness = memoryFreshness(ageDays);

        let content = sanitizedBody;
        if (ageDays >= STALE_MEMORY_DAYS) {
          content = `[STALE — last updated ${freshness}. Verify before acting on this.]\n\n${sanitizedBody}`;
        }

        entries.push({
          path: filePath,
          content,
          type: 'memory',
          name: frontmatter.name || path.basename(file, '.md'),
          description: frontmatter.description || undefined,
          memoryType: parseMemoryType(frontmatter.type),
          ageDays,
          freshness,
          mtimeMs: stat.mtimeMs,
        });
      } catch {
        // Skip unreadable files
      }
    }

    entries.sort((a, b) => (b.mtimeMs ?? 0) - (a.mtimeMs ?? 0));

    // Cap AFTER the recency sort so the newest memories are kept, not an
    // arbitrary alphabetical (chronological-ascending) prefix of the dir.
    return entries.slice(0, MAX_MEMORY_FILES);
  }

  /**
   * Return the stored memory files whose raw content exceeds the recommended
   * {@link MAX_MEMORY_CHARACTER_COUNT} limit. Parity with Claude Code's
   * `getLargeMemoryFiles` (utils/claudemd.ts): an oversized memory file is
   * SURFACED (see {@link getMemoryContext}) rather than silently clipped, so the
   * user can split or trim it before it degrades performance / crowds the context
   * window. Scans the memory directory recursively (same traversal as
   * {@link loadMemories}), including the MEMORY.md index (a runaway index is just
   * as expensive) and excluding the separately-rendered `team/` subtree.
   */
  getLargeMemoryFiles(): LargeMemoryFile[] {
    const memoryDir = this.getMemoryDir();
    let files: string[];
    try {
      files = (fs.readdirSync(memoryDir, { recursive: true }) as string[])
        .filter(f => typeof f === 'string' && f.endsWith('.md') && f.split(/[\\/]/)[0] !== 'team');
    } catch {
      return [];
    }
    const large: LargeMemoryFile[] = [];
    for (const file of files) {
      const filePath = path.join(memoryDir, file);
      try {
        if (!this._statSync(filePath).isFile()) continue;
        const chars = fs.readFileSync(filePath, 'utf-8').length;
        if (chars > MAX_MEMORY_CHARACTER_COUNT) large.push({ path: filePath, chars });
      } catch {
        // Unreadable file — skip (it cannot be a context-bloat source if it can't load).
      }
    }
    large.sort((a, b) => b.chars - a.chars);
    return large;
  }

  /**
   * Collect the absolute paths of every INSTRUCTION file the loader would read
   * for this cwd — the per-directory highest-priority project file + any present
   * local variants (CLAUDE.local.md / SUPERINFERENCE.local.md) up the parent
   * chain (stopping at the git root, honoring {@link MEMORY_EXCLUDES_ENV}), plus
   * the user-global files (~/.ami, ~/.claude). Shares the exact discovery
   * primitives used by {@link loadProjectInstructions} / {@link loadUserGlobalInstructions}
   * ({@link findInstructionFile}, {@link isInstructionExcluded}, {@link isGitRoot}),
   * so the diagnostic walk can never diverge from what is actually loaded. Returns
   * de-duplicated paths. Used by {@link buildMemoryDiagnostics}.
   */
  private collectInstructionFilePaths(): string[] {
    const found = new Set<string>();
    const excludes = this.memoryExcludes();
    let currentDir = path.resolve(this.cwd);
    const root = path.parse(currentDir).root;
    let depth = 0;
    while (depth <= MAX_PARENT_SCAN_DEPTH) {
      const project = this.findInstructionFile(currentDir);
      if (project && !isInstructionExcluded(project, excludes)) found.add(project);
      for (const localFile of LOCAL_VARIANTS) {
        const localPath = path.join(currentDir, localFile);
        if (isInstructionExcluded(localPath, excludes)) continue;
        try {
          if (fs.statSync(localPath).isFile()) found.add(localPath);
        } catch { /* missing/inaccessible — handled as a read diagnostic below if listed */ }
      }
      const parentDir = path.dirname(currentDir);
      if (parentDir === currentDir || currentDir === root) break;
      if (isGitRoot(currentDir)) break;
      currentDir = parentDir;
      depth++;
    }
    // Mirror the additional-directory instruction files loadProjectInstructions pulls
    // in (parity: when opted in via AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD) so an
    // oversized/unreadable instruction file in an --add-dir root is diagnosed too.
    if (this._additionalInstructionDirs.length > 0 && additionalInstructionDirsEnabled()) {
      for (const dir of this._additionalInstructionDirs) {
        const extra = this.findInstructionFile(dir);
        if (extra && !isInstructionExcluded(extra, excludes)) found.add(extra);
      }
    }
    for (const base of [path.join(os.homedir(), '.ami'), path.join(os.homedir(), '.claude')]) {
      const g = this.findInstructionFile(base);
      if (g) found.add(g);
    }
    return [...found];
  }

  /**
   * Build the user-facing memory/instruction-file health diagnostics for the
   * `/doctor` surface. Parity with Claude Code's `buildMemoryDiagnostics`
   * (utils/status.tsx) + `checkClaudeMdFiles` (utils/doctorContextWarnings.ts):
   * every oversized memory or instruction file is SURFACED with its char count
   * and a `/memory to edit` remediation pointer instead of being silently clipped.
   *
   * Three boundary conditions, all previously invisible to the user:
   *  1. A stored memory file over the recommended {@link MAX_MEMORY_CHARACTER_COUNT}
   *     (reuses {@link getLargeMemoryFiles}, already largest-first).
   *  2. An instruction file (CLAUDE.md / AGENTS.md / local variant / user-global)
   *     over {@link MAX_MEMORY_CHARACTER_COUNT} → the same "will impact performance"
   *     warning; or over {@link MAX_INSTRUCTION_FILE_SIZE} (10 KB) → a "will be
   *     truncated — content is dropped from context" warning (AMI truncates these
   *     silently today).
   *  3. An instruction file that exists but cannot be read (permission/corruption)
   *     → a "cannot read" warning rather than a silent skip.
   *
   * All paths are home-redacted ({@link abbreviateHome}) so a message shown in the
   * terminal / written to a log never leaks the user's home directory. Returns an
   * empty list when every file is healthy.
   */
  buildMemoryDiagnostics(): MemoryDiagnostic[] {
    const diags: MemoryDiagnostic[] = [];
    const n = (v: number): string => v.toLocaleString('en-US');

    // 1. Oversized stored memory files (already sorted largest-first).
    for (const f of this.getLargeMemoryFiles()) {
      diags.push({
        level: 'warn',
        file: f.path,
        chars: f.chars,
        message: `Large ${abbreviateHome(f.path)} will impact performance (${n(f.chars)} chars > ${n(MAX_MEMORY_CHARACTER_COUNT)}) · /memory to edit`,
      });
    }

    // 2/3. Instruction files: oversized / to-be-truncated / unreadable.
    for (const file of this.collectInstructionFilePaths()) {
      let raw: string;
      try {
        raw = fs.readFileSync(file, 'utf-8');
      } catch (err) {
        if (isExpectedMissing(err)) continue;
        diags.push({
          level: 'warn',
          file,
          message: `Cannot read instruction file ${abbreviateHome(file)} (${errnoCode(err) || 'unknown'}) — it will be skipped`,
        });
        continue;
      }
      const chars = raw.length;
      if (chars > MAX_MEMORY_CHARACTER_COUNT) {
        diags.push({
          level: 'warn',
          file,
          chars,
          message: `Large ${abbreviateHome(file)} will impact performance (${n(chars)} chars > ${n(MAX_MEMORY_CHARACTER_COUNT)}) · /memory to edit`,
        });
      } else if (chars > MAX_INSTRUCTION_FILE_SIZE) {
        diags.push({
          level: 'warn',
          file,
          chars,
          message: `${abbreviateHome(file)} is ${n(chars)} chars and will be truncated to the ${n(MAX_INSTRUCTION_FILE_SIZE)}-byte cap — content past the cap is dropped from context · /memory to edit`,
        });
      }
    }

    // Enrich each row with a managed/user-managed classification
    // ({@link isAutoManagedMemoryFile}) and a compact relative mtime
    // ({@link formatRelativeTimeAgo}) so the `/doctor` surface can show HOW STALE
    // an offending file is and whether to route it to `/memory` or a plain editor.
    for (const d of diags) {
      d.managed = this.isAutoManagedMemoryFile(d.file);
      const ago = this.fileModifiedAgo(d.file);
      if (ago !== undefined) d.modifiedAgo = ago;
    }

    return diags;
  }

  /**
   * Compact, locale-aware "last modified" label for `filePath` (e.g. `3d ago`),
   * derived from its mtime via {@link formatRelativeTimeAgo}. Returns `undefined`
   * when the file cannot be stat-ed (missing/inaccessible) so a diagnostic row is
   * never blocked by a failed stat. Used to stamp {@link buildMemoryDiagnostics}.
   */
  private fileModifiedAgo(filePath: string): string | undefined {
    try {
      return formatRelativeTimeAgo(new Date(this._statSync(filePath).mtimeMs));
    } catch {
      return undefined;
    }
  }

  /**
   * Save a memory entry to the .ami/memory/ directory.
   *
   * Creates the memory directory if it doesn't exist. The memory is written
   * as a markdown file with YAML frontmatter containing name, description,
   * and type fields.
   *
   * @param name - Memory name (used as filename and frontmatter name field)
   * @param content - Memory body content
   * @param description - One-line summary for relevance matching
   * @param memoryType - Memory type category (defaults to 'project')
   * @param scope - Storage scope (defaults to 'private'); 'team' routes the file
   *                into the shared `team/` sub-directory read by {@link loadTeamMemories}.
   * @param addToIndex - When true, perform Claude Code's TWO-STEP save: after
   *                writing the topic file, also append a one-line pointer
   *                (`- [name](file.md) — description`) to the memory directory's
   *                `MEMORY.md` index (parity with memdir.ts's two-step pattern +
   *                extractMemories' `skipIndex` option, inverted). Defaults to
   *                false so internal single-step callers (e.g. task-result dumps)
   *                do not clutter the human-facing index; the model-facing
   *                write_memory tool opts in for full two-step parity.
   * @returns The absolute path of the written memory file.
   */
  saveMemory(
    name: string,
    content: string,
    description?: string,
    memoryType?: MemoryType,
    scope: MemoryScope = 'private',
    addToIndex: boolean = false,
  ): string {
    // Route by scope: private memories live at the auto-memory root; team
    // memories live in the shared `team/` sub-directory (the one loadTeamMemories
    // reads), so the model can now AUTHOR a team memory, not just read one.
    const baseDir = this.getMemoryDir();
    const memoryDir = scope === 'team' ? path.join(baseDir, 'team') : baseDir;
    ensureMemoryDirMode(memoryDir);

    const safeName = memoryFileSlug(name);

    const filename = `${safeName}.md`;
    // Non-destructive write: never clobber an UNRELATED memory whose distinct name
    // normalized to the same safeName; disambiguate the filename instead. An update
    // of the SAME memory (matching frontmatter name) still overwrites in place.
    const filePath = resolveNonClobberingPath(memoryDir, filename, name);
    const actualFilename = path.basename(filePath);

    const type = memoryType ?? 'project';
    const desc = description ?? name;
    const frontmatter = buildFrontmatter(name, desc, type);
    // Credential/secret boundary: never persist an API key / token that slipped
    // into a memory body to a long-lived .md on disk.
    const fileContent = `${frontmatter}\n${redactCredentials(content)}\n`;

    // Owner-only file mode + chmod (defeat umask) — a memory file may hold
    // personal context and must not be group/world readable.
    fs.writeFileSync(filePath, fileContent, { encoding: 'utf-8', mode: MEMORY_FILE_MODE });
    try { fs.chmodSync(filePath, MEMORY_FILE_MODE); } catch { /* best effort */ }

    // Step 2 of the two-step save: append a one-line pointer to the always-loaded
    // MEMORY.md index at the auto-memory root (a single index may point into the
    // team/ subtree via a relative path). The index is a table of contents, never
    // a memory body — see TWO_STEP_SAVE_GUIDANCE. Dedup + append are the shared
    // computeIndexAppend rule so this never drifts from addToMemoryIndex.
    if (addToIndex) {
      const relFile = scope === 'team' ? `team/${actualFilename}` : actualFilename;
      const entry = `- [${name}](${relFile}) — ${desc}`;
      const indexPath = path.join(baseDir, MEMORY_INDEX_FILENAME);
      let existing = '';
      try { existing = fs.readFileSync(indexPath, 'utf-8'); } catch { /* no index yet */ }
      const next = computeIndexAppend(existing, entry);
      if (next !== null) {
        ensureMemoryDirMode(baseDir);
        fs.writeFileSync(indexPath, next, { encoding: 'utf-8', mode: MEMORY_FILE_MODE });
        try { fs.chmodSync(indexPath, MEMORY_FILE_MODE); } catch { /* best effort */ }
      }
    }

    return filePath;
  }

  /**
   * Get all memory content formatted for the system prompt.
   *
   * Combines project instructions and stored memories into a single string
   * suitable for injection into the system prompt. Returns empty string if
   * there are no instructions or memories.
   */
  /**
   * Load user-global instruction files (the equivalent of ~/.claude/CLAUDE.md).
   * Reads the first existing instruction file from ~/.ami/ then
   * ~/.claude/, resolving @path imports. Returns '' when none exist.
   */
  loadUserGlobalInstructions(): string {
    // Per-tier enable gate (parity: Claude Code's isSettingSourceEnabled('userSettings')):
    // the user-global instruction tier can be independently disabled.
    if (!isMemorySourceEnabled('user')) return '';
    const sections: string[] = [];
    for (const base of [path.join(os.homedir(), '.ami'), path.join(os.homedir(), '.claude')]) {
      const found = this.findInstructionFile(base);
      if (!found) continue;
      try {
        const raw = fs.readFileSync(found, 'utf-8').trim();
        if (raw) sections.push(this.resolveAtImports(sanitizeContent(raw, found), found));
      } catch (err) {
        // Absent is silent; a genuine read error is surfaced (errno-aware reads).
        logUnexpectedReadError(err, found);
      }
    }
    return sections.join('\n\n');
  }

  /**
   * Delete a stored memory by name — the write-side inverse of
   * {@link saveMemory}. Backs the `forget_memory` tool so the model can honour a
   * "forget X" request instead of only being told to "remove outdated memories"
   * with no way to do it (parity with Claude Code's extractMemories "if they ask
   * you to forget something, find and remove the relevant entry" affordance).
   *
   * Removes the topic file(s) matching `name` in BOTH the private root and the
   * shared `team/` sub-directory (a memory may exist under either scope), matching
   * by the file's frontmatter `name` (case/whitespace-normalised, the same key
   * {@link resolveNonClobberingPath} uses for update-in-place) with a slugged
   * filename fallback. Also prunes the one-line pointer from the always-loaded
   * `MEMORY.md` index so a deleted memory never lingers in context. The MEMORY.md
   * index itself is never treated as a deletable memory. Returns the list of
   * removed entries (path + scope) — empty when no memory matched.
   */
  deleteMemory(name: string): { path: string; scope: MemoryScope }[] {
    const norm = (s: string): string => s.replace(/\s+/g, ' ').trim().toLowerCase();
    const target = norm(name);
    const slug = memoryFileSlug(name);
    const baseDir = this.getMemoryDir();
    const removed: { path: string; scope: MemoryScope }[] = [];
    const removedFilenames: { file: string; scope: MemoryScope }[] = [];

    const scopes: { scope: MemoryScope; dir: string }[] = [
      { scope: 'private', dir: baseDir },
      { scope: 'team', dir: path.join(baseDir, 'team') },
    ];
    for (const { scope, dir } of scopes) {
      let entries: string[];
      try {
        entries = fs.readdirSync(dir);
      } catch {
        continue; // scope dir doesn't exist yet
      }
      for (const file of entries) {
        if (!file.endsWith('.md') || file === MEMORY_INDEX_FILENAME) continue;
        const filePath = path.join(dir, file);
        let matched = false;
        try {
          const { frontmatter } = parseFrontmatter(fs.readFileSync(filePath, 'utf-8'), filePath);
          if (norm(frontmatter.name || '') === target) matched = true;
        } catch (err) {
          logUnexpectedReadError(err, filePath);
        }
        // Filename fallback covers a legacy/un-parseable file whose stem still
        // matches the requested name's slug (e.g. `foo.md` / `foo-2.md`).
        if (!matched) {
          const base = file.replace(/\.md$/, '');
          if (base === slug || base.replace(/-\d+$/, '') === slug) matched = true;
        }
        if (!matched) continue;
        try {
          fs.unlinkSync(filePath);
          removed.push({ path: filePath, scope });
          removedFilenames.push({ file, scope });
        } catch (err) {
          logUnexpectedReadError(err, filePath);
        }
      }
    }

    // Prune the index pointers for every removed file so MEMORY.md (loaded every
    // turn) never points at a memory that no longer exists.
    if (removedFilenames.length > 0) {
      const indexPath = path.join(baseDir, MEMORY_INDEX_FILENAME);
      let existing = '';
      try { existing = fs.readFileSync(indexPath, 'utf-8'); } catch { /* no index */ }
      if (existing) {
        const pointers = removedFilenames.map(
          ({ file, scope }) => `(${scope === 'team' ? `team/${file}` : file})`,
        );
        const kept = existing.split('\n').filter(line => !pointers.some(p => line.includes(p)));
        fs.writeFileSync(indexPath, kept.join('\n'), { encoding: 'utf-8', mode: MEMORY_FILE_MODE });
        try { fs.chmodSync(indexPath, MEMORY_FILE_MODE); } catch { /* best effort */ }
      }
    }

    return removed;
  }

  /**
   * Return the sanitized body of a single stored memory by name (case-
   * insensitive), or null if none matches. Backs the read_memory tool so a
   * memory shown only as an index entry can be expanded on demand.
   */
  readMemory(name: string): string | null {
    const target = name.trim().toLowerCase();
    for (const m of this.loadMemories()) {
      if ((m.name || '').trim().toLowerCase() === target) {
        return sanitizeMemoryContent(m.content);
      }
    }
    return null;
  }

  /**
   * `### Searching past context` guidance (Claude Code memdir/memdir.ts
   * `buildSearchingPastContextSection`). Gives the model the concrete shell
   * commands to recall prior work: a recursive grep over its own memory topic
   * files (`*.md`) and, as a last resort, over the per-project session transcript
   * JSONL store. Both paths are resolved from the live store locations so the
   * commands are copy-paste runnable for THIS project, not placeholders.
   */
  buildSearchingPastContextSection(): string {
    const memDir = this.getMemoryDir();
    const sessionDir = projectStateDir(canonicalGitRoot(this.cwd), 'sessions');
    return [
      '### Searching past context',
      'When looking for prior work or context:',
      `1. Search topic files in your memory directory: \`grep -rn "<search term>" ${memDir} --include="*.md"\``,
      `2. Session transcript logs (last resort — large files, slow): \`grep -rn "<search term>" ${sessionDir} --include="*.jsonl"\``,
      'Use narrow search terms (error messages, file paths, function names) rather than broad keywords.',
    ].join('\n');
  }

  getMemoryContext(opts?: { storedInlineBudget?: number; autoMemoryEnabled?: boolean }): string {
    const sections: string[] = [];

    const userGlobal = this.loadUserGlobalInstructions();
    if (userGlobal) {
      sections.push(`## User Instructions (${PROVENANCE_USER_GLOBAL})\n\n${userGlobal}`);
    }

    const instructions = this.loadProjectInstructions();
    if (instructions) {
      sections.push(`## Project Instructions\n\n${instructions}`);
    }

    // Auto-memory gate: when disabled (AMI_DISABLE_AUTO_MEMORY env or the resolved
    // autoMemoryEnabled setting), the persistent memory system is suppressed from
    // the prompt entirely — no stored/team memories, no health warning, no memory
    // guidance. Project/user INSTRUCTION files above are NOT auto-memory and still
    // load. Mirrors Claude Code's isAutoMemoryEnabled dropping the memory section.
    if (!isAutoMemoryEnabled(process.env, opts?.autoMemoryEnabled)) {
      return sections.join('\n\n');
    }

    // Whether any instruction content precedes the memory system. Used to decide
    // whether to surface the empty-memory affordance (below): a project that has
    // instructions but no memories yet should still learn that a persistent
    // memory system exists, while a completely blank context stays empty so
    // getMemoryContext's "no content ⇒ empty string" contract is preserved.
    const hasInstructionContent = sections.length > 0;

    const memories = this.loadMemories();
    if (memories.length > 0) {
      // When the combined stored-memory bodies exceed the caller's inline budget,
      // emit a compact INDEX (name + description + size) instead of every body;
      // the read_memory tool expands any entry on demand. Small memory sets stay
      // fully inline (no tool round-trip needed). No budget → always full.
      const totalBody = memories.reduce((n, m) => n + m.content.length, 0);
      const useIndex = typeof opts?.storedInlineBudget === 'number'
        && opts.storedInlineBudget >= 0
        && totalBody > opts.storedInlineBudget;

      if (useIndex) {
        const indexLines = memories.map(m => {
          const typeTag = m.memoryType ? `[${m.memoryType}] ` : '';
          const ageTag = m.freshness ? `, ${m.freshness}` : '';
          const desc = m.description ? ` — ${m.description}` : '';
          // Exact ISO-8601 mtime alongside the coarse freshness bucket (parity:
          // Claude Code's formatMemoryManifest, memdir/memoryScan.ts) — gives the
          // recall selector a precise, sortable time anchor so two memories in the
          // same freshness bucket ("3 days ago") are still distinguishable by recency.
          const isoTag = typeof m.mtimeMs === 'number' ? `, ${new Date(m.mtimeMs).toISOString()}` : '';
          return `- ${typeTag}${m.name || 'Untitled'}${desc} (${m.content.length} chars${ageTag}${isoTag})`;
        });
        sections.push(`## Stored Memories (index — call read_memory with a name for the full body)\n\n${indexLines.join('\n')}`);
      } else {
        const memoryLines = memories.map(m => {
          const typeTag = m.memoryType ? `[${m.memoryType}] ` : '';
          const ageTag = m.freshness ? ` (${m.freshness})` : '';
          const header = `### ${typeTag}${m.name || 'Untitled'}${ageTag}`;
          const desc = m.description ? `> ${m.description}` : '';
          let body = sanitizeMemoryContent(m.content);
          if ((m.ageDays ?? 0) > 1) {
            body = `[Note: This memory is ${m.freshness} old. Verify against current state before acting.]\n${body}`;
          }
          const parts = [header];
          if (desc) parts.push(desc);
          parts.push('', body);
          return parts.join('\n');
        });

        sections.push(`## Stored Memories\n\n${memoryLines.join('\n\n')}`);
      }
    }

    const teamMemories = this.loadTeamMemories().map(sanitizeMemoryContent);
    if (teamMemories.length > 0) {
      sections.push(`## Team Memories\n\n${teamMemories.join('\n\n')}`);
    }

    // Empty-memory affordance (parity with Claude Code's buildMemoryPrompt
    // `## MEMORY.md … currently empty` branch): when the project already has
    // instruction context but no stored/team memories yet, tell the model the
    // persistent memory system exists and is simply empty, so it knows it can
    // write to it — rather than giving zero signal on a fresh memory store. A
    // fully-blank context (no instructions either) stays empty to preserve the
    // "no content ⇒ empty string" contract.
    if (hasInstructionContent && memories.length === 0 && teamMemories.length === 0) {
      sections.push(
        `## ${MEMORY_INDEX_FILENAME}\n\n` +
        `Your ${MEMORY_INDEX_FILENAME} is currently empty. When you save new memories, they will appear here.`,
      );
    }

    // Oversized-memory-file diagnostic: flag (never silently clip) any stored
    // memory file over the recommended cap so the user can split/trim it.
    const large = this.getLargeMemoryFiles();
    if (large.length > 0) {
      const lines = large.map(f => `- ${path.basename(f.path)} — ${f.chars} chars`);
      sections.push(
        `## Memory Health Warning\n` +
        `The following memory file(s) exceed the recommended ${MAX_MEMORY_CHARACTER_COUNT}-character limit ` +
        `and may degrade performance or crowd out context. Consider splitting or trimming them (use /memory to edit):\n` +
        `${lines.join('\n')}`,
      );
    }

    if (memories.length > 0 || teamMemories.length > 0) {
      // Host/SDK-injected extra memory guidelines (AMI_MEMORY_EXTRA_GUIDELINES),
      // appended to the guidance block so an organization can add memory rules
      // without patching the prompt builder.
      const extra = resolveMemoryExtraGuidelines();
      const extraGuidelinesBlock = extra ? `\n\n### Additional memory guidelines\n${extra}` : '';
      const hasOldMemories = memories.some(m => (m.ageDays ?? 0) > 1);
      const driftCaveat = hasOldMemories
        ? `\n\n## Memory Drift Warning\nMemories may be stale. Always verify against current file contents, git log, and observed behavior before acting on recalled information.`
        : '';

      const memoryGuidance = `## Memory Guidelines
### When to save memories
- User preferences, corrections, project conventions
- NOT: code patterns derivable from files, git history, debugging recipes, ephemeral task state
- Include *why* you are saving so you can judge edge cases later.

${WHAT_NOT_TO_SAVE_GUIDANCE}

### Memory freshness
- Verify memories >1 day old against current state before acting
- Trust observed reality (file contents, git log) over stale memories

### Memory types
${MEMORY_TYPES_GUIDANCE}

${MEMORY_SCOPE_GUIDANCE}

${WHEN_TO_ACCESS_GUIDANCE}

${this.buildSearchingPastContextSection()}

${BEFORE_RECOMMENDING_GUIDANCE}

${PERSISTENCE_GUIDANCE}

### Saving a memory
Lead feedback/project memories with the rule/fact, then a **Why:** line and a **How to apply:** line. Use this frontmatter:
${MEMORY_FRONTMATTER_EXAMPLE}

${TWO_STEP_SAVE_GUIDANCE}${driftCaveat}${extraGuidelinesBlock}`;
      sections.push(memoryGuidance);
    }

    return sections.join('\n\n');
  }

  /**
   * Select the most relevant memories for a given query context using BM25 scoring.
   * Returns up to `maxResults` memories sorted by relevance.
   *
   * `opts.alreadySurfaced` — paths already shown in prior turns are filtered out
   * BEFORE scoring/short-circuit, so the fixed result budget is spent on fresh
   * candidates instead of re-picking files the caller will discard (mirrors
   * Claude Code's findRelevantMemories `alreadySurfaced`).
   *
   * `opts.recentTools` — when a tool is in active use, its pure usage/API
   * reference memory is noise (the conversation already demonstrates it), so
   * such `reference`-type memories are dropped; warning/gotcha/known-issue
   * memories about the same tool are preserved (mirrors the `recentTools` rule).
   */
  selectRelevantMemories(
    query: string,
    maxResults: number = 5,
    opts?: { recentTools?: readonly string[]; alreadySurfaced?: ReadonlySet<string> },
  ): MemoryEntry[] {
    let all = this.loadMemories();

    // Cross-turn dedup: drop already-surfaced memories before anything else so
    // the budget goes to fresh candidates, not re-picks the caller discards.
    const surfaced = opts?.alreadySurfaced;
    if (surfaced && surfaced.size > 0) {
      all = all.filter(m => !surfaced.has(m.path));
    }

    // Active-tool precision filter: suppress pure-reference docs for tools
    // already in active use, but keep their warnings/gotchas/known-issues.
    const recentTools = opts?.recentTools;
    if (recentTools && recentTools.length > 0) {
      all = all.filter(m => !isSuppressedToolReference(m, recentTools));
    }

    // Candidate denominator for the recall-shape diagnostic is the post-filter
    // set (what the selector actually chose FROM), computed once up front so the
    // telemetry denominator matches the pool across every selection branch.
    const candidateCount = all.length;
    const queryTokens = tokenize(query);

    let selected: MemoryEntry[];
    if (all.length <= maxResults) {
      // Fewer candidates than the budget — every one is surfaced.
      selected = all;
    } else if (queryTokens.length === 0) {
      // No query signal to rank by — fall back to the first N (load order).
      selected = all.slice(0, maxResults);
    } else {
      const N = all.length;
      const docFreq = new Map<string, number>();
      const allMemTexts = all.map(m => `${m.name || ''} ${m.description || ''} ${m.content}`);
      for (const text of allMemTexts) {
        const unique = new Set(tokenize(text));
        for (const t of unique) {
          docFreq.set(t, (docFreq.get(t) || 0) + 1);
        }
      }

      const idfMap = new Map<string, number>();
      for (const qt of queryTokens) {
        const df = docFreq.get(qt) || 0;
        idfMap.set(qt, Math.log((N - df + 0.5) / (df + 0.5) + 1));
      }

      const scored = all.map((mem, i) => ({
        mem,
        score: computeRelevanceScore(allMemTexts[i], queryTokens, idfMap),
      }));

      scored.sort((a, b) => b.score - a.score);
      selected = scored.slice(0, maxResults).map(s => s.mem);
    }

    this.logRecallShape(candidateCount, selected, queryTokens.length);
    return selected;
  }

  /**
   * Emit a PII-free memory recall-shape diagnostic for one {@link selectRelevantMemories}
   * call. Parity with Claude Code's `logMemoryRecallShape`
   * (memdir/findRelevantMemories.ts): the event fires on EVERY recall — including
   * one that ran but selected nothing — because the selection-rate denominator
   * needs the "ran, picked nothing" case, which is encoded with a `-1` age
   * sentinel so it is distinguishable from "recall never ran" (no event at all).
   *
   * Routed through {@link logForDiagnosticsNoPII}, the machine-monitored,
   * NO-PII JSONL health channel (a no-op unless `AMI_DIAGNOSTICS_FILE` is set):
   * only coarse counts + a freshness age are recorded — never memory paths,
   * bodies, names, or the query text — honoring that channel's PII contract.
   */
  private logRecallShape(candidateCount: number, selected: MemoryEntry[], queryTokenCount: number): void {
    logForDiagnosticsNoPII('info', 'memory_recall', {
      candidates: candidateCount,
      selected: selected.length,
      // -1 sentinel: "recall ran but selected nothing" vs. a real freshest-age.
      topAgeDays: selected.length > 0 ? (selected[0]?.ageDays ?? 0) : -1,
      queryTokens: queryTokenCount,
    });
  }

  /**
   * Load team memories from the shared team memory directory.
   * Team memories live in .ami/memory/team/ and are
   * typically committed to version control for shared context.
   */
  loadTeamMemories(): string[] {
    const teamDir = path.join(this.getMemoryDir(), 'team');
    if (!fs.existsSync(teamDir)) return [];
    return this.loadMemoriesFromDir(teamDir);
  }

  private loadMemoriesFromDir(dir: string): string[] {
    try {
      const out: string[] = [];
      // RECURSIVE scan (parity: Claude Code memdir/memoryScan.ts `readdir(dir,
      // { recursive: true })`) so team memories organized into topic SUBDIRECTORIES
      // are discovered, not just the flat top level. The MEMORY.md index is a table
      // of contents, not a memory body — exclude it by BASENAME at ANY depth (mirrors
      // loadMemories) so a team/.../MEMORY.md isn't dumped verbatim into the prompt.
      // The aggregate count is then bounded by MAX_TEAM_MEMORY_FILES (shared store
      // has no per-author limit).
      const names = (fs.readdirSync(dir, { recursive: true }) as string[])
        .filter(f =>
          typeof f === 'string' &&
          f.endsWith('.md') &&
          path.basename(f) !== MEMORY_INDEX_FILENAME)
        .slice(0, MAX_TEAM_MEMORY_FILES);
      for (const f of names) {
        const candidate = path.join(dir, f);
        // SYMLINK-ESCAPE CONTAINMENT: a `.md` entry in the (version-controlled,
        // shared) team-memory dir may be a symlink pointing OUT of it — e.g. to
        // `~/.ssh/config` — which would exfiltrate an arbitrary file into the
        // system prompt. Verify the realpath is still contained within the team
        // dir before reading; skip anything that escapes.
        if (realpathWithin(candidate, dir) === null) continue;
        // BOUNDED READ: skip a runaway team-memory file rather than slurping it
        // whole into the context (mirrors the loadMemories MAX_MEMORY_FILE_BYTES
        // guard) — the team dir is shared/untrusted and must not stuff the prompt.
        try {
          if (this._statSync(candidate).size > MAX_MEMORY_FILE_BYTES) continue;
        } catch { continue; }
        const raw = fs.readFileSync(candidate, 'utf-8');
        // SECRET BOUNDARY FOR SHARED MEMORY: a team-memory file is shared with and
        // synced to every collaborator, so a leaked API key / token in it would
        // both land in this session's system prompt AND propagate to everyone.
        // Scan shared files BEFORE loading; on a hit, SKIP the file entirely (never
        // surface it to the model) and emit a user-facing `memorySecretWarning` so
        // the credential can be removed. Mirrors Claude Code's destination-scoped
        // team-memory secret guard (teamMemorySync/index.ts + teamMemSecretGuard.ts):
        // detection is scoped to the SHARED store — private memory is already
        // credential-redacted on write (see saveMemory), so this does not double-scan it.
        if (this.memoryScopeForPath(candidate) === 'team') {
          const secrets = scanForSecrets(raw);
          if (secrets.length > 0) {
            if (this._hookCallback) {
              this._hookCallback('memorySecretWarning', {
                filePath: candidate,
                scope: 'team',
                location: this.getMemoryScopeDisplay(candidate),
                credentials: secrets.map(s => s.label),
              });
            }
            coreLog(
              'memory',
              `skipping shared memory file with potential secrets ${redactLogPath(candidate)}`,
              { credentials: secrets.map(s => s.label).join(', ') },
              'warn',
            );
            continue;
          }
        }
        const { body } = parseFrontmatter(raw, candidate);
        const content = sanitizeContent(body.trim(), candidate);
        if (content.length > 0) out.push(content);
      }
      return out;
    } catch {
      return [];
    }
  }

  /**
   * Returns the absolute path to the memory directory.
   */
  getMemoryDir(): string {
    // Full-path override (AMI_MEMORY_DIR) wins when set + valid: an SDK/host can pin
    // memory to an explicit mount regardless of cwd/project key.
    const override = resolveMemoryDirOverride();
    if (override) return override;
    // Auto-memory lives GLOBALLY under ~/.ami/projects/<slug>/memory (like Claude Code's
    // ~/.claude/projects/<slug>/memory). The slug is keyed on the CANONICAL git root,
    // not the raw cwd, so every worktree and subdirectory of one repo shares a single
    // auto-memory directory instead of fragmenting it per `cd`/per worktree.
    return projectStateDir(canonicalGitRoot(this.cwd), 'memory');
  }

  /**
   * Absolute path to the shared TEAM memory sub-directory (`<memoryDir>/team`),
   * the one {@link loadTeamMemories} reads and {@link saveMemory} with
   * `scope: 'team'` writes into. Team memory is shared with everyone working in
   * the project and (typically) checked into VCS.
   */
  getTeamMemoryDir(): string {
    return path.join(this.getMemoryDir(), 'team');
  }

  /**
   * Count persisted memory topic files split by scope, for the save-confirmation
   * summary surfaced by the `write_memory` tool. Mirrors Claude Code's
   * teamMemSaved count split (team vs private). The `MEMORY.md` index is a table
   * of contents, not a memory body, so it is excluded by basename at any depth;
   * files under the nested `team/` sub-tree are counted as team, everything else
   * under the auto-memory root as private. Missing directories count as 0.
   */
  countMemoriesByScope(): { private: number; team: number } {
    const base = this.getMemoryDir();
    const teamDir = this.getTeamMemoryDir();
    const isMemFile = (f: unknown): f is string =>
      typeof f === 'string' && f.endsWith('.md') && path.basename(f) !== MEMORY_INDEX_FILENAME;
    let priv = 0;
    let team = 0;
    try {
      for (const f of fs.readdirSync(base, { recursive: true }) as string[]) {
        if (!isMemFile(f)) continue;
        // Normalize the platform separator so the team-subtree test is portable.
        const rel = f.split(path.sep).join('/');
        if (rel === 'team' || rel.startsWith('team/')) continue; // counted as team below
        priv++;
      }
    } catch { /* no auto-memory dir yet */ }
    try {
      for (const f of fs.readdirSync(teamDir, { recursive: true }) as string[]) {
        if (isMemFile(f)) team++;
      }
    } catch { /* no team dir yet */ }
    return { private: priv, team };
  }

  /**
   * Whether the workspace is inside a git repository — the cwd or any ancestor
   * carries a `.git` entry (a directory in a normal clone, a `gitdir:` pointer FILE
   * in a linked worktree / submodule; both recognized by {@link isGitRoot}). Parity
   * with Claude Code's `projectIsInGitRepo` (utils/git.ts): used to describe whether
   * editing the project instruction file is version-controlled/shared or purely
   * local. Walks to the filesystem root (unbounded by the instruction-scan depth) so
   * a repo rooted several levels up is still detected.
   */
  projectIsInGitRepo(): boolean {
    let dir = path.resolve(this.cwd);
    for (;;) {
      if (isGitRoot(dir)) return true;
      const parent = path.dirname(dir);
      if (parent === dir) return false;
      dir = parent;
    }
  }

  /**
   * Describe the project instruction file for the `/memory` surface, branching the
   * verb on git membership (parity with Claude Code's MemoryFileSelector
   * `${isGit ? 'Checked in at' : 'Saved in'} ./CLAUDE.md`): a checked-in file's
   * edits are version-controlled/shared, a non-repo file's edits are purely local.
   * Returns the discovered file's relative path, the `checkedIn` boolean, and a
   * ready-to-render description. `null` when the workspace has no project
   * instruction file at its root (nothing to describe).
   */
  describeProjectMemory(): { path: string; relativePath: string; checkedIn: boolean; description: string } | null {
    const found = this.findInstructionFile(path.resolve(this.cwd));
    if (!found) return null;
    const relativePath = path.relative(this.cwd, found) || path.basename(found);
    const display = relativePath.startsWith('.') || path.isAbsolute(relativePath)
      ? relativePath
      : `./${relativePath}`;
    const checkedIn = this.projectIsInGitRepo();
    return {
      path: found,
      relativePath: display,
      checkedIn,
      description: `${checkedIn ? 'Checked in at' : 'Saved in'} ${display}`,
    };
  }

  /**
   * Is `absPath` inside the auto-memory tree at all (private root OR the nested
   * team sub-dir)? Traversal-safe: the boundary check normalizes/expands and
   * symlink-resolves via the shared {@link isPathWithin} primitive, so a
   * `..`-laden or symlinked path cannot spoof membership. Mirrors Claude Code's
   * `isAutoMemPath` (memdir/paths.ts) / `isAgentMemoryPath` (AgentTool/
   * agentMemory.ts) `normalize().startsWith(dir + sep)` membership predicate,
   * reusing AMI's one canonical containment helper instead of a second
   * normalize-based check. Used by permission/collapse classification.
   */
  isMemoryPath(absPath: string): boolean {
    return isPathWithin(absPath, this.getMemoryDir(), this.cwd);
  }

  /**
   * Is `absPath` a MEMORY FILE (a `.md` inside the auto-memory tree)? Parity with
   * Claude Code's `isMemoryFilePath` (utils/claudemd.ts): recognizes a path as a
   * memory file so an edit to it can trigger a memory-section refresh. Narrows
   * {@link isMemoryPath} to markdown so a sibling non-memory artifact in the dir
   * is not misclassified.
   */
  isMemoryFilePath(absPath: string): boolean {
    return absPath.toLowerCase().endsWith('.md') && this.isMemoryPath(absPath);
  }

  /**
   * Is `absPath` inside the shared TEAM memory sub-dir? Traversal-safe (shared
   * {@link isPathWithin}). Parity with Claude Code's `isTeamMemFile`
   * (memdir/teamMemPaths.ts via utils/teamMemoryOps.ts): the classifier a
   * search/write/edit guard uses to recognize an operation that targets shared
   * team memory and must be routed through the stricter (secret-scanned) path.
   */
  isTeamMemFile(absPath: string): boolean {
    return isPathWithin(absPath, this.getTeamMemoryDir(), this.cwd);
  }

  /**
   * Resolve which memory STORE a path belongs to, or `null` when it is outside
   * every store. TEAM IS CHECKED FIRST because the team dir is nested inside the
   * memory root (`<memoryDir>/team`), so a team path also satisfies
   * {@link isMemoryPath}; the team-before-personal order is the invariant that
   * keeps a shared path from being mislabeled `personal`. Mirrors Claude Code's
   * `memoryScopeForPath` (utils/memoryFileDetection.ts).
   */
  memoryScopeForPath(absPath: string): MemoryPathScope | null {
    if (this.isTeamMemFile(absPath)) return 'team';
    if (this.isMemoryPath(absPath)) return 'personal';
    return null;
  }

  /**
   * Human-readable "scope (resolved path)" label for a memory location, for the
   * `/memory` surface and diagnostics. With no argument, describes the default
   * private store; with a path, names the store the path resolves to (or
   * `External` when it is outside every memory store). Mirrors Claude Code's
   * `getMemoryScopeDisplay` (AgentTool/agentMemory.ts) which renders a
   * capitalized scope plus its directory.
   */
  getMemoryScopeDisplay(absPath?: string): string {
    if (absPath === undefined) {
      return `Private (${abbreviateHome(this.getMemoryDir())})`;
    }
    switch (this.memoryScopeForPath(absPath)) {
      case 'team':
        return `Team (${abbreviateHome(this.getTeamMemoryDir())})`;
      case 'personal':
        return `Private (${abbreviateHome(this.getMemoryDir())})`;
      default:
        return `External (${abbreviateHome(absPath)})`;
    }
  }

  /**
   * Is `absPath` a USER-MANAGED instruction file — one a human authors and edits
   * directly (CLAUDE.md / AGENTS.md / SUPERINFERENCE.md and the other recognized
   * {@link INSTRUCTION_FILES}, their `.local.md` variants, or a rule file under
   * `.ami/rules/` or `.claude/rules/`)? These are NOT part of the auto-memory
   * system and should keep full-diff / direct-edit treatment. Mirrors the
   * "excludes" half of Claude Code's `isAutoManagedMemoryFile`
   * (utils/memoryFileDetection.ts). Matching is on the basename for the named
   * instruction files and on the normalized path for the rules sub-tree.
   */
  isUserManagedInstructionFile(absPath: string): boolean {
    const base = path.basename(absPath);
    if ((INSTRUCTION_FILES as readonly string[]).includes(base)) return true;
    if ((LOCAL_VARIANTS as readonly string[]).includes(base)) return true;
    const norm = absPath.replace(/\\/g, '/');
    // `.ami/rules/*.md|txt` or `.claude/rules/*.md|txt` (any nesting depth).
    return /(?:^|\/)\.(?:ami|claude)\/rules\/.+\.(?:md|txt)$/i.test(norm);
  }

  /**
   * Is `absPath` an AMI-MANAGED memory file — a file the memory system writes and
   * curates (stored memory, session memory, team memory, daily logs) rather than
   * a human-authored instruction file? Parity with Claude Code's
   * `isAutoManagedMemoryFile` (utils/memoryFileDetection.ts): a path inside the
   * auto-memory tree ({@link isMemoryFilePath}, which already spans the private
   * root and the nested team sub-dir) that is NOT a user-managed instruction file
   * ({@link isUserManagedInstructionFile}). Used by collapse/badge/diagnostic
   * logic to decide whether a file edit should route to `/memory` (managed) or a
   * plain editor (user-managed), and to tag {@link buildMemoryDiagnostics} rows.
   */
  isAutoManagedMemoryFile(absPath: string): boolean {
    if (this.isUserManagedInstructionFile(absPath)) return false;
    return this.isMemoryFilePath(absPath);
  }

  /**
   * Classify a TOOL USE as targeting TEAM memory, returning the kind of
   * operation or `null` when it does not. Parity with Claude Code's
   * `isTeamMemorySearch` + `isTeamMemoryWriteOrEdit` (utils/teamMemoryOps.ts): a
   * read/search tool (`file_read`, `grep`, `glob`) whose `path`/`file_path`
   * targets a team file is `'search'`; a mutation tool (`file_edit`) whose
   * `file_path`/`path` targets a team file is `'write'`. Gated by
   * {@link isTeamMemoryEnabled} — when the team-memory capability is off this is
   * always `null` (matching Claude Code's `feature('TEAMMEM')` gate), so no
   * surface treats an op as team-scoped unless the user opted in.
   */
  classifyTeamMemoryToolUse(
    toolName: string,
    toolInput: unknown,
    env: NodeJS.ProcessEnv = process.env,
  ): 'search' | 'write' | null {
    if (!isTeamMemoryEnabled(env)) return null;
    const input = toolInput as { path?: string; file_path?: string } | undefined;
    if (!input) return null;
    if (toolName === 'file_edit') {
      const target = input.file_path ?? input.path;
      return target !== undefined && this.isTeamMemFile(target) ? 'write' : null;
    }
    if (toolName === 'file_read' || toolName === 'grep' || toolName === 'glob') {
      const target = input.path ?? input.file_path;
      return target !== undefined && this.isTeamMemFile(target) ? 'search' : null;
    }
    return null;
  }

  /**
   * Absolute path of the DATE-PARTITIONED daily memory log for `date` (default:
   * today): `<memoryDir>/logs/YYYY/MM/YYYY-MM-DD.md`. Parity with Claude Code's
   * `getAutoMemDailyLogPath` (memdir/paths.ts): rather than only maintaining the
   * live MEMORY.md index, the agent can APPEND timestamped working notes to a
   * per-day log that a later consolidation pass distills. The month is
   * zero-padded so the path sorts lexicographically by date.
   */
  getDailyMemoryLogPath(date: Date = new Date()): string {
    const yyyy = date.getFullYear().toString();
    const mm = (date.getMonth() + 1).toString().padStart(2, '0');
    const dd = date.getDate().toString().padStart(2, '0');
    return path.join(this.getMemoryDir(), 'logs', yyyy, mm, `${yyyy}-${mm}-${dd}.md`);
  }

  /**
   * Append `entry` (a single working note) to today's (or `date`'s)
   * {@link getDailyMemoryLogPath}, creating the `logs/YYYY/MM/` tree and the log
   * file on first write. Content is {@link sanitizeMemoryContent | sanitized} and
   * {@link redactCredentials | credential-redacted} before it is persisted — a
   * daily log is as sensitive as any other stored memory. The file is created
   * owner-only and a trailing newline terminates each entry so successive appends
   * stay line-separated. Returns the log path written to.
   */
  appendDailyMemoryLog(entry: string, date: Date = new Date()): string {
    const logPath = this.getDailyMemoryLogPath(date);
    ensureMemoryDirMode(path.dirname(logPath));
    createFileIfAbsent(logPath);
    const clean = redactCredentials(sanitizeMemoryContent(entry)).trimEnd();
    fs.appendFileSync(logPath, `${clean}\n`, { encoding: 'utf-8', mode: MEMORY_FILE_MODE });
    try { fs.chmodSync(logPath, MEMORY_FILE_MODE); } catch { /* best effort */ }
    return logPath;
  }

  /**
   * Create `absPath` as an empty file IF it does not already exist, without
   * clobbering an existing file (EEXIST-safe `wx` write via
   * {@link createFileIfAbsent}). Parity with Claude Code's `handleSelectMemoryFile`
   * (commands/memory/memory.tsx): the `/memory` surface synthesizes rows for
   * absent User/Project memory files so the user can create one from the menu;
   * selecting such a row creates the file (preserving content if it raced into
   * existence) before opening it for edit. Ensures the parent directory exists
   * first. Returns `true` when this call created the file.
   */
  createMemoryFileIfAbsent(absPath: string): boolean {
    ensureMemoryDirMode(path.dirname(absPath));
    return createFileIfAbsent(absPath);
  }

  /**
   * Save session memory facts to a session-specific markdown file.
   *
   * Facts are stored as `.ami/memory/session-<sessionId>.md`
   * with each fact rendered as a bullet point grouped by category.
   */
  saveSessionMemory(sessionId: string, facts: SessionFact[]): void {
    if (facts.length === 0) return;

    const memoryDir = this.getMemoryDir();
    ensureMemoryDirMode(memoryDir);

    const safeId = sessionId.replace(/[^a-zA-Z0-9_-]/g, '_');
    const filename = `session-${safeId}.md`;
    const filePath = path.join(memoryDir, filename);

    const lines: string[] = [
      '---',
      `name: session-${safeId}`,
      `description: Auto-extracted session facts`,
      'type: project',
      '---',
      '',
    ];

    // Group facts by category
    const grouped = new Map<string, SessionFact[]>();
    for (const fact of facts) {
      const group = grouped.get(fact.category) || [];
      group.push(fact);
      grouped.set(fact.category, group);
    }

    for (const [category, categoryFacts] of grouped) {
      lines.push(`### ${category}`);
      for (const f of categoryFacts) {
        // Credential boundary: a secret captured in an extracted fact must not be
        // written to the persisted session-memory file.
        lines.push(`- ${redactCredentials(f.fact)} (confidence: ${f.confidence})`);
      }
      lines.push('');
    }

    fs.writeFileSync(filePath, lines.join('\n'), { encoding: 'utf-8', mode: MEMORY_FILE_MODE });
    try { fs.chmodSync(filePath, MEMORY_FILE_MODE); } catch { /* best effort */ }
  }

  /**
   * Load session memory facts from a session-specific markdown file.
   *
   * Parses the `.ami/memory/session-<sessionId>.md` file and
   * returns an array of SessionFact objects.
   */
  loadSessionMemory(sessionId: string): SessionFact[] {
    const safeId = sessionId.replace(/[^a-zA-Z0-9_-]/g, '_');
    const filename = `session-${safeId}.md`;
    const filePath = path.join(this.getMemoryDir(), filename);

    let raw: string;
    try {
      raw = fs.readFileSync(filePath, 'utf-8');
    } catch (err) {
      // ENOENT (no session file yet) is the expected fallback; any other error
      // (corrupt/locked/permission-denied) is logged before falling back, so an
      // inaccessible session-memory file is not silently equated with "empty".
      logUnexpectedReadError(err, filePath);
      return [];
    }

    const { body } = parseFrontmatter(raw);
    const facts: SessionFact[] = [];
    const validCategories = new Set<SessionFact['category']>([
      'decision', 'file_modification', 'user_preference', 'error_fix', 'convention',
    ]);

    let currentCategory: SessionFact['category'] | null = null;

    for (const line of body.split('\n')) {
      const trimmed = line.trim();

      // Detect category headers like "### decision"
      const headerMatch = trimmed.match(/^###\s+(\S+)/);
      if (headerMatch) {
        const cat = headerMatch[1] as SessionFact['category'];
        if (validCategories.has(cat)) {
          currentCategory = cat;
        }
        continue;
      }

      // Parse fact lines like "- some fact (confidence: 0.9)"
      if (currentCategory && trimmed.startsWith('- ')) {
        const factMatch = trimmed.match(/^-\s+(.+?)\s+\(confidence:\s+([\d.]+)\)$/);
        if (factMatch) {
          const confidence = parseFloat(factMatch[2]);
          if (!isNaN(confidence)) {
            facts.push({
              fact: factMatch[1],
              category: currentCategory,
              confidence,
            });
          }
        }
      }
    }

    return facts;
  }

  // ── Rules directory scanning ──────────────────────────────────────────

  /**
   * Scan and load instruction files from `.ami/rules/` recursively.
   *
   * Reads all `.md` and `.txt` files, sanitizes their content, and returns
   * each file's content as a string. Files with `paths:` frontmatter are
   * included unconditionally here — use `loadRulesForFile()` for filtered loading.
   */
  async loadRulesDirectory(cwd: string): Promise<string[]> {
    const rulesDir = path.join(cwd, '.ami', 'rules');
    const rules: string[] = [];

    try {
      const entries = await this._readdir(rulesDir, { withFileTypes: true, recursive: true });
      for (const entry of entries) {
        if (!entry.isFile()) continue;
        if (!entry.name.endsWith('.md') && !entry.name.endsWith('.txt')) continue;
        const parentDir = entry.parentPath ?? (entry as any).path ?? rulesDir;
        const filePath = path.join(parentDir, entry.name);
        try {
          const raw = await fsp.readFile(filePath, 'utf-8');
          const content = sanitizeContent(raw.trim(), filePath);
          if (content) {
            rules.push(content);
          }
        } catch { /* skip unreadable files */ }
      }
    } catch { /* rules dir may not exist */ }

    return rules;
  }

  /**
   * Load rules from `.ami/rules/` that apply to a specific file.
   *
   * Rules with a `paths:` frontmatter field are only included when the
   * current file matches one of the globs. Rules without `paths:` always apply.
   */
  async loadRulesForFile(cwd: string, currentFile: string): Promise<InstructionSource[]> {
    const rulesDir = path.join(cwd, '.ami', 'rules');
    const sources: InstructionSource[] = [];

    try {
      const entries = await this._readdir(rulesDir, { withFileTypes: true, recursive: true });
      for (const entry of entries) {
        if (!entry.isFile()) continue;
        if (!entry.name.endsWith('.md') && !entry.name.endsWith('.txt')) continue;
        const parentDir = entry.parentPath ?? (entry as any).path ?? rulesDir;
        const filePath = path.join(parentDir, entry.name);
        try {
          const raw = await fsp.readFile(filePath, 'utf-8');
          const { frontmatter, body } = parseFrontmatter(raw, filePath);

          // Conditional rules: skip if paths: is set and doesn't match. A rule
          // whose patterns normalize to "unconditional" (all `**`, or empty) is
          // loaded eagerly rather than filtered per-file (normalizeFrontmatterPaths
          // returns null), and a trailing `/**` is stripped so `src/**` matches
          // `src/foo.ts` via the same subtree semantics as `src`.
          if (frontmatter.paths) {
            const globs = normalizeFrontmatterPaths(parseFrontmatterList(frontmatter.paths));
            if (globs && !matchesPaths(globs, currentFile)) {
              continue;
            }
          }

          const content = sanitizeContent(body.trim(), filePath);
          if (content) {
            sources.push({ content, level: 'managed', path: filePath });
          }
        } catch { /* skip unreadable files */ }
      }
    } catch { /* rules dir may not exist */ }

    return sources;
  }

  // ── @include directive resolution ─────────────────────────────────────

  /**
   * Resolve `@include <path>` directives in instruction/rule content.
   *
   * Recursively inlines referenced files up to a maximum depth of 5.
   * Tracks seen paths to prevent circular references — a circular inclusion
   * is replaced with a `[circular reference: ...]` marker.
   *
   * @param content - The raw content potentially containing @include directives
   * @param basePath - Absolute path of the file containing the content (for relative resolution)
   * @param depth - Current recursion depth (capped at 5)
   * @param seen - Set of already-visited absolute paths for cycle detection
   */
  async resolveIncludes(
    content: string,
    basePath: string,
    depth: number = 0,
    seen: Set<string> = new Set(),
  ): Promise<string> {
    if (depth > 5) return content;

    const includePattern = /@include\s+(.+)/g;
    let result = content;
    let match;

    // Collect all matches first to avoid issues with modifying the string during iteration
    const matches: { fullMatch: string; includePath: string }[] = [];
    while ((match = includePattern.exec(content)) !== null) {
      matches.push({
        fullMatch: match[0],
        includePath: path.resolve(path.dirname(basePath), match[1].trim()),
      });
    }

    let searchFrom = 0;
    for (const { fullMatch, includePath } of matches) {
      const pos = result.indexOf(fullMatch, searchFrom);
      if (pos === -1) continue;

      // WORKSPACE CONFINEMENT: `@include ../../etc/passwd` (or a symlink inside the
      // workspace pointing out) must not inline an arbitrary file into instructions.
      // Resolve symlinks and verify containment before touching the file.
      const real = realpathWithin(includePath, this.cwd);
      if (real === null) {
        const replacement = `[include blocked — outside workspace: ${path.basename(includePath)}]`;
        result = result.slice(0, pos) + replacement + result.slice(pos + fullMatch.length);
        searchFrom = pos + replacement.length;
        continue;
      }

      // SYMLINK-AWARE CYCLE DETECTION: key `seen` on the REAL (symlink-resolved)
      // path, not the lexical includePath, so two aliases of one file (one via a
      // symlink) cannot slip past the cycle guard and recurse until the depth cap.
      // The marker still shows the authored basename.
      if (seen.has(real)) {
        const replacement = `[circular reference: ${path.basename(includePath)}]`;
        result = result.slice(0, pos) + replacement + result.slice(pos + fullMatch.length);
        searchFrom = pos + replacement.length;
        continue;
      }
      seen.add(real);
      try {
        // Deny-by-default: only inline recognized text extensions (extensionless
        // files allowed). An allowlist can't be defeated by a novel/multi-dotted
        // binary extension the way the old binary DENYlist could.
        if (!isTextImportPath(includePath)) {
          const replacement = `[non-text file skipped: ${path.basename(includePath)}]`;
          result = result.slice(0, pos) + replacement + result.slice(pos + fullMatch.length);
          searchFrom = pos + replacement.length;
          seen.delete(real);
          continue;
        }
        let included = await fsp.readFile(includePath, 'utf-8');
        included = sanitizeContent(included, includePath);
        included = await this.resolveIncludes(included, includePath, depth + 1, seen);
        result = result.slice(0, pos) + included + result.slice(pos + fullMatch.length);
        searchFrom = pos + included.length;
      } catch {
        const replacement = `[include not found: ${path.basename(includePath)}]`;
        result = result.slice(0, pos) + replacement + result.slice(pos + fullMatch.length);
        searchFrom = pos + replacement.length;
      } finally {
        // Treat `seen` as an ancestor stack: remove this path once its subtree
        // is resolved so a diamond (same file reached via two sibling branches)
        // is not mistaken for a cycle. True cycles stay on the active path.
        seen.delete(real);
      }
    }

    return result;
  }

  // ── MEMORY.md index management ────────────────────────────────────────

  /**
   * Read the MEMORY.md index file and return all index entries.
   *
   * The index file lives at `.ami/memory/MEMORY.md` and contains
   * lines like `- [topic](filename.md) — description` that serve as a
   * human-readable and machine-parseable table of contents.
   */
  async readMemoryIndex(cwd: string): Promise<string[]> {
    const indexPath = path.join(projectStateDir(cwd, 'memory'), 'MEMORY.md');
    try {
      const content = await fsp.readFile(indexPath, 'utf-8');
      // The entrypoint is injected every turn — cap it (lines + bytes) so a
      // runaway index can't stuff the context / inflate cost.
      return truncateEntrypointContent(content).split('\n').filter(l => l.startsWith('- ['));
    } catch (err) {
      // ENOENT (no index yet) is expected; any other read error is logged.
      logUnexpectedReadError(err, indexPath);
      return [];
    }
  }

  /**
   * Add an entry to the MEMORY.md index file if it doesn't already exist.
   *
   * Creates the file if it doesn't exist. Entries are expected in the format
   * `- [topic](filename.md) — description`.
   */
  async addToMemoryIndex(cwd: string, entry: string): Promise<void> {
    const indexPath = path.join(projectStateDir(cwd, 'memory'), 'MEMORY.md');
    try {
      await fsp.mkdir(path.dirname(indexPath), { recursive: true, mode: MEMORY_DIR_MODE });
      try { await fsp.chmod(path.dirname(indexPath), MEMORY_DIR_MODE); } catch { /* best effort */ }
      // Atomically create the index if absent WITHOUT clobbering existing content
      // (EEXIST-safe `wx` write via {@link createFileIfAbsent}) — closes the
      // read-then-write race where two concurrent appends both observe the file
      // missing and the second overwrites the first's entry.
      createFileIfAbsent(indexPath);
      let content = '';
      try { content = await fsp.readFile(indexPath, 'utf-8'); } catch { /* file doesn't exist */ }
      const next = computeIndexAppend(content, entry);
      if (next !== null) {
        await fsp.writeFile(indexPath, next, { encoding: 'utf-8', mode: MEMORY_FILE_MODE });
        try { await fsp.chmod(indexPath, MEMORY_FILE_MODE); } catch { /* best effort */ }
      }
    } catch { /* best effort */ }
  }

  /**
   * Remove an entry from the MEMORY.md index file.
   * Matches by substring — if any line contains the given text, it is removed.
   */
  async removeFromMemoryIndex(cwd: string, entrySubstring: string): Promise<void> {
    const indexPath = path.join(projectStateDir(cwd, 'memory'), 'MEMORY.md');
    try {
      const content = await fsp.readFile(indexPath, 'utf-8');
      const lines = content.split('\n').filter(l => !l.includes(entrySubstring));
      await fsp.writeFile(indexPath, lines.join('\n'), 'utf-8');
    } catch { /* best effort */ }
  }

  // ── Instruction source collection ─────────────────────────────────────

  /**
   * Collect all instruction sources with level annotations.
   *
   * Returns an array of `InstructionSource` objects from all three levels:
   * - `managed`: from `.ami/rules/` directory
   * - `project`: from project instruction files (CLAUDE.md, SUPERINFERENCE.md, etc.)
   * - `user`: from `.local.md` variants and memory entries
   *
   * @param currentFile - Optional file path for conditional rule filtering
   */
  async collectInstructionSources(currentFile?: string): Promise<InstructionSource[]> {
    const sources: InstructionSource[] = [];

    // 1. Managed rules from .ami/rules/
    if (currentFile) {
      const managed = await this.loadRulesForFile(this.cwd, currentFile);
      sources.push(...managed);
    } else {
      const rules = await this.loadRulesDirectory(this.cwd);
      for (const content of rules) {
        sources.push({ content, level: 'managed', path: path.join(this.cwd, '.ami', 'rules') });
      }
    }

    // 2. Project-level instruction files
    const excludes = this.memoryExcludes();
    // Per-tier enable gates (parity: Claude Code's per-source isSettingSourceEnabled);
    // identical gating to loadProjectInstructions so the two discovery passes agree.
    const projectEnabled = isMemorySourceEnabled('project');
    const localEnabled = isMemorySourceEnabled('local');
    let currentDir = path.resolve(this.cwd);
    const root = path.parse(currentDir).root;
    let depth = 0;

    while (depth <= MAX_PARENT_SCAN_DEPTH) {
      const found = projectEnabled ? this.findInstructionFile(currentDir) : null;
      if (found && !isInstructionExcluded(found, excludes)) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            const content = sanitizeContent(raw, found);
            sources.push({ content, level: 'project', path: found });
          }
        } catch (err) {
          // Absent is silent; a genuine read error is surfaced (errno-aware reads).
          logUnexpectedReadError(err, found);
        }
      }

      // 3. Local-level override variants (`*.local.md`) — gitignored, project-local,
      //    distinct from a hand-authored user-global instruction (parity: Claude's
      //    'Local' memory scope, utils/memory/types.ts).
      if (localEnabled) for (const localFile of LOCAL_VARIANTS) {
        const localPath = path.join(currentDir, localFile);
        if (isInstructionExcluded(localPath, excludes)) continue;
        try {
          if (fs.statSync(localPath).isFile()) {
            const raw = fs.readFileSync(localPath, 'utf-8').trim();
            if (raw) {
              const content = sanitizeContent(raw, localPath);
              sources.push({ content, level: 'local', path: localPath });
            }
          }
        } catch (err) {
          // Absent is silent; a genuine read error is surfaced (errno-aware reads).
          logUnexpectedReadError(err, localPath);
        }
      }

      const parentDir = path.dirname(currentDir);
      if (parentDir === currentDir || currentDir === root) break;
      // Stop at the git root — `.git` is a dir (clone) or a file (worktree/submodule).
      if (isGitRoot(currentDir)) break;
      currentDir = parentDir;
      depth++;
    }

    // 4. User-global instructions (~/.ami, ~/.claude/CLAUDE.md)
    const userGlobal = this.loadUserGlobalInstructions();
    if (userGlobal) {
      sources.push({ content: userGlobal, level: 'user', path: '<user-global>' });
    }

    // 5. Auto-captured stored memories (the `memory/` store) — tagged 'automem'
    //    (parity: Claude's 'AutoMem' scope) so they are distinguishable from a
    //    hand-authored user-global instruction in provenance surfaces.
    const memories = this.loadMemories();
    for (const mem of memories) {
      sources.push({ content: mem.content, level: 'automem', path: mem.path });
    }

    // 6. Shared team-memory store — a FIRST-CLASS 'team' scope, loaded ONLY when
    //    the team-memory capability is enabled (parity: Claude's feature('TEAMMEM')
    //    gate on the 'TeamMem' scope, utils/memory/types.ts). Each shared file is
    //    already symlink-contained + secret-scanned by loadTeamMemories.
    if (isTeamMemoryEnabled()) {
      const teamDir = this.getTeamMemoryDir();
      for (const content of this.loadTeamMemories()) {
        sources.push({ content, level: 'team', path: teamDir });
      }
    }

    return sources;
  }

  /**
   * Summarize the memory/instruction load for this cwd — the file count (overall
   * and per {@link MemorySourceLevel}), the combined content size, and how long the
   * load took. Reuses {@link collectInstructionSources} (the one canonical discovery
   * pass covering managed/project/local/user/automem/team) so the summary can never
   * diverge from what is actually loaded, then emits a no-PII `memory_load_summary`
   * diagnostic (parity with Claude Code's `memory_files_completed` /
   * `tengu_claudemd__initial_load` events, utils/claudemd.ts:L1014-L1040) for machine
   * monitoring. The returned {@link MemoryLoadSummary} is rendered by `/doctor` so the
   * user can see the context cost of what memory contributes each turn.
   */
  async getMemoryLoadSummary(currentFile?: string): Promise<MemoryLoadSummary> {
    const startTime = Date.now();
    const sources = await this.collectInstructionSources(currentFile);
    const byLevel = Object.fromEntries(
      MEMORY_SOURCE_LEVELS.map(level => [level, 0]),
    ) as Record<MemorySourceLevel, number>;
    let totalBytes = 0;
    for (const s of sources) {
      byLevel[s.level] += 1;
      totalBytes += s.content.length;
    }
    const summary: MemoryLoadSummary = {
      fileCount: sources.length,
      totalBytes,
      durationMs: Date.now() - startTime,
      byLevel,
    };
    // No-PII introspection surface (paths/content are never logged — only counts,
    // size and timing), mirroring Claude Code's diagnostics event so an operator can
    // monitor memory context cost without leaking what was loaded.
    logForDiagnosticsNoPII('info', 'memory_load_summary', {
      file_count: summary.fileCount,
      total_content_length: summary.totalBytes,
      duration_ms: summary.durationMs,
      ...Object.fromEntries(
        MEMORY_SOURCE_LEVELS.map(level => [`${level}_count`, byLevel[level]]),
      ),
    });
    return summary;
  }
}
