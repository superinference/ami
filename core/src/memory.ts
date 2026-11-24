import * as fs from 'fs';
import { projectStateDir, canonicalGitRoot } from './reference-journal';
import * as fsp from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import type { SessionFact } from './session-memory';
import type { AgentMemoryScope } from './skills';
import { log as coreLog, logForDiagnosticsNoPII } from './logger';
import { redactCredentials } from './credential-guard';
import './analytics';
import { abbreviateHome, isPathWithin } from './utils/path';
import {
  formatRelativeTime,
  formatRelativeTimeAgo,
  type RelativeTimeStyle,
  type RelativeTimeOptions,
} from './utils/format';
import { scanForSecrets } from './tools/tool-utils';
import { resolveConfiguredMemoryExcludes, resolveTeamMemoryLoadingEnabled } from './config';
import { sequential } from './utils/sequential';
import { groupBy } from './utils/group-by';
import {
  isMemoryFileAccess,
  classifyToolSessionFileType,
  countMemoryFileAccessFromEntries,
  isAutoManagedMemoryPattern,
  type SessionFileType,
  type MemoryDirs,
  type MemoryAccessEntry,
} from './memory-file-detection';
import { FRONTMATTER_MAX_LINES, INSTRUCTION_FILES, LOCAL_VARIANTS, MAX_ENTRYPOINT_LINES, MAX_INSTRUCTION_FILE_SIZE, MAX_MEMORY_CHARACTER_COUNT, MAX_MEMORY_FILES, MAX_PARENT_SCAN_DEPTH, MEMORY_INDEX_FILENAME, MEMORY_SOURCE_LEVELS, MEMORY_TYPES, PROVENANCE_LOCAL, PROVENANCE_PROJECT, PROVENANCE_USER_GLOBAL, redactLogPath, resolveExternalMemoryImportConsent, resolveMemoryDirOverride, resolveMemoryExcludes, resolveMemoryExtraGuidelines, sanitizeMemoryContent } from './memory-memory-diagnostic';
import type { InstructionSource, LargeMemoryFile, MemoryDiagnostic, MemoryEntry, MemoryHeader, MemoryLoadSummary, MemoryPathScope, MemoryRelevanceConfig, MemoryScope, MemorySourceLevel, MemoryType } from './memory-memory-diagnostic';
import { MEMORY_DIR_MODE, MEMORY_FILE_MODE, additionalInstructionDirsEnabled, bestEffortChmod, bestEffortChmodAsync, createFileIfAbsent, detectNestedWorktree, ensureMemoryDirMode, errnoCode, isAutoMemoryEnabled, isExpectedMissing, isGitRoot, isMemorySourceEnabled, isTeamMemoryEnabled, logUnexpectedReadError, realpathDeepest, realpathWithin, resolveMemoryRelevanceConfig, truncateEntrypointContent } from './memory-truncate-entrypoint-content';
import { AT_IMPORT_PATTERN, STALE_MEMORY_DAYS, formatImportTree, isInstructionExcluded, matchesPaths, memoryAgeDays, memoryFreshness, normalizeFrontmatterPaths, parseFrontmatterList, prosePlainLines, sanitizeContent } from './memory-strip-surrounding-quotes';
import type { MemoryImportNode } from './memory-strip-surrounding-quotes';
import { AGENT_MEMORY_DISPLAY_NAME, MAX_TEAM_MEMORY_FILES, buildFrontmatter, buildMemoryHeader, computeIndexAppend, computeRelevanceScore, isMemoryHeaderFile, isSuppressedToolReference, memoryFileSlug, parseFrontmatter, parseMemoryType, resolveNonClobberingPath, tokenize } from './memory-parse-frontmatter';
import { AGENT_MEMORY_SCOPE_NOTE, getAgentMemoryDir } from './memory-get-agent-memory-dir';
export { FRONTMATTER_MAX_LINES, MAX_ENTRYPOINT_BYTES, MAX_ENTRYPOINT_LINES, MAX_MEMORY_CHARACTER_COUNT, MAX_MEMORY_FILES, MEMORY_INDEX_FILENAME, MEMORY_PATH_SCOPES, MEMORY_SCOPES, MEMORY_SOURCE_LEVELS, MEMORY_TYPES, resolveExternalMemoryImportConsent, resolveMemoryDirOverride, resolveMemoryExcludes, resolveMemoryExtraGuidelines, sanitizeMemoryContent } from './memory-memory-diagnostic';
export type { InstructionSource, LargeMemoryFile, MemoryDiagnostic, MemoryEntry, MemoryHeader, MemoryLoadSummary, MemoryPathScope, MemoryRelevanceConfig, MemoryScope, MemorySourceLevel, MemoryType } from './memory-memory-diagnostic';
export { DEFAULT_MEMORY_RELEVANCE, MEMORY_SOURCES, additionalInstructionDirsEnabled, bestEffortChmod, bestEffortChmodAsync, isAutoMemoryEnabled, isExpectedMissing, isMemorySourceEnabled, isTeamMemoryEnabled, resolveMemoryRelevanceConfig, truncateEntrypointContent } from './memory-truncate-entrypoint-content';
export type { MemorySource } from './memory-truncate-entrypoint-content';
export { formatImportTree, matchesPaths, memoryAgeDays, memoryFreshnessReminder, normalizeFrontmatterPaths, parseFrontmatterList, parseInlineFlowSequence, stripHtmlComments, stripSurroundingQuotes } from './memory-strip-surrounding-quotes';
export type { MemoryImportNode } from './memory-strip-surrounding-quotes';
export { AGENT_REMOTE_MEMORY_DIR_ENV, MAX_INDEX_ENTRY_CHARS, clampIndexEntry, computeIndexAppend, indexEntryTarget, memoryFileSlug, parseFrontmatter } from './memory-parse-frontmatter';
export { getAgentMemoryDir, getAgentMemoryEntrypoint, sanitizeAgentTypeForPath } from './memory-get-agent-memory-dir';

/**
 * Deny-by-default allowlist of file extensions that `@include` / `@path` imports
 * are permitted to inline into instructions/memory. Parity with the reference's
 * `TEXT_FILE_EXTENSIONS` (the reference's markdown-memory module `parseMemoryFileContent`): a crafted
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
 * Mirrors the reference's `if (ext && !TEXT_FILE_EXTENSIONS.has(ext))` gate, so a
 * multi-dotted binary like `libatomic.so.1` (extname `.1`) is correctly refused.
 */
export function isTextImportPath(filePath: string): boolean {
  const ext = path.extname(filePath).toLowerCase();
  return ext === '' || TEXT_FILE_EXTENSIONS.has(ext);
}

/**
 * Per-type memory taxonomy guidance surfaced in the system prompt. Mirrors the reference
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
 * explicit-save gate (the reference `WHAT_NOT_TO_SAVE_SECTION`). The gate prevents
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
 * `### Memory scope` block distinguishing the two storage scopes (the reference
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
 * memory" anti-pattern bullet (the reference `WHEN_TO_ACCESS_SECTION`): when the
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
 * `### Before recommending from memory` verification checklist (the reference
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
 * `### Memory and other forms of persistence` block (the reference
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
 * Frontmatter example with the body-structure scaffold (the reference
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

/**
 * Two-step save guidance teaching the MEMORY.md index pattern (the reference
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

// Relative-time formatting (`formatRelativeTime` / `formatRelativeTimeAgo`) is a
// pure, leaf-safe display helper, so its canonical home is `utils/format` next to
// `formatFileSize` / `formatLogMetadata` that compose it (mirroring the
// reference's single `utils/format.ts`). It is re-exported here — rather than
// defined twice — because {@link MemoryManager.buildMemoryDiagnostics} stamps each
// health warning with how recently the offending file changed, and the
// superinference memory-gate dates its last-consolidation line the same way.
export { formatRelativeTime, formatRelativeTimeAgo };
export type { RelativeTimeStyle, RelativeTimeOptions };

/** Per-file upper bound when scanning the memory dir: a single runaway file must
 *  not be slurped whole into the context/scan. */
const MAX_MEMORY_FILE_BYTES = 256 * 1024;

/**
 * Read only the first `maxLines` lines of a file via a BOUNDED byte read (open +
 * single `readSync` of a fixed window), never loading the whole file. Backs the
 * header-only {@link MemoryManager.scanMemoryHeaders} so a frontmatter scan over
 * many memories can't be forced to slurp a runaway body. A YAML frontmatter block
 * is tiny, so a generous byte window comfortably contains `maxLines` lines; the
 * window is clamped to {@link MAX_MEMORY_FILE_BYTES} and the result trimmed to
 * `maxLines`. The fd is always closed, even on a partial/failed read.
 */
function readFileHeadBounded(filePath: string, maxLines: number): string {
  // ~512 bytes/line is a generous upper bound for a frontmatter line; cap the
  // whole window so even a pathological single-line file can't blow the read.
  const windowBytes = Math.min(MAX_MEMORY_FILE_BYTES, Math.max(4096, maxLines * 512));
  const fd = fs.openSync(filePath, 'r');
  try {
    const buf = Buffer.allocUnsafe(windowBytes);
    const bytesRead = fs.readSync(fd, buf, 0, windowBytes, 0);
    return buf.toString('utf-8', 0, bytesRead).split('\n').slice(0, maxLines).join('\n');
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Async sibling of {@link readFileHeadBounded}: read the bounded frontmatter
 * window via `fs/promises` so many memory headers can be read concurrently, and
 * forward an optional {@link AbortSignal} so a slow/large scan is cancellable
 * mid-flight (parity: the reference's `readFileInRange(..., signal)` used by
 * `scanMemoryFiles`). The signal is checked once the handle is open (the point a
 * large read would block); the handle is always closed, even on abort/failure.
 */
async function readFileHeadBoundedAsync(
  filePath: string,
  maxLines: number,
  signal?: AbortSignal,
): Promise<string> {
  const windowBytes = Math.min(MAX_MEMORY_FILE_BYTES, Math.max(4096, maxLines * 512));
  const fh = await fsp.open(filePath, 'r');
  try {
    signal?.throwIfAborted();
    const buf = Buffer.allocUnsafe(windowBytes);
    const { bytesRead } = await fh.read(buf, 0, windowBytes, 0);
    return buf.toString('utf-8', 0, bytesRead).split('\n').slice(0, maxLines).join('\n');
  } finally {
    await fh.close();
  }
}

/**
 * Build the "Persistent Agent Memory" prompt block for a memory-enabled agent
 * (parity: the reference's `loadAgentMemoryPrompt` + `buildMemoryPrompt`). Seeds
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
  /** Lazily-resolved, per-instance cache of the effective external-import consent
   *  ({@link resolveExternalMemoryImportConsent}) for {@link cwd} — read once and
   *  reused across every `@import` resolved this load. */
  private _externalImportConsent: { approved: boolean; roots: string[] } | null = null;
  /** Lazily-resolved, per-instance cache of the config-sourced team-memory LOADING
   *  gate ({@link ProjectConfig.teamMemoryEnabled}) for {@link cwd} — read once and
   *  reused so every {@link loadTeamMemories} call in one load shares the decision. */
  private _teamMemoryLoadingEnabled: boolean | null = null;
  /** Absolute ADDITIONAL instruction directories (the engine's `allowedReadRoots`
   *  — dirs granted via `--add-dir` / `--context-file`). Loaded into the prompt only
   *  when {@link additionalInstructionDirsEnabled}. Empty when none supplied. */
  private _additionalInstructionDirs: string[];

  /** In-process serializer for MEMORY.md index mutations. The index is updated
   *  by a read-then-write cycle ({@link addToMemoryIndex} /
   *  {@link removeFromMemoryIndex}); two concurrent mutations could each observe
   *  the pre-write content and the second clobber the first's change. Routing
   *  both through ONE {@link sequential} wrapper runs them strictly one-at-a-time
   *  in FIFO order, each resolving with its own result. */
  private readonly _serializeIndexMutation = sequential(
    async (task: () => Promise<void>): Promise<void> => { await task(); },
  );

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

  /** The effective external-import consent for this workspace, resolved once per
   *  instance ({@link resolveExternalMemoryImportConsent}) and reused across every
   *  `@import` the current load resolves. */
  private externalImportConsent(): { approved: boolean; roots: string[] } {
    if (this._externalImportConsent === null) {
      this._externalImportConsent = resolveExternalMemoryImportConsent(this.cwd);
    }
    return this._externalImportConsent;
  }

  /**
   * Decide whether a specific outside-workspace `@import` target is sanctioned by the
   * persisted/env consent, returning its canonical REAL path when allowed or `null`
   * when it must stay blocked. Consent must be granted
   * ({@link ProjectConfig.externalMemoryImportsApproved} / the approve env); then the
   * target is symlink-resolved ({@link realpathDeepest}) so a link cannot smuggle a path
   * out of an approved root, and — when a trusted-roots allowlist is configured — the
   * real path must resolve within one of those roots. With consent but no roots, any
   * external file is permitted (approve-once for a wholly trusted repo). Returns `null`
   * whenever consent is absent, the path cannot be resolved, or it falls outside every
   * configured trusted root.
   */
  private resolveApprovedExternalImport(resolved: string): string | null {
    const consent = this.externalImportConsent();
    if (!consent.approved) return null;
    const real = realpathDeepest(resolved);
    if (real === null) return null;
    if (consent.roots.length > 0) {
      const within = consent.roots.some(root => isPathWithin(real, root, this.cwd));
      if (!within) return null;
    }
    return real;
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
   * Resolve ONE raw `@path` token to its security-cleared absolute target,
   * applying the shared unescape → fragment-strip → `~`/relative-resolution →
   * WORKSPACE-CONFINEMENT (realpath, with the sanctioned approved-external escape
   * hatch) policy used by BOTH {@link resolveAtImports} (inline substitution) and
   * {@link buildImportGraph} (parent-tracked hierarchy) — one canonical resolver
   * so the two can never diverge on what a path means or whether it escapes the
   * workspace. Returns a discriminated result; the caller decides how to render
   * each outcome (inline content / a block marker / a graph node) and owns any
   * stat/text-gate/cycle checks and hook side effects, which differ per surface.
   */
  private resolveAtImportPath(rawPath: string, basePath: string):
    | { kind: 'skip' }
    | { kind: 'blocked'; resolved: string }
    | { kind: 'ok'; real: string; resolved: string; external: boolean } {
    // Unescape `\ ` → ` ` and drop a trailing `#fragment` (anchors are not part
    // of the filesystem path).
    let atPath = rawPath.replace(/\\ /g, ' ');
    const hashIdx = atPath.indexOf('#');
    if (hashIdx >= 0) atPath = atPath.slice(0, hashIdx);
    if (atPath === '') return { kind: 'skip' };
    let resolved: string;
    try {
      resolved = atPath.startsWith('~')
        ? path.join(os.homedir(), atPath.slice(1))
        : path.resolve(path.dirname(basePath), atPath);
    } catch { return { kind: 'skip' }; }
    // WORKSPACE CONFINEMENT: resolve symlinks first so a link inside the workspace
    // that points out of it is also caught. A persisted, explicit approval MAY
    // sanction a specific external file (resolved to its REAL path so a symlink
    // still cannot escape the approved roots).
    const within = realpathWithin(resolved, this.cwd);
    if (within !== null) return { kind: 'ok', real: within, resolved, external: false };
    const approvedReal = this.resolveApprovedExternalImport(resolved);
    if (approvedReal === null) return { kind: 'blocked', resolved };
    return { kind: 'ok', real: approvedReal, resolved, external: true };
  }

  /**
   * Inline the reference-style `@path` imports in instruction content. A bare
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
      // Parity with the reference's lexer-token `@include` extraction, which skips
      // `code` tokens (indented code blocks AND inline code spans alike); AMI's
      // line-based resolver previously tracked only triple-backtick fences.
      if (/^(?: {4}|\t)/.test(lines[i])) continue;
      // Mask INLINE CODE SPANS (`` `…` ``) so an `@path` inside backticks is not
      // inlined (parity with the reference's markdown-lexer import extraction, which
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
      // with the reference's safeResolvePath).
      line = line.replace(new RegExp(AT_IMPORT_PATTERN, 'g'), (full, pre, p) => {
        // Shared security-cleared resolution (unescape, fragment-strip, confinement
        // + approved-external escape hatch) — see {@link resolveAtImportPath}.
        const r = this.resolveAtImportPath(p, basePath);
        if (r.kind === 'skip') return full;
        if (r.kind === 'blocked') {
          // WORKSPACE CONFINEMENT: never inline a file outside the launch cwd. A crafted
          // CLAUDE.md/AGENTS.md could otherwise exfiltrate arbitrary files into the system
          // prompt via `@~/.ssh/id_rsa`, `@/etc/passwd`, or `@../../other/secret`. Leave a
          // visible marker (so it is never silent) and notify via the hook.
          if (this._hookCallback) this._hookCallback('instructionsLoaded', { filePath: r.resolved, loadReason: 'import-blocked-outside-workspace' });
          return `${pre}[import blocked — outside workspace: ${path.basename(r.resolved)}]`;
        }
        const { real, resolved } = r;
        // A sanctioned approved-external file fires a DISTINCT load reason so the
        // decision is observable (parity: the escape-hatch branch is auditable).
        if (r.external && this._hookCallback) this._hookCallback('instructionsLoaded', { filePath: real, loadReason: 'import-allowed-external-approved' });
        // Cycle detection keys on the REAL path so two aliases of one file (one via
        // a symlink) can't slip past `seen` and re-enter the recursion.
        if (seen.has(real)) return `${pre}[circular import: ${path.basename(resolved)}]`;
        try {
          if (!fs.statSync(real).isFile()) return full;
          // Deny-by-default extension gate (parity with @include + the reference):
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
          // loadReason 'include' (parity: the reference reports a distinct 'include'
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

  /**
   * Build the `@include` hierarchy rooted at `filePath` as a flat, PARENT-BEFORE-CHILD
   * (pre-order DFS) list of {@link MemoryImportNode} — each tagged with the file that
   * imported it and its nesting depth. Parity with the reference's `processMemoryFile`
   * (the reference's markdown-memory module), which models `@includes` as SEPARATE parent-tracked entries
   * (main file first, then its imports) rather than AMI's inline substitution — giving
   * an OBSERVABLE import graph with per-file provenance (surfaced via `/memory`) without
   * changing what the model is injected. Uses the SAME security-cleared resolver
   * ({@link resolveAtImportPath}) and prose-line scan ({@link prosePlainLines}) as
   * {@link resolveAtImports}, so a token the inline path would refuse (code span,
   * outside-workspace, non-text, cycle) never appears in the graph either. Cycles,
   * depth>5, blocked / non-existent / non-text targets are skipped (never looped).
   */
  buildImportGraph(filePath: string): MemoryImportNode[] {
    const out: MemoryImportNode[] = [];
    const seen = new Set<string>();
    // The ROOT is an explicitly-chosen entrypoint (not an untrusted import), so it is
    // symlink-resolved WITHOUT cwd-confinement — a user-global `~/.ami/CLAUDE.md` is a
    // legitimate root. Its @-imports, however, stay cwd-confined via resolveAtImportPath,
    // exactly matching what resolveAtImports would actually inline.
    const rootReal = realpathDeepest(path.resolve(this.cwd, filePath));
    if (rootReal === null) return out;
    const walk = (real: string, parent: string | null, depth: number): void => {
      if (depth > 5 || seen.has(real)) return;
      // Gate exactly like the inline resolver: a node appears only for an EXISTING,
      // text-eligible, readable file — a non-text/missing/unreadable target (root or
      // child) contributes nothing (parity: processMemoryFile skips empty/unreadable).
      let content: string;
      try {
        if (!this._statSync(real).isFile() || !isTextImportPath(real)) return;
        content = fs.readFileSync(real, 'utf-8');
      } catch { return; }
      seen.add(real);
      out.push({ path: real, parent, depth });
      for (const prose of prosePlainLines(content)) {
        const re = new RegExp(AT_IMPORT_PATTERN, 'g');
        let m: RegExpExecArray | null;
        while ((m = re.exec(prose)) !== null) {
          const r = this.resolveAtImportPath(m[2], real);
          if (r.kind === 'ok') walk(r.real, real, depth + 1);
        }
      }
    };
    walk(rootReal, null, 0);
    return out;
  }

  /**
   * Import-hierarchy views for the user + project instruction entrypoints, as
   * pre-formatted indented tree rows (parity: the reference MemoryFileSelector's nested
   * `L <path>` view). Only an entrypoint that actually `@`-imports another file yields
   * a tree, so a flat CLAUDE.md/AGENTS.md contributes nothing. Reuses
   * {@link enumerateInstructionCandidates} (no duplicate entrypoint discovery) and
   * {@link buildImportGraph}; powers the `/memory` surface's import-tree section.
   */
  getInstructionImportTrees(): Array<{ scope: 'user' | 'project'; rows: string[] }> {
    const trees: Array<{ scope: 'user' | 'project'; rows: string[] }> = [];
    for (const cand of this.enumerateInstructionCandidates()) {
      if (!cand.exists) continue;
      const nodes = this.buildImportGraph(cand.path);
      if (nodes.some(n => n.depth > 0)) {
        trees.push({ scope: cand.scope, rows: formatImportTree(nodes, this.cwd) });
      }
    }
    return trees;
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

    // Per-tier enable gates (parity: the reference's per-source isSettingSourceEnabled).
    const projectEnabled = isMemorySourceEnabled('project');
    const localEnabled = isMemorySourceEnabled('local');

    // Nested-worktree de-duplication (parity: the reference the reference's markdown-memory module): when cwd
    // is a git worktree nested inside its own main repo, the plain upward walk stops at
    // the worktree's `.git` and MISSES the main repo's gitignored Local files. Once the
    // walk passes the worktree root we continue up to the canonical main root in a
    // `localOnly` mode — loading ONLY `*.local.md` (which exist only in the main
    // checkout) and SKIPPING checked-in Project files (already present in the worktree's
    // own checkout, so loading them again from the main tree would double them).
    const nested = detectNestedWorktree(this.cwd);
    let localOnly = false;

    while (depth <= MAX_PARENT_SCAN_DEPTH || localOnly) {
      const found = (projectEnabled && !localOnly) ? this.findInstructionFile(currentDir) : null;
      if (found && !isInstructionExcluded(found, excludes)) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            loaded.add(found);
            const content = this.resolveAtImports(sanitizeContent(raw, found), found);
            const relativePath = path.relative(this.cwd, found) || path.basename(found);
            // Per-source provenance/trust label (parity: the reference's getClaudeMds
            // descriptive suffixes) — tells the model whether editing this file is
            // version-controlled/shared vs. local, surfacing WHERE the instruction
            // came from rather than a bare path header.
            const prefix = depth === 0 ? '' : '(parent) ';
            const label = `${prefix}${relativePath} (${PROVENANCE_PROJECT})`;
            sections.push(`## ${label}\n\n${content}`);
            // Differentiated InstructionsLoaded load reason + memory_type (parity:
            // the reference reports session_start for the first/own load and
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
          // shared errno classifier instead of vanishing (parity: the reference's
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
      // linked worktree / submodule — stop at either. The ONE exception: in a nested
      // worktree, the worktree root's own `.git` stop would miss the main repo's Local
      // files, so instead of stopping we flip into `localOnly` mode and keep walking up
      // to the canonical main root (whose `.git` DIR then ends the walk).
      if (isGitRoot(currentDir)) {
        if (nested && !localOnly && path.normalize(currentDir) === path.normalize(nested.worktreeRoot)) {
          localOnly = true;
        } else {
          break;
        }
      }
      currentDir = parentDir;
      depth++;
    }

    // ADDITIONAL directories (parity: the reference's CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD
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
          // apart from a cwd/parent one (parity: the reference's additional-directory branch).
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
    return this.loadMemoriesWithMeta().entries;
  }

  /**
   * Like {@link loadMemories}, but also reports `totalFound` — the number of
   * valid memory entries discovered BEFORE the {@link MAX_MEMORY_FILES} cap is
   * applied. When `totalFound > entries.length` the index was clipped (the oldest
   * memories were dropped); {@link getMemoryContext} uses this to surface a
   * visible truncation note so the clipping is never silent.
   */
  loadMemoriesWithMeta(): { entries: MemoryEntry[]; totalFound: number } {
    const memoryDir = this.getMemoryDir();
    let files: string[];

    try {
      // RECURSIVE scan (parity: the reference memdir/memoryScan.ts `readdir(dir,
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
      return { entries: [], totalFound: 0 };
    }

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
    // `totalFound` is the pre-cap count so the caller can tell the model when
    // the index was clipped (the oldest memories are not in context).
    return { entries: entries.slice(0, MAX_MEMORY_FILES), totalFound: entries.length };
  }

  /**
   * Single-pass, header-ONLY scan of the stored-memory directory: for every memory
   * file, read a BOUNDED prefix ({@link FRONTMATTER_MAX_LINES} lines) to parse its
   * frontmatter and reuse the mtime/size from the SAME stat call — never slurping the
   * body. Parity with the reference's `scanMemoryFiles` (memdir/memoryScan.ts): a
   * listing/manifest surface (the `/memory` menu) that only needs names, descriptions
   * and types must not pay to read every body, and a runaway body can never force a
   * giant read. Returns headers sorted newest-first, capped like {@link loadMemories};
   * the `team/` subtree and the `MEMORY.md` index are excluded (both loaded elsewhere).
   */
  scanMemoryHeaders(): MemoryHeader[] {
    const memoryDir = this.getMemoryDir();
    let files: string[];
    try {
      files = (fs.readdirSync(memoryDir, { recursive: true }) as string[])
        .filter(f => isMemoryHeaderFile(f));
    } catch (err) {
      logUnexpectedReadError(err, memoryDir);
      return [];
    }

    const headers: MemoryHeader[] = [];
    for (const file of files) {
      const filePath = path.join(memoryDir, file);
      try {
        const stat = this._statSync(filePath);
        if (!stat.isFile()) continue;
        const head = readFileHeadBounded(filePath, FRONTMATTER_MAX_LINES);
        headers.push(buildMemoryHeader(filePath, file, head, stat.mtimeMs, stat.size));
      } catch {
        // Per-file graceful failure — skip an unreadable file rather than aborting
        // the whole scan (mirrors scanMemoryFiles' per-file allSettled filtering).
      }
    }
    headers.sort((a, b) => b.mtimeMs - a.mtimeMs);
    return headers.slice(0, MAX_MEMORY_FILES);
  }

  /**
   * Async, PARALLEL, CANCELLABLE sibling of {@link scanMemoryHeaders}, matching
   * the reference's `scanMemoryFiles(dir, signal)` exactly (memdir/memoryScan.ts):
   *
   *   - **Parallel reads** — every header is read concurrently via
   *     `Promise.allSettled`, so a large store's headers load in one I/O wave
   *     instead of a serial blocking loop on the hot path (gap: AMI's scan was
   *     fully sequential).
   *   - **Per-file graceful failure** — a rejected read (unreadable file, or one
   *     dropped by an abort) is filtered out; a single bad file never aborts the
   *     whole scan (parity with the reference's `filter(r => r.status === 'fulfilled')`).
   *   - **Cancellation** — an optional {@link AbortSignal} is forwarded into each
   *     read ({@link readFileHeadBoundedAsync}); once aborted, in-flight reads
   *     reject and are dropped, so a slow scan can be cancelled mid-flight (gap:
   *     AMI's scan had no signal/abort parameter).
   *
   * The result is identical in SHAPE to {@link scanMemoryHeaders} — the same
   * file filter, header construction, newest-first sort, and {@link MAX_MEMORY_FILES}
   * cap are reused (no duplicated logic) — so the two differ only in execution
   * strategy (sync-sequential vs async-parallel-cancellable).
   */
  async scanMemoryHeadersAsync(signal?: AbortSignal): Promise<MemoryHeader[]> {
    const memoryDir = this.getMemoryDir();
    let files: string[];
    try {
      files = ((await fsp.readdir(memoryDir, { recursive: true })) as string[])
        .filter(f => isMemoryHeaderFile(f));
    } catch (err) {
      logUnexpectedReadError(err, memoryDir);
      return [];
    }

    const settled = await Promise.allSettled(
      files.map(async (file): Promise<MemoryHeader | null> => {
        signal?.throwIfAborted();
        const filePath = path.join(memoryDir, file);
        const stat = await fsp.stat(filePath);
        if (!stat.isFile()) return null;
        const head = await readFileHeadBoundedAsync(filePath, FRONTMATTER_MAX_LINES, signal);
        return buildMemoryHeader(filePath, file, head, stat.mtimeMs, stat.size);
      }),
    );

    const headers = settled
      .filter((r): r is PromiseFulfilledResult<MemoryHeader | null> => r.status === 'fulfilled')
      .map(r => r.value)
      .filter((h): h is MemoryHeader => h !== null);
    headers.sort((a, b) => b.mtimeMs - a.mtimeMs);
    return headers.slice(0, MAX_MEMORY_FILES);
  }

  /**
   * Return the stored memory files whose raw content exceeds the recommended
   * {@link MAX_MEMORY_CHARACTER_COUNT} limit. Parity with the reference's
   * `getLargeMemoryFiles` (the reference's markdown-memory module): an oversized memory file is
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
   * `/doctor` surface. Parity with the reference's `buildMemoryDiagnostics`
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
   * @param addToIndex - When true, perform the reference's TWO-STEP save: after
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
    bestEffortChmod(filePath, MEMORY_FILE_MODE);

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
        bestEffortChmod(indexPath, MEMORY_FILE_MODE);
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
    // Per-tier enable gate (parity: the reference's isSettingSourceEnabled('userSettings')):
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
   * with no way to do it (parity with the reference's extractMemories "if they ask
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
        bestEffortChmod(indexPath, MEMORY_FILE_MODE);
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
   * `### Searching past context` guidance (the reference memdir/memdir.ts
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
    // load. Mirrors the reference's isAutoMemoryEnabled dropping the memory section.
    if (!isAutoMemoryEnabled(process.env, opts?.autoMemoryEnabled)) {
      return sections.join('\n\n');
    }

    // Whether any instruction content precedes the memory system. Used to decide
    // whether to surface the empty-memory affordance (below): a project that has
    // instructions but no memories yet should still learn that a persistent
    // memory system exists, while a completely blank context stays empty so
    // getMemoryContext's "no content ⇒ empty string" contract is preserved.
    const hasInstructionContent = sections.length > 0;

    const { entries: memories, totalFound: totalMemoryFiles } = this.loadMemoriesWithMeta();
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
          // the reference's formatMemoryManifest, memdir/memoryScan.ts) — gives the
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

    // Empty-memory affordance (parity with the reference's buildMemoryPrompt
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

    // Index-clipped diagnostic: when more memory topic files exist than the
    // per-turn cap, only the newest MAX_MEMORY_FILES are loaded and the rest are
    // dropped from context. Surface this (never silently) so the model knows its
    // index outgrew the budget and can consolidate — mirrors the reference's
    // visible entrypoint truncation warning (truncateEntrypointContent).
    if (totalMemoryFiles > memories.length) {
      sections.push(
        `## Memory Health Warning\n` +
        `Only the ${MAX_MEMORY_FILES} most recent memories are loaded (${totalMemoryFiles} topic files exist); ` +
        `the oldest ${totalMemoryFiles - memories.length} are NOT in context. ` +
        `Consolidate or remove stale topic files (use /memory to edit) to keep the index within the limit.`,
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
   * the reference's findRelevantMemories `alreadySurfaced`).
   *
   * `opts.recentTools` — when a tool is in active use, its pure usage/API
   * reference memory is noise (the conversation already demonstrates it), so
   * such `reference`-type memories are dropped; warning/gotcha/known-issue
   * memories about the same tool are preserved (mirrors the `recentTools` rule).
   *
   * `opts.relevance` — override the BM25 scoring constants for this call; any
   * omitted field falls back to the env-resolved {@link resolveMemoryRelevanceConfig}
   * (`AMI_MEMORY_BM25_K1` / `_B` / `_AVGDL`), so recall precision is tunable
   * per-call or per-deployment without changing the scorer.
   */
  selectRelevantMemories(
    query: string,
    maxResults: number = 5,
    opts?: {
      recentTools?: readonly string[];
      alreadySurfaced?: ReadonlySet<string>;
      relevance?: Partial<MemoryRelevanceConfig>;
    },
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
      // Tokenize each memory ONCE and reuse the token arrays for both the docFreq
      // tally and the per-memory relevance score. The old code tokenized every
      // memory twice per recall — once here and again inside computeRelevanceScore —
      // doubling the toLowerCase + regex + split pass over the full corpus.
      const allMemTokens = allMemTexts.map(tokenize);
      for (const tokens of allMemTokens) {
        const unique = new Set(tokens);
        for (const t of unique) {
          docFreq.set(t, (docFreq.get(t) || 0) + 1);
        }
      }

      const idfMap = new Map<string, number>();
      for (const qt of queryTokens) {
        const df = docFreq.get(qt) || 0;
        idfMap.set(qt, Math.log((N - df + 0.5) / (df + 0.5) + 1));
      }

      // BM25 constants: env-resolved defaults, overridable per-call via opts.relevance.
      const relevanceCfg: MemoryRelevanceConfig = {
        ...resolveMemoryRelevanceConfig(),
        ...opts?.relevance,
      };
      const scored = all.map((mem, i) => ({
        mem,
        score: computeRelevanceScore(allMemTokens[i], queryTokens, idfMap, relevanceCfg),
      }));

      scored.sort((a, b) => b.score - a.score);
      selected = scored.slice(0, maxResults).map(s => s.mem);
    }

    this.logRecallShape(candidateCount, selected, queryTokens.length);
    return selected;
  }

  /**
   * Emit a PII-free memory recall-shape diagnostic for one {@link selectRelevantMemories}
   * call. Parity with the reference's `logMemoryRecallShape`
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
  /**
   * Whether shared TEAM-memory loading is enabled for this workspace, resolved once
   * per instance from the config waterfall ({@link resolveTeamMemoryLoadingEnabled},
   * default ON) and reused. An explicit `teamMemoryEnabled: false` in any config tier
   * (project/global/local/managed) turns the shared-store read OFF.
   */
  private teamMemoryLoadingEnabled(): boolean {
    if (this._teamMemoryLoadingEnabled === null) {
      this._teamMemoryLoadingEnabled = resolveTeamMemoryLoadingEnabled(this.cwd);
    }
    return this._teamMemoryLoadingEnabled;
  }

  loadTeamMemories(): string[] {
    // Config off-switch: when a tier sets `teamMemoryEnabled: false`, the shared
    // team-memory subtree is never read/rendered (parity: the reference's TEAMMEM enable gate).
    if (!this.teamMemoryLoadingEnabled()) return [];
    const teamDir = path.join(this.getMemoryDir(), 'team');
    if (!fs.existsSync(teamDir)) return [];
    return this.loadMemoriesFromDir(teamDir);
  }

  private loadMemoriesFromDir(dir: string): string[] {
    try {
      const out: string[] = [];
      // RECURSIVE scan (parity: the reference memdir/memoryScan.ts `readdir(dir,
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
        // the credential can be removed. Mirrors the reference's destination-scoped
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
    // Auto-memory lives GLOBALLY under ~/.ami/projects/<slug>/memory (like the reference's
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
   * Snapshot of the shared TEAM-memory subsystem for introspection surfaces
   * (`/memory`, `/doctor`). Parity with the reference's team-memory entrypoint
   * reporting (memdir/teamMemPaths.ts `isTeamMemoryEnabled` + `getTeamMemPath` /
   * `getTeamMemEntrypoint`): tells the user WHETHER the team store is active, WHERE
   * it lives, its always-loaded `MEMORY.md` entrypoint, and HOW MANY shared memory
   * files it currently holds — so a disabled or empty team store is visible rather
   * than silently contributing nothing. `fileCount` reuses the canonical
   * {@link countMemoriesByScope} team tally (no second traversal/definition).
   */
  getTeamMemoryState(): { enabled: boolean; dir: string; entrypoint: string; fileCount: number } {
    const dir = this.getTeamMemoryDir();
    return {
      enabled: isTeamMemoryEnabled(),
      dir,
      entrypoint: path.join(dir, MEMORY_INDEX_FILENAME),
      fileCount: this.countMemoriesByScope().team,
    };
  }

  /**
   * Count persisted memory topic files split by scope, for the save-confirmation
   * summary surfaced by the `write_memory` tool. Mirrors the reference's
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
   * with the reference's `projectIsInGitRepo` (utils/git.ts): used to describe whether
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
   * verb on git membership (parity with the reference's MemoryFileSelector
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
   * Enumerate the canonical USER and PROJECT instruction-file entrypoints with an
   * `exists` flag — so the `/memory` surface can offer a "(new)" affordance for an
   * absent one, telling the user they CAN create it (parity with the reference's
   * MemoryFileSelector, which synthesizes non-existent User/Project CLAUDE.md rows
   * labeled `(new)`). When a tier already has an instruction file it is reported as
   * existing at its real discovered path (reusing {@link findInstructionFile} — the
   * same primitive the loader uses, so this can never diverge from what is actually
   * loaded); otherwise the canonical creatable path (`CLAUDE.md`) is returned with
   * `exists:false`. User tier checks `~/.ami` then `~/.claude`; project tier checks
   * the cwd.
   */
  enumerateInstructionCandidates(): { scope: 'user' | 'project'; path: string; relativePath: string; exists: boolean }[] {
    const canonical = INSTRUCTION_FILES[0]; // 'CLAUDE.md' — highest-priority creatable entrypoint
    const userBase = path.join(os.homedir(), '.ami');
    const existingUser = this.findInstructionFile(userBase)
      ?? this.findInstructionFile(path.join(os.homedir(), '.claude'));
    const userPath = existingUser ?? path.join(userBase, canonical);
    const cwd = path.resolve(this.cwd);
    const existingProject = this.findInstructionFile(cwd);
    const projectPath = existingProject ?? path.join(cwd, canonical);
    const projectRel = path.relative(this.cwd, projectPath) || path.basename(projectPath);
    return [
      { scope: 'user', path: userPath, relativePath: abbreviateHome(userPath), exists: existingUser !== null },
      { scope: 'project', path: projectPath, relativePath: projectRel, exists: existingProject !== null },
    ];
  }

  /**
   * Is `absPath` inside the auto-memory tree at all (private root OR the nested
   * team sub-dir)? Traversal-safe: the boundary check normalizes/expands and
   * symlink-resolves via the shared {@link isPathWithin} primitive, so a
   * `..`-laden or symlinked path cannot spoof membership. Mirrors the reference's
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
   * the reference's `isMemoryFilePath` (the reference's markdown-memory module): recognizes a path as a
   * memory file so an edit to it can trigger a memory-section refresh. Narrows
   * {@link isMemoryPath} to markdown so a sibling non-memory artifact in the dir
   * is not misclassified.
   */
  isMemoryFilePath(absPath: string): boolean {
    return absPath.toLowerCase().endsWith('.md') && this.isMemoryPath(absPath);
  }

  /**
   * Is `absPath` inside the shared TEAM memory sub-dir? Traversal-safe (shared
   * {@link isPathWithin}). Parity with the reference's `isTeamMemFile`
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
   * keeps a shared path from being mislabeled `personal`. Mirrors the reference's
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
   * `External` when it is outside every memory store). Mirrors the reference's
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
   * "excludes" half of the reference's `isAutoManagedMemoryFile`
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
   * a human-authored instruction file? Parity with the reference's
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
   * operation or `null` when it does not. Parity with the reference's
   * `isTeamMemorySearch` + `isTeamMemoryWriteOrEdit` (utils/teamMemoryOps.ts): a
   * read/search tool (`file_read`, `grep`, `glob`) whose `path`/`file_path`
   * targets a team file is `'search'`; a mutation tool (`file_edit`) whose
   * `file_path`/`path` targets a team file is `'write'`. Gated by
   * {@link isTeamMemoryEnabled} — when the team-memory capability is off this is
   * always `null` (matching the reference's `feature('TEAMMEM')` gate), so no
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
   * Notify that a tool use MUTATED a shared team-memory file, so a live sync /
   * reload can fire. Parity with the reference's `sessionFileAccessHooks`
   * PostToolUse path, which — for Edit/Write ONLY (never Read) on an
   * `isTeamMemFile` path — calls `teamMemWatcher.notifyTeamMemoryWrite()` to
   * re-sync the shared store. AMI previously loaded team memories only at
   * prompt-build time with no write detection; this closes that gap by emitting
   * a `teamMemoryWrite` hook event (consumed via {@link setHookCallback} by a
   * sync service / UI) exactly when a shared memory file is edited or written.
   *
   * Returns `true` when a notification fired. Fires only when ALL hold:
   *   - team memory is enabled ({@link isTeamMemoryEnabled}, the reference's `TEAMMEM` gate);
   *   - the tool is a mutation (`file_edit` or `file_write`) — a `file_read`
   *     (or any non-mutating tool) NEVER notifies, mirroring the reference's
   *     Edit/Write-only switch arms;
   *   - the path resolves inside the shared team store ({@link isTeamMemFile}).
   */
  notifyTeamMemoryWrite(
    toolName: string,
    absPath: string,
    env: NodeJS.ProcessEnv = process.env,
  ): boolean {
    if (!isTeamMemoryEnabled(env)) return false;
    if (toolName !== 'file_edit' && toolName !== 'file_write') return false;
    if (!this.isTeamMemFile(absPath)) return false;
    if (this._hookCallback) {
      this._hookCallback('teamMemoryWrite', {
        filePath: absPath,
        scope: 'team',
        location: this.getMemoryScopeDisplay(absPath),
      });
    }
    return true;
  }

  /**
   * Absolute path of the DATE-PARTITIONED daily memory log for `date` (default:
   * today): `<memoryDir>/logs/YYYY/MM/YYYY-MM-DD.md`. Parity with the reference's
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
    bestEffortChmod(logPath, MEMORY_FILE_MODE);
    return logPath;
  }

  /**
   * Create `absPath` as an empty file IF it does not already exist, without
   * clobbering an existing file (EEXIST-safe `wx` write via
   * {@link createFileIfAbsent}). Parity with the reference's `handleSelectMemoryFile`
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
    const filePath = this.getSessionMemoryPath(sessionId);

    const lines: string[] = [
      '---',
      `name: session-${safeId}`,
      `description: Auto-extracted session facts`,
      'type: project',
      '---',
      '',
    ];

    // Group facts by category (shared, prototype-safe grouping helper).
    const grouped = groupBy(facts, (fact) => fact.category);

    for (const [category, categoryFacts] of grouped) {
      lines.push(`### ${category}`);
      for (const f of categoryFacts) {
        // Credential boundary: a secret captured in an extracted fact must not be
        // written to the persisted session-memory file.
        lines.push(`- ${redactCredentials(f.fact)} (confidence: ${f.confidence})`);
      }
      lines.push('');
    }

    const fileContent = lines.join('\n');
    fs.writeFileSync(filePath, fileContent, { encoding: 'utf-8', mode: MEMORY_FILE_MODE });
    bestEffortChmod(filePath, MEMORY_FILE_MODE);

    // No-PII introspection surface for the auto-extracted session-memory WRITE path
    // (parity with the reference's `tengu_session_memory_file_read` /
    // `sessionMemory.ts` content-length tracking, services/SessionMemory): record
    // HOW MANY facts were persisted and the resulting file size — never the fact
    // text or the path — so an operator can observe session-memory growth.
    logForDiagnosticsNoPII('info', 'session_memory_saved', {
      content_length: fileContent.length,
      fact_count: facts.length,
    });
  }

  /**
   * Absolute path of the auto-extracted session-memory notes file for `sessionId`
   * (`<memoryDir>/session-<safeId>.md`). The session id is path-sanitized to a safe
   * slug so a hostile id cannot escape the memory directory. Single source of the
   * session-memory path shared by {@link saveSessionMemory} / {@link loadSessionMemory};
   * also the introspection accessor a `/memory`-style surface uses to show WHERE the
   * current conversation's auto-extracted notes live (parity with the reference's
   * `getSessionMemoryPath`, services/SessionMemory/sessionMemoryUtils.ts).
   */
  getSessionMemoryPath(sessionId: string): string {
    const safeId = sessionId.replace(/[^a-zA-Z0-9_-]/g, '_');
    return path.join(this.getMemoryDir(), `session-${safeId}.md`);
  }

  /**
   * Load session memory facts from a session-specific markdown file.
   *
   * Parses the `.ami/memory/session-<sessionId>.md` file and
   * returns an array of SessionFact objects.
   */
  loadSessionMemory(sessionId: string): SessionFact[] {
    const filePath = this.getSessionMemoryPath(sessionId);

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

    // No-PII introspection event for the session-memory LOAD path — emitted only on
    // a successful read (an ENOENT/other fallback returns early above), mirroring
    // the reference's `logEvent('tengu_session_memory_loaded', { content_length })`
    // (services/SessionMemory/sessionMemoryUtils.ts): it makes the reinjection of
    // persisted session memory into the conversation observable (how much was
    // reloaded + how many facts) without logging any fact text or the path.
    logForDiagnosticsNoPII('info', 'session_memory_loaded', {
      content_length: raw.length,
      fact_count: facts.length,
    });

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
    } catch (err) {
      // Distinguish the EXPECTED "no rules dir" case (ENOENT/ENOTDIR — silent
      // fallback) from an UNEXPECTED IO failure (EACCES/EMFILE/EIO — the dir
      // exists but could not be read), which is surfaced via the memory logger so
      // a transient error is never silently equated with "zero managed rules".
      logUnexpectedReadError(err, rulesDir);
    }

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
    } catch (err) {
      // Same errno discrimination as loadRulesDirectory: ENOENT/ENOTDIR is the
      // silent "no rules dir" fallback; any other code (EACCES/EMFILE/EIO) is a
      // real failure and is logged rather than masked as "no file-scoped rules".
      logUnexpectedReadError(err, rulesDir);
    }

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
    // Serialize against every other index mutation on this manager so two
    // concurrent read-then-write cycles cannot interleave (the second would
    // otherwise clobber the first's entry). See {@link _serializeIndexMutation}.
    return this._serializeIndexMutation(async () => {
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
          await bestEffortChmodAsync(indexPath, MEMORY_FILE_MODE);
        }
      } catch { /* best effort */ }
    });
  }

  /**
   * Remove an entry from the MEMORY.md index file.
   * Matches by substring — if any line contains the given text, it is removed.
   */
  async removeFromMemoryIndex(cwd: string, entrySubstring: string): Promise<void> {
    // Share the serializer with {@link addToMemoryIndex} so an append and a
    // removal that race do not read-then-write over each other.
    return this._serializeIndexMutation(async () => {
      const indexPath = path.join(projectStateDir(cwd, 'memory'), 'MEMORY.md');
      try {
        const content = await fsp.readFile(indexPath, 'utf-8');
        const lines = content.split('\n').filter(l => !l.includes(entrySubstring));
        await fsp.writeFile(indexPath, lines.join('\n'), 'utf-8');
      } catch { /* best effort */ }
    });
  }

  // ── Memory / transcript file-access classification ─────────────────────

  /** The directories that define what counts as memory/transcript for this
   *  workspace, for {@link detectMemoryFileAccess} / {@link classifyToolSessionFile}. */
  private _memoryDetectDirs(): MemoryDirs {
    return {
      memoryDir: this.getMemoryDir(),
      // Session transcripts live in the project's sessions store, keyed on the
      // canonical git root (same keying as {@link getMemoryDir}).
      transcriptDir: projectStateDir(canonicalGitRoot(this.cwd), 'sessions'),
    };
  }

  /**
   * Reusable predicate (parity: the reference `isMemoryFileAccess`): given a tool
   * name and its raw input, report whether the call touches a memory or session
   * file for THIS workspace. Lets a consumer re-label generic
   * `file_read`/`grep`/`glob`/`file_edit` activity as a memory operation.
   */
  detectMemoryFileAccess(toolName: string, toolInput: unknown): boolean {
    return isMemoryFileAccess(toolName, toolInput, this._memoryDetectDirs());
  }

  /**
   * Classify a tool call to its session-file type (`'session_memory'` |
   * `'session_transcript'` | `null`) for THIS workspace. Parity with the reference
   * Code's `getSessionFileTypeFromInput`.
   */
  classifyToolSessionFile(toolName: string, toolInput: unknown): SessionFileType | null {
    return classifyToolSessionFileType(toolName, toolInput, this._memoryDetectDirs());
  }

  /**
   * Count the memory/session file accesses across a run of transcript entries for
   * THIS workspace — the "N memories recalled" figure. Reuses the canonical
   * {@link countMemoryFileAccessFromEntries} (same predicate as
   * {@link detectMemoryFileAccess}), resolving the workspace's memory/transcript
   * dirs. `normalizeToolName` maps persisted (reference-journal) tool names into
   * the AMI domain the predicate speaks. Parity: the reference's
   * `countMemoryFileAccessFromEntries`.
   */
  countMemoryFileAccessFromEntries(
    entries: ReadonlyArray<MemoryAccessEntry>,
    normalizeToolName?: (name: string) => string,
  ): number {
    return countMemoryFileAccessFromEntries(entries, this._memoryDetectDirs(), normalizeToolName);
  }

  /**
   * Does a grep/glob PATTERN target auto-managed memory files only (never a
   * user-managed instruction file)? Thin wrapper over the canonical
   * {@link isAutoManagedMemoryPattern} so a consumer can classify a glob-only
   * search (no concrete path) as a memory operation. Parity: the reference's
   * `isAutoManagedMemoryPattern`.
   */
  isAutoManagedMemoryPattern(pattern: string): boolean {
    return isAutoManagedMemoryPattern(pattern);
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
    // Per-tier enable gates (parity: the reference's per-source isSettingSourceEnabled);
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
      //    distinct from a hand-authored user-global instruction (parity: the reference's
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
    //    (parity: the reference's 'AutoMem' scope) so they are distinguishable from a
    //    hand-authored user-global instruction in provenance surfaces.
    const memories = this.loadMemories();
    for (const mem of memories) {
      sources.push({ content: mem.content, level: 'automem', path: mem.path });
    }

    // 6. Shared team-memory store — a FIRST-CLASS 'team' scope, loaded ONLY when
    //    the team-memory capability is enabled (parity: the reference's feature('TEAMMEM')
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
   * diagnostic (parity with the reference's `memory_files_completed` /
   * `tengu_claudemd__initial_load` events, the reference's markdown-memory module:L1014-L1040) for machine
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
    // size and timing), mirroring the reference's diagnostics event so an operator can
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
