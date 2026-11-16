import * as fs from 'fs';
import {
  projectStateDir,
  canonicalGitRoot,
  expandConfigDir,
  isDangerousStorageRoot,
} from './reference-journal';
import * as fsp from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import type { SessionFact } from './session-memory';
import { log as coreLog } from './logger';
import { redactCredentials } from './credential-guard';
import { isEnvTruthy } from './analytics';

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
 * Instruction source with level differentiation.
 * - managed: from .ami/rules/ directory (team-managed rules)
 * - user: from memory/ or user-specific files (CLAUDE.local.md, SUPERINFERENCE.local.md)
 * - project: from project instruction files (CLAUDE.md, SUPERINFERENCE.md, etc.)
 */
export interface InstructionSource {
  content: string;
  level: 'managed' | 'user' | 'project';
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
 *  separately (readMemoryIndex) and must be excluded from the memory-body scan. */
const MEMORY_INDEX_FILENAME = 'MEMORY.md';

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

function sanitizeContent(content: string, filePath: string): string {
  if (content.length > MAX_INSTRUCTION_FILE_SIZE) {
    coreLog('memory', `Instruction file ${filePath} exceeds ${MAX_INSTRUCTION_FILE_SIZE} bytes — truncating`);
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
    coreLog('memory', `Prompt injection patterns detected in ${filePath}`);
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

function memoryFreshness(ageDays: number): string {
  if (ageDays === 0) return 'today';
  if (ageDays === 1) return 'yesterday';
  if (ageDays <= 7) return `${ageDays} days ago`;
  if (ageDays <= 30) return `${Math.floor(ageDays / 7)} weeks ago`;
  return `${Math.floor(ageDays / 30)} months ago`;
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
      `Failed to parse frontmatter${sourcePath ? ` in ${sourcePath}` : ''}: unterminated '---' block`,
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
    if (colonIdx === -1) continue;
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

/** Per-file upper bound when scanning the memory dir: a single runaway file must
 *  not be slurped whole into the context/scan. */
const MAX_MEMORY_FILE_BYTES = 256 * 1024;

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

  constructor(cwd: string) {
    this.cwd = cwd;
  }

  setHookCallback(cb: (event: string, data: any) => void): void { this._hookCallback = cb; }

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
      lines[i] = lines[i].replace(/(^|\s)@(~?[\w./~-]+)/g, (full, pre, p) => {
        let resolved: string;
        try {
          resolved = p.startsWith('~')
            ? path.join(os.homedir(), p.slice(1))
            : path.resolve(path.dirname(basePath), p);
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
          return `${pre}${imported}`;
        } catch {
          return full;
        }
      });
    }
    return lines.join('\n');
  }

  loadProjectInstructions(): string {
    const sections: string[] = [];
    let currentDir = path.resolve(this.cwd);
    const root = path.parse(currentDir).root;
    let depth = 0;

    while (depth <= MAX_PARENT_SCAN_DEPTH) {
      const found = this.findInstructionFile(currentDir);
      if (found) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            const content = this.resolveAtImports(sanitizeContent(raw, found), found);
            const relativePath = path.relative(this.cwd, found) || path.basename(found);
            const label = depth === 0 ? relativePath : `(parent) ${relativePath}`;
            sections.push(`## ${label}\n\n${content}`);
            if (this._hookCallback) this._hookCallback('instructionsLoaded', { filePath: found, loadReason: 'startup' });
          }
        } catch {
          // File unreadable — skip silently
        }
      }

      for (const localFile of LOCAL_VARIANTS) {
        const localPath = path.join(currentDir, localFile);
        try {
          if (fs.statSync(localPath).isFile()) {
            const raw = fs.readFileSync(localPath, 'utf-8').trim();
            if (raw) {
              const content = this.resolveAtImports(sanitizeContent(raw, localPath), localPath);
              const relativePath = path.relative(this.cwd, localPath) || path.basename(localPath);
              const label = depth === 0 ? relativePath : `(parent) ${relativePath}`;
              sections.push(`## ${label}\n\n${content}`);
            }
          }
        } catch {
          // File doesn't exist or inaccessible — expected
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
      } catch {
        // File doesn't exist or inaccessible
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
        } catch {}
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
    } catch {
      // Directory doesn't exist yet — no memories
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
   * @returns The absolute path of the written memory file.
   */
  saveMemory(
    name: string,
    content: string,
    description?: string,
    memoryType?: MemoryType,
    scope: MemoryScope = 'private',
  ): string {
    // Route by scope: private memories live at the auto-memory root; team
    // memories live in the shared `team/` sub-directory (the one loadTeamMemories
    // reads), so the model can now AUTHOR a team memory, not just read one.
    const baseDir = this.getMemoryDir();
    const memoryDir = scope === 'team' ? path.join(baseDir, 'team') : baseDir;
    fs.mkdirSync(memoryDir, { recursive: true });

    const safeName = name
      .replace(/[^a-zA-Z0-9_-]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '')
      .toLowerCase();

    const filename = `${safeName}.md`;
    const filePath = path.join(memoryDir, filename);

    const type = memoryType ?? 'project';
    const desc = description ?? name;
    const frontmatter = buildFrontmatter(name, desc, type);
    // Credential/secret boundary: never persist an API key / token that slipped
    // into a memory body to a long-lived .md on disk.
    const fileContent = `${frontmatter}\n${redactCredentials(content)}\n`;

    fs.writeFileSync(filePath, fileContent, 'utf-8');
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
    const sections: string[] = [];
    for (const base of [path.join(os.homedir(), '.ami'), path.join(os.homedir(), '.claude')]) {
      const found = this.findInstructionFile(base);
      if (!found) continue;
      try {
        const raw = fs.readFileSync(found, 'utf-8').trim();
        if (raw) sections.push(this.resolveAtImports(sanitizeContent(raw, found), found));
      } catch { /* unreadable — skip */ }
    }
    return sections.join('\n\n');
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
      sections.push(`## User Instructions (global)\n\n${userGlobal}`);
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
          return `- ${typeTag}${m.name || 'Untitled'}${desc} (${m.content.length} chars${ageTag})`;
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

### Saving a memory
Lead feedback/project memories with the rule/fact, then a **Why:** line and a **How to apply:** line. Use this frontmatter:
${MEMORY_FRONTMATTER_EXAMPLE}

${TWO_STEP_SAVE_GUIDANCE}${driftCaveat}`;
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

    if (all.length <= maxResults) return all;

    const queryTokens = tokenize(query);
    if (queryTokens.length === 0) return all.slice(0, maxResults);

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
    return scored.slice(0, maxResults).map(s => s.mem);
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
      for (const f of fs.readdirSync(dir)) {
        if (!f.endsWith('.md')) continue;
        // The MEMORY.md index is a table of contents, not a memory body — exclude
        // it from the body scan (mirrors loadMemories) so a team/MEMORY.md index
        // isn't dumped verbatim into the prompt as if it were a memory.
        if (f === MEMORY_INDEX_FILENAME) continue;
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
   * Save session memory facts to a session-specific markdown file.
   *
   * Facts are stored as `.ami/memory/session-<sessionId>.md`
   * with each fact rendered as a bullet point grouped by category.
   */
  saveSessionMemory(sessionId: string, facts: SessionFact[]): void {
    if (facts.length === 0) return;

    const memoryDir = this.getMemoryDir();
    fs.mkdirSync(memoryDir, { recursive: true });

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

    fs.writeFileSync(filePath, lines.join('\n'), 'utf-8');
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
    } catch {
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
      if (realpathWithin(includePath, this.cwd) === null) {
        const replacement = `[include blocked — outside workspace: ${path.basename(includePath)}]`;
        result = result.slice(0, pos) + replacement + result.slice(pos + fullMatch.length);
        searchFrom = pos + replacement.length;
        continue;
      }

      if (seen.has(includePath)) {
        const replacement = `[circular reference: ${path.basename(includePath)}]`;
        result = result.slice(0, pos) + replacement + result.slice(pos + fullMatch.length);
        searchFrom = pos + replacement.length;
        continue;
      }
      seen.add(includePath);
      try {
        // Deny-by-default: only inline recognized text extensions (extensionless
        // files allowed). An allowlist can't be defeated by a novel/multi-dotted
        // binary extension the way the old binary DENYlist could.
        if (!isTextImportPath(includePath)) {
          const replacement = `[non-text file skipped: ${path.basename(includePath)}]`;
          result = result.slice(0, pos) + replacement + result.slice(pos + fullMatch.length);
          searchFrom = pos + replacement.length;
          seen.delete(includePath);
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
        seen.delete(includePath);
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
    } catch {
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
      await fsp.mkdir(path.dirname(indexPath), { recursive: true });
      let content = '';
      try { content = await fsp.readFile(indexPath, 'utf-8'); } catch { /* file doesn't exist */ }
      if (!content.includes(entry)) {
        content = content.trimEnd() + '\n' + entry + '\n';
        await fsp.writeFile(indexPath, content, 'utf-8');
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
    let currentDir = path.resolve(this.cwd);
    const root = path.parse(currentDir).root;
    let depth = 0;

    while (depth <= MAX_PARENT_SCAN_DEPTH) {
      const found = this.findInstructionFile(currentDir);
      if (found) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            const content = sanitizeContent(raw, found);
            sources.push({ content, level: 'project', path: found });
          }
        } catch { /* skip */ }
      }

      // 3. User-level local variants
      for (const localFile of LOCAL_VARIANTS) {
        const localPath = path.join(currentDir, localFile);
        try {
          if (fs.statSync(localPath).isFile()) {
            const raw = fs.readFileSync(localPath, 'utf-8').trim();
            if (raw) {
              const content = sanitizeContent(raw, localPath);
              sources.push({ content, level: 'user', path: localPath });
            }
          }
        } catch { /* skip */ }
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

    // 5. User memories
    const memories = this.loadMemories();
    for (const mem of memories) {
      sources.push({ content: mem.content, level: 'user', path: mem.path });
    }

    return sources;
  }
}
