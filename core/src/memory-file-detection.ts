/**
 * Memory / transcript file-access classifier.
 *
 * Parity with Claude Code's `utils/memoryFileDetection.ts` +
 * `utils/sessionFileAccessHooks.ts`: given a tool name and its raw input,
 * classify whether the call is touching a memory or session-transcript file.
 * This is a pure, telemetry-independent predicate the engine/TUI can use to
 * re-label generic `file_read` / `grep` / `glob` / `file_edit` activity as a
 * memory recall/search/write so the user can SEE when the agent is reading or
 * writing its own memory — distinct from ordinary project files.
 *
 * It is intentionally free of any global config read: the active directories are
 * passed in ({@link MemoryDirs}) so the functions stay pure and layer-clean
 * (`core/` self-contained building block). {@link MemoryManager} supplies the
 * concrete AMI paths via {@link MemoryManager.detectMemoryFileAccess}.
 */
import * as path from 'path';
import { isPathWithin } from './utils/path';

/** The two kinds of session-scoped file a tool call can target. */
export type SessionFileType = 'session_memory' | 'session_transcript';

/** The directories that define what counts as memory/transcript for a project. */
export interface MemoryDirs {
  /** Absolute memory directory (`MemoryManager.getMemoryDir()`). */
  memoryDir: string;
  /** Absolute directory holding session transcript files (`*.json`/`*.jsonl`).
   *  Omit to disable transcript classification. */
  transcriptDir?: string;
}

/**
 * Normalize a path/pattern to a stable string-comparable form: forward-slash
 * separators and, on Windows (case-insensitive filesystem), lowercase. Mirrors
 * Claude Code's `toComparable`.
 */
function toComparable(p: string): string {
  const posixForm = p.split('\\').join('/');
  return process.platform === 'win32' ? posixForm.toLowerCase() : posixForm;
}

/**
 * Classify a concrete file PATH. Returns `'session_memory'` for a
 * `session-<id>.md` file inside the memory dir, `'session_transcript'` for a
 * `*.json`/`*.jsonl` file inside the transcript dir, else `null`.
 */
export function detectSessionFileType(
  filePath: string,
  dirs: MemoryDirs,
): SessionFileType | null {
  if (!filePath) return null;
  const baseCmp = toComparable(path.basename(filePath));
  if (
    baseCmp.startsWith('session-') &&
    baseCmp.endsWith('.md') &&
    isPathWithin(filePath, dirs.memoryDir, dirs.memoryDir)
  ) {
    return 'session_memory';
  }
  if (
    dirs.transcriptDir &&
    (baseCmp.endsWith('.jsonl') || baseCmp.endsWith('.json')) &&
    isPathWithin(filePath, dirs.transcriptDir, dirs.transcriptDir)
  ) {
    return 'session_transcript';
  }
  return null;
}

/**
 * Classify a GLOB/PATTERN string (Grep `glob`/`include`, Glob `pattern`), where
 * we only have intent, not a real path. Mirrors Claude Code's
 * `detectSessionPatternType` heuristics.
 */
export function detectSessionPatternType(
  pattern: string,
): SessionFileType | null {
  if (!pattern) return null;
  const normalized = toComparable(pattern);
  if (
    normalized.includes('session-') &&
    (normalized.includes('.md') || normalized.endsWith('*'))
  ) {
    return 'session_memory';
  }
  if (
    normalized.includes('.jsonl') ||
    (normalized.includes('sessions') && normalized.includes('.json'))
  ) {
    return 'session_transcript';
  }
  return null;
}

/** Pull the `file_path` from a path-taking tool's input (Read/Edit/Write). */
function filePathFromInput(toolName: string, input: unknown): string | null {
  if (!input || typeof input !== 'object') return null;
  const o = input as Record<string, unknown>;
  if (
    toolName === 'file_read' ||
    toolName === 'file_edit' ||
    toolName === 'file_write'
  ) {
    return typeof o.file_path === 'string' ? o.file_path : null;
  }
  return null;
}

/**
 * Classify a tool CALL (name + raw input) to its session-file type, checking an
 * explicit path first and the glob/pattern heuristics second — matching Claude
 * Code's `getSessionFileTypeFromInput` ordering. Returns `null` for a tool whose
 * input does not target a session file.
 */
export function classifyToolSessionFileType(
  toolName: string,
  input: unknown,
  dirs: MemoryDirs,
): SessionFileType | null {
  if (!input || typeof input !== 'object') return null;
  const o = input as Record<string, unknown>;
  switch (toolName) {
    case 'file_read':
    case 'file_edit':
    case 'file_write': {
      const fp = typeof o.file_path === 'string' ? o.file_path : null;
      return fp ? detectSessionFileType(fp, dirs) : null;
    }
    case 'grep': {
      // The real file-filter for grep is `glob`/`include`; `pattern` is the
      // SEARCH regex, never a file path, so it is intentionally not consulted.
      if (typeof o.path === 'string') {
        const t = detectSessionFileType(o.path, dirs);
        if (t) return t;
      }
      const glob =
        (typeof o.glob === 'string' && o.glob) ||
        (typeof o.include === 'string' && o.include) ||
        '';
      return glob ? detectSessionPatternType(glob) : null;
    }
    case 'glob': {
      if (typeof o.path === 'string') {
        const t = detectSessionFileType(o.path, dirs);
        if (t) return t;
      }
      return typeof o.pattern === 'string'
        ? detectSessionPatternType(o.pattern)
        : null;
    }
    default:
      return null;
  }
}

/**
 * Reusable predicate: does this tool call touch a memory or session file?
 * True when the call targets a `session_memory` file (Read/Grep/Glob) OR a
 * Read/Edit whose `file_path` lies within the memory dir (covers stored memory,
 * the MEMORY.md index, and the nested `team/` store). Mirrors Claude Code's
 * `isMemoryFileAccess` (session_memory OR isAutoMemFile/isTeamMemFile).
 */
export function isMemoryFileAccess(
  toolName: string,
  input: unknown,
  dirs: MemoryDirs,
): boolean {
  if (classifyToolSessionFileType(toolName, input, dirs) === 'session_memory') {
    return true;
  }
  const fp = filePathFromInput(toolName, input);
  if (fp && isPathWithin(fp, dirs.memoryDir, dirs.memoryDir)) {
    return true;
  }
  return false;
}

/**
 * The tool names whose calls can TOUCH a memory/session file — the allowlist a
 * transcript walker filters `tool_use` blocks by before applying
 * {@link isMemoryFileAccess}. Parity with Claude Code's
 * `MEMORY_ACCESS_TOOL_NAMES` (utils/attribution.ts): read, grep, glob, edit and
 * write (AMI tool names). A tool outside this set can never be a memory access,
 * so the walker skips it cheaply before the structural predicate runs.
 */
export const MEMORY_ACCESS_TOOL_NAMES: ReadonlySet<string> = new Set([
  'file_read',
  'grep',
  'glob',
  'file_edit',
  'file_write',
]);

/** One `tool_use` block of an assistant transcript entry (the only block kind the
 *  memory counter inspects — others are ignored). */
export interface MemoryToolUseBlock {
  type: string;
  name?: unknown;
  input?: unknown;
}

/**
 * A minimal shape of one transcript entry (reference-journal `AssistantEntry` /
 * Claude Code `Entry`): the walker only needs the entry `type` and the
 * assistant message's `content` blocks. Deliberately structural so it accepts
 * both a live {@link MemoryToolUseBlock}-bearing message and a persisted journal
 * line without coupling to either concrete type.
 */
export interface MemoryAccessEntry {
  type?: unknown;
  message?: { content?: unknown } | null;
}

/**
 * Count how many times the agent TOUCHED a memory/session file across a run of
 * transcript entries — the figure feeding the "N memories recalled" attribution
 * suffix. Walks each `assistant` entry's `tool_use` blocks, filters by
 * {@link MEMORY_ACCESS_TOOL_NAMES}, then applies the SAME {@link isMemoryFileAccess}
 * predicate the PostToolUse session-file-access guard uses — so a hook and this
 * counter never disagree. Parity with Claude Code's
 * `countMemoryFileAccessFromEntries` (utils/attribution.ts L213-243), covering
 * read/grep/glob/edit/write (not just reads).
 *
 * `normalizeToolName` maps a stored tool name into the AMI tool-name domain the
 * predicate/allowlist speak (the reference journal persists REFERENCE names —
 * `Read`/`Grep`/… — so a journal-reading caller supplies a ref→ami map); it
 * defaults to identity for callers that already pass AMI-named entries.
 */
export function countMemoryFileAccessFromEntries(
  entries: ReadonlyArray<MemoryAccessEntry>,
  dirs: MemoryDirs,
  normalizeToolName: (name: string) => string = (n) => n,
): number {
  let count = 0;
  for (const entry of entries) {
    if (!entry || entry.type !== 'assistant') continue;
    const content = entry.message?.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (!block || typeof block !== 'object') continue;
      const b = block as MemoryToolUseBlock;
      if (b.type !== 'tool_use' || typeof b.name !== 'string') continue;
      const amiName = normalizeToolName(b.name);
      if (!MEMORY_ACCESS_TOOL_NAMES.has(amiName)) continue;
      if (isMemoryFileAccess(amiName, b.input, dirs)) count++;
    }
  }
  return count;
}

/** Normalized glob substrings that mark an auto-managed AGENT-memory store
 *  (AMI's `agent-memory-local/` / `agent-memory-remote/` trees, plus the generic
 *  `agent-memory/` form). These are curated by the memory system, never authored
 *  by hand, so a glob reaching them is a memory operation. */
const AGENT_MEMORY_GLOB_MARKERS = [
  'agent-memory/',
  'agent-memory-local/',
  'agent-memory-remote/',
];

/**
 * Does a GLOB/PATTERN target auto-managed memory files ONLY (never a
 * user-managed instruction file like AGENTS.md / `.ami/rules/`)? True when the
 * pattern reads as a session file ({@link detectSessionPatternType}) or reaches
 * an agent-memory tree. Used by collapse/badge logic so a grep/glob whose only
 * filter is a memory glob (no concrete path) is still recognized as a memory
 * search. Parity with Claude Code's `isAutoManagedMemoryPattern`
 * (utils/memoryFileDetection.ts L277-289).
 */
export function isAutoManagedMemoryPattern(pattern: string): boolean {
  if (!pattern) return false;
  if (detectSessionPatternType(pattern) !== null) return true;
  const normalized = toComparable(pattern);
  return AGENT_MEMORY_GLOB_MARKERS.some((m) => normalized.includes(m));
}
