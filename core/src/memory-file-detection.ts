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

/** Pull the `file_path` from a path-taking tool's input (Read/Edit). */
function filePathFromInput(toolName: string, input: unknown): string | null {
  if (!input || typeof input !== 'object') return null;
  const o = input as Record<string, unknown>;
  if (toolName === 'file_read' || toolName === 'file_edit') {
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
    case 'file_edit': {
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
