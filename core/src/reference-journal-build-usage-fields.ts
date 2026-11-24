// Split from reference-journal.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import type { DiffHunk } from './types';
import { COMPAT_VERSION, MAX_SESSION_ID_LENGTH, MAX_SLUG_LENGTH, canonicalizePath, cappedComponent, runGit } from './reference-journal-persist-positive-capability';

export function resolveGitRootUncached(cwd: string): string {
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
  return cappedComponent(slug, cwd, MAX_SLUG_LENGTH);
}

/**
 * Sanitize a session/agent id for use as a filename: neutralize separators /
 * traversal, then bound the length ({@link cappedComponent}) so an arbitrarily
 * long id can never overflow a filesystem path component (ENAMETOOLONG). The
 * length cap hashes the RAW id, so two long ids differing only past the cap still
 * map to distinct files.
 */
export function sanitizeId(id: string): string {
  const safe = id.replace(/[\/\\]/g, '_').replace(/\.\./g, '_');
  return cappedComponent(safe, id, MAX_SESSION_ID_LENGTH);
}

// ---------------------------------------------------------------------------
// Vocabulary mapping (our internal ids -> the dashboard's names)
// ---------------------------------------------------------------------------

/** Our tool ids -> the dashboard's tool vocabulary (PascalCase the reference names). */
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
  /** Cache-creation tokens written at the 5-minute ephemeral TTL. When BOTH this
   *  and {@link cacheCreation1hTokens} are omitted the full aggregate is treated
   *  as a 5m write (the provider surfaced no per-TTL split). */
  cacheCreation5mTokens?: number;
  /** Cache-creation tokens written at the 1-hour ephemeral TTL. */
  cacheCreation1hTokens?: number;
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
 * Cache-creation is split per ephemeral TTL (parity: the reference's emptyUsage
 * `cache_creation.{ephemeral_5m_input_tokens, ephemeral_1h_input_tokens}`): when
 * the provider surfaces an explicit 5m/1h split it is honored; otherwise the full
 * cache-creation total is attributed to the 5m TTL (the default ephemeral window),
 * which is correct for pricing and preserves prior behaviour.
 */
export function buildUsageFields(u: JournalUsageInput): JournalUsage {
  const cacheRead = Math.max(0, u.cachedPromptTokens ?? 0);
  const cacheCreate = Math.max(0, u.cacheCreationTokens ?? 0);
  const input = Math.max(0, u.promptTokens - cacheRead - cacheCreate);
  // Honor an explicit per-TTL split when the provider supplies either arm; with
  // neither present, the whole aggregate is a 5m write (no 1h data surfaced).
  const hasSplit = u.cacheCreation5mTokens !== undefined || u.cacheCreation1hTokens !== undefined;
  const ephemeral5m = hasSplit ? Math.max(0, u.cacheCreation5mTokens ?? 0) : cacheCreate;
  const ephemeral1h = hasSplit ? Math.max(0, u.cacheCreation1hTokens ?? 0) : 0;
  return {
    input_tokens: input,
    output_tokens: Math.max(0, u.completionTokens),
    cache_creation_input_tokens: cacheCreate,
    cache_read_input_tokens: cacheRead,
    cache_creation: { ephemeral_5m_input_tokens: ephemeral5m, ephemeral_1h_input_tokens: ephemeral1h },
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

/**
 * A per-hunk structured diff recorded on a tool_result journal line. This is the
 * SAME shape as a file-mutating tool's {@link DiffHunk} — the journal reuses that
 * ONE canonical type (no parallel definition) so the patch a replay/introspection
 * surface reads is byte-identical to what the tool produced.
 */
export type StructuredPatchHunk = DiffHunk;

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
    /**
     * The structured patch was omitted (left empty) because the edited file
     * exceeded the diff-scan cap — the edit happened but its location could not
     * be rendered. Lets a replay/introspection surface distinguish "edit location
     * not shown — file too large to scan" from a genuine no-op. Parity: the reference's
     * readEditContext `truncated: true` signal.
     */
    truncated?: boolean;
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
