// Split from logger.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { redactCredentials } from './credential-guard';
import { isEnvTruthy } from './privacy-level';
import { isSafeSegment } from './skill-catalog';
import { AMI_VERSION } from './version';

// Restrictive modes: the log records raw tool inputs/outputs and error text that
// can contain secrets and file contents. On a shared host the predictable /tmp
// path would otherwise be world-readable, so the directory is created 0700 and
// the file 0600 (owner-only) — a data-minimization/least-exposure defense.
export const DIR_MODE = 0o700;

export const FILE_MODE = 0o600;

// ---------------------------------------------------------------------------
// Configurable debug-payload formatter. ONE tunable limit governs how much of a
// debug body (tool input/output, API payload, etc.) is persisted; both helpers
// collapse newlines into `\n` and append a `... (N chars)` suffix that reveals
// the ORIGINAL length so a truncated record is never mistaken for the whole
// payload. Mirrors the reference's DEBUG_MSG_LIMIT / debugTruncate / debugBody
// (bridge/debugUtils.ts) — a single knob instead of scattered magic slices.
// ---------------------------------------------------------------------------
export const DEFAULT_DEBUG_MSG_LIMIT = 2000;

/** Resolve the debug-body length cap (`AMI_DEBUG_MSG_LIMIT`, default 2000). */
export function getDebugMsgLimit(): number {
  const raw = process.env.AMI_DEBUG_MSG_LIMIT;
  if (raw !== undefined) {
    const parsed = Number(raw);
    if (Number.isInteger(parsed) && parsed >= 0) return parsed;
  }
  return DEFAULT_DEBUG_MSG_LIMIT;
}

/**
 * Collapse newlines and truncate a string for debug logging, appending
 * `... (N chars)` with the original length when it overflows the limit. Mirrors
 * the reference's `debugTruncate`.
 */
export function debugTruncate(s: string): string {
  const flat = s.replace(/\r\n|\r|\n/g, '\\n');
  const limit = getDebugMsgLimit();
  if (flat.length <= limit) return flat;
  return flat.slice(0, limit) + `... (${flat.length} chars)`;
}

/**
 * Serialize a JSON-serializable value, REDACT credentials, THEN truncate to the
 * limit (redact-before-truncate so a secret split across the cut boundary is
 * still removed). Circular/unstringifiable values fall back to `String(data)`.
 * Mirrors the reference's `debugBody`.
 */
export function debugBody(data: unknown): string {
  let raw: string;
  if (typeof data === 'string') {
    raw = data;
  } else {
    try {
      raw = JSON.stringify(data) ?? String(data);
    } catch {
      raw = String(data);
    }
  }
  const s = redactCredentials(raw);
  const limit = getDebugMsgLimit();
  if (s.length <= limit) return s;
  return s.slice(0, limit) + `... (${s.length} chars)`;
}

// ---------------------------------------------------------------------------
// Severity levels (ordered) — a verbosity knob, not a new log channel.
// ---------------------------------------------------------------------------
/**
 * Ordered log severities. Records tagged below the active minimum level are
 * dropped before they touch disk, letting a user raise or lower verbosity at
 * runtime without a rebuild. Mirrors the reference's DebugLogLevel.
 */
export const LOG_LEVELS = ['verbose', 'debug', 'info', 'warn', 'error'] as const;

export type LogLevel = (typeof LOG_LEVELS)[number];

export const LEVEL_ORDER: Record<LogLevel, number> = {
  verbose: 0,
  debug: 1,
  info: 2,
  warn: 3,
  error: 4,
};

export const DEFAULT_MIN_LEVEL: LogLevel = 'debug';

/**
 * Minimum severity to persist. Reads `AMI_DEBUG_LOG_LEVEL` first, then the
 * Claude-compatible `CLAUDE_CODE_DEBUG_LOG_LEVEL`, and falls back safely to
 * `'debug'` on an unset or invalid value (so `verbose` high-volume diagnostics
 * are filtered out by default). Read per call so a value set mid-session (and in
 * tests) takes effect immediately. Mirrors the reference's getMinDebugLogLevel.
 */
export function getMinLogLevel(): LogLevel {
  const raw = (process.env.AMI_DEBUG_LOG_LEVEL ?? process.env.CLAUDE_CODE_DEBUG_LOG_LEVEL)
    ?.toLowerCase()
    .trim();
  if (raw && Object.prototype.hasOwnProperty.call(LEVEL_ORDER, raw)) {
    return raw as LogLevel;
  }
  return DEFAULT_MIN_LEVEL;
}

// ---------------------------------------------------------------------------
// Debug component/category filter (AMI's analogue of the reference's
// `--debug=<pattern>` / debugFilter.ts). The `AMI_DEBUG_COMPONENTS` spec is a
// comma list that selects which log lines reach disk by their component tag and
// any category heuristically extracted from the message:
//   - `api,hooks`   → inclusive: show ONLY the `api` and `hooks` categories
//   - `!1p,!file`   → exclusive: show everything EXCEPT `1p` and `file`
//   - `*` / empty / undefined / whitespace → no filtering (show all; default)
//   - a mixed spec (`api,!file`) is invalid and falls back to show-all
// This mirrors the reference's parseDebugFilter/extractDebugCategories/
// shouldShowDebugCategories so a user can narrow noisy debug output by subsystem.
// ---------------------------------------------------------------------------

/** Parsed debug-filter configuration. Mirrors the reference's `DebugFilter`. */
export interface DebugFilter {
  include: string[];
  exclude: string[];
  isExclusive: boolean;
}

/**
 * Extract lowercase categories from a free-form message, so a filter can match
 * on content as well as the explicit component tag. Supports `category: msg`,
 * `[CATEGORY] msg`, `MCP server "name": msg`, and a `1p event:` marker. Heuristic
 * matches are bounded (secondary category must be <30 chars and contain no space)
 * to keep pathological input from being treated as a category, and the result is
 * de-duped. Mirrors the reference's `extractDebugCategories`.
 */
export function extractDebugCategories(message: string): string[] {
  const categories: string[] = [];

  // MCP pattern first, to avoid the generic prefix matcher stealing it.
  const mcpMatch = message.match(/^MCP server ["']([^"']+)["']/);
  if (mcpMatch && mcpMatch[1]) {
    categories.push('mcp');
    categories.push(mcpMatch[1].toLowerCase());
  } else {
    const prefixMatch = message.match(/^([^:[]+):/);
    if (prefixMatch && prefixMatch[1]) {
      categories.push(prefixMatch[1].trim().toLowerCase());
    }
  }

  const bracketMatch = message.match(/^\[([^\]]+)]/);
  if (bracketMatch && bracketMatch[1]) {
    categories.push(bracketMatch[1].trim().toLowerCase());
  }

  if (message.toLowerCase().includes('1p event:')) {
    categories.push('1p');
  }

  const secondaryMatch = message.match(/:\s*([^:]+):/);
  if (secondaryMatch && secondaryMatch[1]) {
    // Strip a trailing qualifier word so "Installation type: dev" → "installation".
    // (Done in code rather than in the regex to keep the pattern ReDoS-safe —
    // star height 1, no quantifier nested under the optional group.)
    const secondary = secondaryMatch[1].trim().toLowerCase().replace(/\s+(?:type|mode|status|event)$/, '');
    // Only accept a reasonable category name — reject absurdly long or
    // whitespace-laden fragments.
    if (secondary.length < 30 && !secondary.includes(' ')) {
      categories.push(secondary);
    }
  }

  return Array.from(new Set(categories));
}

/**
 * Decide whether a line with the given categories passes the filter. Fail-closed:
 * when a filter is active but no categories were found, the line is dropped in
 * BOTH modes (uncategorized output is excluded by default when a user has scoped
 * debugging). Mirrors the reference's `shouldShowDebugCategories`.
 */
export function shouldShowDebugCategories(categories: string[], filter: DebugFilter | null): boolean {
  if (!filter) return true;
  if (categories.length === 0) return false;
  if (filter.isExclusive) {
    return !categories.some((cat) => filter.exclude.includes(cat));
  }
  return categories.some((cat) => filter.include.includes(cat));
}

/**
 * Build the category set for a log line from its explicit component tag plus any
 * categories heuristically extracted from the message, de-duped. The explicit
 * component leads so an empty/blank component yields a purely message-derived set
 * (and an empty set when nothing is extractable — which the fail-closed boundary
 * then drops under an active filter).
 */
export function categoriesForLine(component: string, msg: string): string[] {
  const cats = extractDebugCategories(msg);
  const c = component.trim().toLowerCase();
  if (c) cats.unshift(c);
  return Array.from(new Set(cats));
}

/**
 * Resolve the active log FILE NAME. When `AMI_DEBUG_SESSION_ID` is set to a
 * path-safe segment, each session writes to its own `<sessionId>.log` so
 * concurrent sessions never interleave and each run is independently readable;
 * otherwise the shared `engine.log`. The id is validated with the same
 * path-safety guard the skill catalog uses, so a malicious value cannot escape
 * the log directory. Mirrors the reference's per-session `<sessionId>.txt`.
 */
export function logFileName(): string {
  const sid = process.env.AMI_DEBUG_SESSION_ID?.trim();
  if (sid && isSafeSegment(sid)) return `${sid}.log`;
  return 'engine.log';
}

/** Whether a per-session log file (not the shared `engine.log`) is active. */
export function hasPerSessionFile(): boolean {
  const sid = process.env.AMI_DEBUG_SESSION_ID?.trim();
  return !!sid && isSafeSegment(sid) && !process.env.AMI_DEBUG_FILE?.trim();
}

// ---------------------------------------------------------------------------
// In-memory recent-errors ring (bug-report / diagnostics introspection).
// ---------------------------------------------------------------------------
export const MAX_IN_MEMORY_ERRORS = 100;

/**
 * Standard enrichment fields stamped onto every persisted error entry (the dated
 * JSONL error sink below) AND merged into the structured `data` of each
 * `logError()` debug line. Mirrors the reference's errorLogSink `appendToLog`,
 * which tags every record with `cwd` + `sessionId` + `version` so a later
 * bug-report/triage flow can attribute an error to the exact run, working
 * directory, and build that produced it. `sessionId` reuses `AMI_DEBUG_SESSION_ID`
 * (the same id that names the per-session log file, path-safety validated);
 * `version` is the single-source {@link AMI_VERSION}; `cwd` is the live process
 * working directory.
 */
export function standardLogFields(): { sessionId: string; cwd: string; version: string } {
  const sid = process.env.AMI_DEBUG_SESSION_ID?.trim();
  return {
    sessionId: sid && isSafeSegment(sid) ? sid : 'unknown',
    cwd: process.cwd(),
    version: AMI_VERSION,
  };
}

/**
 * Escape embedded CR/LF so a single logical record cannot be split into multiple
 * physical lines (log-injection / line-forging defense). Only the free-form
 * `component` and `msg` need this; structured `data` is already newline-escaped
 * by JSON.stringify.
 */
export function escapeLogField(s: string): string {
  return s.replace(/\r\n|\r|\n/g, '\\n');
}

// ---------------------------------------------------------------------------
// Deferred write queue (buffer-before-ready + replay-on-attach).
//
// A line whose at-rest write fails — the log dir is not yet created during the
// startup boundary window, or is transiently unwritable — is buffered here
// (bounded, oldest evicted) and replayed IN ORDER the instant a later write
// succeeds, so no line logged before the sink is ready is silently dropped.
// The on-disk file is AMI's "sink"; {@link flushPendingLogWrites} is the
// idempotent drain. Mirrors the reference's errorQueue + drain-on-attach.
// ---------------------------------------------------------------------------
export const MAX_PENDING_WRITES = 100;

// ---------------------------------------------------------------------------
// Output routing + at-rest persistence gate.
// ---------------------------------------------------------------------------
/**
 * Whether debug output is routed to stderr instead of the on-disk file. Honors
 * the `AMI_DEBUG_TO_STDERR` env flag and the Claude-compatible
 * `--debug-to-stderr` / `-d2e` argv flags. When active, `log()` writes the
 * formatted line to stderr and bypasses the file writer, so a user can stream
 * debug output live without a log file. Mirrors the reference's `isDebugToStdErr`.
 */
export function isDebugToStderr(): boolean {
  return (
    isEnvTruthy(process.env.AMI_DEBUG_TO_STDERR) ||
    process.argv.includes('--debug-to-stderr') ||
    process.argv.includes('-d2e')
  );
}

/**
 * Runtime-presence seam (ESM-safe). Returns the active Node version, or
 * `undefined` in a non-node/degraded runtime. Behavior-preserving indirection so
 * {@link shouldPersistToDisk}'s non-node boundary guard is exercisable in a test
 * (via `mock.method(runtimeHooks, 'nodeVersion', () => undefined)`) without
 * deleting the real `process` global. Mirrors the reference's `typeof process`
 * boundary guard in shouldLogDebugMessage.
 */
export const runtimeHooks = {
  nodeVersion: (): string | undefined => process.versions.node,
};

// ---------------------------------------------------------------------------
// Dated structured JSONL error sink (opt-in, PII-redacted) — a machine-readable
// per-run error log SEPARATE from the human debug log. Mirrors the reference's
// errorLogSink: one date-stamped `.jsonl` file whose records carry the standard
// enrichment fields, so errors from a run can be parsed/triaged after the fact.
// ---------------------------------------------------------------------------
/**
 * Convert a Date to a filesystem-safe, lexically-sortable stamp by replacing the
 * `:` and `.` that some filesystems forbid with `-` (e.g.
 * `2026-11-17T08-30-00-000Z`). Mirrors the reference's `dateToFilename` so dated
 * log files sort by time and never collide on a colon.
 */
export function dateToFilename(date: Date): string {
  return date.toISOString().replace(/[:.]/g, '-');
}

/**
 * Resolve the structured error-sink directory from `AMI_ERRORS_DIR` (the reference-
 * compatible `CLAUDE_CODE_ERRORS_DIR` fallback), or undefined when neither is
 * set — so the dated JSONL error file is strictly opt-in and zero-cost by
 * default.
 */
export function getErrorsDir(): string | undefined {
  const dir = (process.env.AMI_ERRORS_DIR ?? process.env.CLAUDE_CODE_ERRORS_DIR)?.trim();
  return dir || undefined;
}

// ---------------------------------------------------------------------------
// HTTP / network error enrichment (url + status + server body message).
// ---------------------------------------------------------------------------
/**
 * Extract a human-readable server message from an error's response payload.
 * Accepts a raw string body, `{ message }`, or `{ error: { message } }`.
 * Mirrors the reference's `extractServerMessage`.
 */
export function extractServerMessage(data: unknown): string | undefined {
  if (typeof data === 'string') {
    // Provider SDKs (AI SDK `APICallError.responseBody`) hand back the raw HTTP
    // body as a JSON STRING, not a parsed object. Parse it opportunistically so
    // the nested `{ error: { message } }` / `{ message }` is surfaced instead of
    // the whole JSON blob; a non-JSON body (plain text) falls through unchanged.
    const trimmed = data.trim();
    if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
      try {
        const parsed: unknown = JSON.parse(trimmed);
        if (parsed && typeof parsed === 'object') {
          const nested = extractServerMessage(parsed);
          if (nested) return nested;
        }
      } catch {
        // Not JSON — return the raw string body below.
      }
    }
    return data;
  }
  if (data && typeof data === 'object') {
    const obj = data as Record<string, unknown>;
    if (typeof obj.message === 'string') return obj.message;
    const inner = obj.error;
    if (inner && typeof inner === 'object' && typeof (inner as Record<string, unknown>).message === 'string') {
      return (inner as Record<string, unknown>).message as string;
    }
  }
  return undefined;
}

/**
 * Build a bracketed `[url=…,code=…,status=…,body=…] ` prefix from an HTTP/network
 * error object for inclusion in the persisted error log. Duck-types three shapes
 * so no HTTP client dependency is required:
 *   - axios-style (`error.config.url`, `error.response.status`, `error.response.data`),
 *   - fetch-style (`error.url`, `error.status`, `error.body`), and
 *   - AI SDK `APICallError` (`error.url`, `error.statusCode`, `error.responseBody`) —
 *     the shape every provider API failure actually arrives in on AMI's streaming
 *     path, so a logged provider error now names the endpoint and the server's own
 *     message instead of a bare `HTTP <status>`.
 *
 * It additionally surfaces the network error CODE (`error.code`, e.g.
 * `ECONNREFUSED` / `ETIMEDOUT`) so a pure connection failure that carries no url
 * or status is still attributable — the reference's `api.ts` logs exactly
 * `err.code ?? 'unknown'` on a network error. Returns `''` only when NOTHING safe
 * to log is found.
 *
 * LOG HYGIENE (security contract): this is the single sanctioned wrapper for
 * turning an HTTP/network error into log text. It reads ONLY the response/network
 * metadata named above and **deliberately NEVER reads the REQUEST side** —
 * `error.config.data` (the POST body, which for auth/token exchanges holds the raw
 * credential) and `error.config.headers` (the `Authorization` header) are never
 * touched, so a secret in the outgoing request can never reach the log through
 * this path. Mirrors the explicit "do not log err.config.data" guard in the reference
 * Code's `api.ts` and the axios-error enrichment in its `errorLogSink`.
 */
export function httpErrorContext(error: unknown): string {
  if (!error || typeof error !== 'object') return '';
  const err = error as Record<string, unknown>;
  const config = err.config as Record<string, unknown> | undefined;
  const response = err.response as Record<string, unknown> | undefined;
  const code = typeof err.code === 'string' && err.code !== '' ? err.code : undefined;
  const url = (config?.url ?? err.url) as unknown;
  if (typeof url !== 'string' || url === '') {
    // No request URL. Still surface a bare network error CODE so a connection
    // failure (ECONNREFUSED/ETIMEDOUT) remains attributable; the request body and
    // headers are never read, mirroring the reference's "log err.code alone" guard.
    return code ? `[code=${code}] ` : '';
  }

  const parts = [`url=${url}`];
  if (code) parts.push(`code=${code}`);
  const status = (response?.status ?? err.status ?? err.statusCode) as unknown;
  if (typeof status === 'number') parts.push(`status=${status}`);
  const body = extractServerMessage(response?.data ?? err.body ?? err.data ?? err.responseBody);
  if (body) parts.push(`body=${body}`);
  return `[${parts.join(',')}] `;
}

export const DEFAULT_TAIL_LINES = 20;
