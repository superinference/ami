import * as fs from 'fs';
import * as path from 'path';
import { resolveCacheDir } from '../reference-journal';
import { redactCredentials } from '../credential-guard';

/**
 * Per-MCP-server structured log sink (the reference-parity `getMCPLogsPath` +
 * `logMCPError`/`logMCPDebug`, utils/errorLogSink.ts).
 *
 * AMI previously funnelled EVERY MCP server's activity through the single shared
 * `coreLog('mcp', …)` engine log, so one noisy/erroring server was indistinguishable
 * from another and there was no per-server audit trail to hand a user debugging a
 * specific flaky server. This sink mirrors the reference: each server gets its own
 * dated JSONL file under `<configDir>/mcp-logs-<server>/<YYYY-MM-DD>.jsonl`, with
 * distinct `{error,…}` and `{debug,…}` record shapes, so a `/mcp` diagnostics view
 * (or a human) can read exactly one server's errors in isolation.
 *
 * Every value that can carry a secret (error text, debug payloads) is routed through
 * {@link redactCredentials} before it touches disk, and the directory/file are created
 * owner-only (0700/0600) — the same data-minimization posture as the core logger.
 */

/** Directory mode for the per-server log dir (owner-only — records may carry secrets). */
const DIR_MODE = 0o700;
/** File mode for the per-server log file (owner-only). */
const FILE_MODE = 0o600;

/**
 * Sanitize a server name into a single filesystem-safe path segment (the reference-parity
 * `sanitizePath` on the `mcp-logs-<server>` dir): every character outside
 * `[A-Za-z0-9._-]` becomes `_` so a server name with slashes, colons (reserved as a
 * Windows drive separator), or `..` can never escape the log directory or collide
 * with a path separator. An empty/all-stripped name collapses to `_`.
 */
export function sanitizeMcpLogSegment(serverName: string): string {
  const cleaned = serverName.replace(/[^A-Za-z0-9._-]/g, '_').replace(/^\.+$/, '_');
  return cleaned.length > 0 ? cleaned : '_';
}

/** UTC `YYYY-MM-DD` date stamp for the log file name (injectable clock for tests). */
export function mcpLogDateStamp(now: Date = new Date()): string {
  return now.toISOString().slice(0, 10);
}

/** Options shared by the log-sink helpers (all seams are injectable for tests). */
export interface McpLogSinkOptions {
  /** Clock override (defaults to `new Date()`), for a deterministic date/timestamp. */
  now?: Date;
  /** Config-dir override (defaults to {@link resolveConfigDir}). */
  configDir?: string;
  /** Working directory recorded in each entry (defaults to `process.cwd()`). */
  cwd?: string;
  /** Session id recorded in each entry when the integrator supplies one. */
  sessionId?: string;
}

/**
 * The per-server dated JSONL log path (the reference-parity `getMCPLogsPath(serverName)`):
 * `<cacheDir>/mcp-logs-<sanitized-server>/<YYYY-MM-DD>.jsonl`. The server name is
 * sanitized to one safe segment so it can never traverse outside the base dir.
 *
 * These are TRANSIENT, discardable diagnostics, so they default to the cache dir
 * ({@link resolveCacheDir}) — honoring `XDG_CACHE_HOME` as a knob distinct from the
 * durable data dir (parity: the reference's `cachePaths` cache base). With no XDG cache
 * env set the cache dir IS the data dir, so the default location is unchanged; an
 * explicit `configDir` override always wins.
 */
export function getMcpServerLogPath(serverName: string, opts?: McpLogSinkOptions): string {
  const base = opts?.configDir ?? resolveCacheDir();
  return path.join(base, `mcp-logs-${sanitizeMcpLogSegment(serverName)}`, `${mcpLogDateStamp(opts?.now)}.jsonl`);
}

/** Common fields every per-server log record carries. */
interface McpLogRecordBase {
  timestamp: string;
  cwd: string;
  sessionId?: string;
}
/** A per-server MCP error record (the reference-parity `logMCPError` shape). */
export interface McpErrorLogRecord extends McpLogRecordBase {
  error: string;
}
/** A per-server MCP debug record (the reference-parity `logMCPDebug` shape). */
export interface McpDebugLogRecord extends McpLogRecordBase {
  debug: string;
}

/** Build the common base fields for a record from the (optional) seams. */
function baseRecord(opts?: McpLogSinkOptions): McpLogRecordBase {
  const rec: McpLogRecordBase = {
    timestamp: (opts?.now ?? new Date()).toISOString(),
    cwd: opts?.cwd ?? process.cwd(),
  };
  if (opts?.sessionId) rec.sessionId = opts.sessionId;
  return rec;
}

/** Append one JSONL record to a server's dated log file. Best-effort: a write
 *  failure (unwritable dir, full disk) is swallowed so logging never breaks a
 *  connection or crashes the manager. */
function appendRecord(serverName: string, record: object, opts?: McpLogSinkOptions): void {
  const file = getMcpServerLogPath(serverName, opts);
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: DIR_MODE });
    fs.appendFileSync(file, JSON.stringify(record) + '\n', { mode: FILE_MODE });
  } catch {
    // Never throw from a log sink.
  }
}

/**
 * Append a per-server MCP ERROR record (the reference-parity `logMCPError`). An `Error`
 * contributes its stack (falling back to its message); any other value is
 * stringified. The text is credential-redacted before it is written.
 */
export function logMcpError(serverName: string, error: unknown, opts?: McpLogSinkOptions): void {
  const raw = error instanceof Error ? (error.stack || error.message) : String(error);
  const record: McpErrorLogRecord = { ...baseRecord(opts), error: redactCredentials(raw) };
  appendRecord(serverName, record, opts);
}

/**
 * Append a per-server MCP DEBUG record (the reference-parity `logMCPDebug`). The message is
 * credential-redacted before it is written.
 */
export function logMcpDebug(serverName: string, message: string, opts?: McpLogSinkOptions): void {
  const record: McpDebugLogRecord = { ...baseRecord(opts), debug: redactCredentials(message) };
  appendRecord(serverName, record, opts);
}
