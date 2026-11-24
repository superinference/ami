// Split from diagnostic-tracking.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import { errorMessage } from '../credential-guard';

/** LSP DiagnosticSeverity enum → human-readable name (1=Error…4=Hint). */
export const DIAGNOSTIC_SEVERITY_NAMES = ['Error', 'Warning', 'Info', 'Hint'] as const;

/**
 * Max bytes of a file we will stream into a language server. Matches the reference
 * Code's `MAX_LSP_FILE_SIZE_BYTES` (10MB). A larger file can OOM or wedge the
 * server, so the open/change path skips reading it and the LSP tool reports it.
 */
export const MAX_LSP_FILE_SIZE_BYTES = 10_000_000;

/**
 * Per-file / total caps on untrusted diagnostics injected into the model's
 * context, matching the reference's LSPDiagnosticRegistry constants. A noisy or
 * malicious server cannot flood the conversation past these.
 */
export const MAX_DIAGNOSTICS_PER_FILE = 10;

export const MAX_TOTAL_DIAGNOSTICS = 30;

/**
 * PARITY (additive, resource-exhaustion boundary): max number of distinct files
 * whose baseline the {@link DiagnosticTracker} retains at once. Mirrors the reference
 * Code's `LRUCache({ max: MAX_DELIVERED_FILES })` (LSPDiagnosticRegistry.ts
 * L46-56) — "prevents unbounded memory growth in long sessions". AMI tracks a
 * per-file diagnostic baseline (an array of `Diagnostic` objects) keyed by
 * normalized path; without a bound, a long session that edits thousands of
 * distinct files would retain every baseline forever (the Map is only cleared by
 * an explicit `reset()`), growing memory without limit. The tracker evicts the
 * least-recently-used baseline once this many files are tracked.
 */
export const MAX_TRACKED_BASELINE_FILES = 500;

/** Char budget for a rendered diagnostics block (the reference's 4000) + marker. */
export const MAX_DIAGNOSTICS_SUMMARY_CHARS = 4000;

/**
 * PARITY (additive): per-diagnostic message char cap for UNTRUSTED server text.
 * Mirrors the reference's LSPDiagnosticRegistry message truncation (L296-310): a
 * language server (possibly malicious) can emit an arbitrarily long `message`,
 * which would otherwise flood a single diagnostic line and blow the context
 * budget before the block-level {@link MAX_DIAGNOSTICS_SUMMARY_CHARS} cap even
 * applies. Each server-provided message is truncated to this length at
 * conversion time ({@link truncateDiagnosticMessage}).
 */
export const MAX_DIAGNOSTIC_MESSAGE_CHARS = 1000;

/**
 * Truncate `text` to `max` chars, replacing the tail with `marker` when it
 * overflows. One canonical truncation helper shared by the block renderer
 * ({@link formatDiagnosticsDetail}), the per-message cap
 * ({@link truncateDiagnosticMessage}), and the references-output cap in the LSP
 * tools — so there is a single truncation implementation, never a per-site copy.
 * A `max` at or below the marker length collapses to just the marker.
 */
export function truncateWithMarker(text: string, max: number, marker = '…[truncated]'): string {
  if (text.length <= max) return text;
  if (max <= marker.length) return marker;
  return text.slice(0, max - marker.length) + marker;
}

/**
 * PARITY (additive, defensive): coerce an UNTRUSTED server-provided diagnostic
 * message to a bounded string. The LSP spec says `message` is a string, but we
 * never trust wire data: a malformed/malicious server may send a non-string (or
 * an exotic object whose `String()` coercion throws). This coerces safely inside
 * a try/catch — a coercion failure yields a fixed placeholder rather than
 * aborting the whole {@link convertPublishDiagnostics} batch — then truncates to
 * {@link MAX_DIAGNOSTIC_MESSAGE_CHARS}. Mirrors the reference's defensive
 * serialization + message truncation before diagnostics are logged/surfaced.
 */
export function truncateDiagnosticMessage(message: unknown): string {
  let text: string;
  try {
    text = typeof message === 'string' ? message : String(message ?? '');
  } catch {
    return '<unrenderable diagnostic message>';
  }
  return truncateWithMarker(text, MAX_DIAGNOSTIC_MESSAGE_CHARS);
}

/**
 * True for a UNC path (`\\server\share` or `//server/share`). Touching one with
 * stat()/readFile() on Windows triggers an outbound SMB/NTLM auth, leaking
 * credentials — so the LSP entry points short-circuit before any filesystem
 * access. Mirrors the reference's validateInput UNC guard.
 */
export function isUncPath(p: string): boolean {
  return p.startsWith('\\\\') || p.startsWith('//');
}

/** Size of a file in bytes, or `undefined` when it cannot be stat()ed. */
export function lspFileSizeBytes(filePath: string): number | undefined {
  try {
    return fs.statSync(filePath).size;
  } catch {
    return undefined;
  }
}

/**
 * Distinct outcome of {@link validateLspFilePath}'s failure path. `errorCode`
 * mirrors the reference's LSPTool.validateInput classification (LSPTool.ts
 * L175-208) so a programmatic caller can branch on WHY the path was rejected:
 *   1 = file does not exist (ENOENT)
 *   2 = path exists but is not a regular file (dir, socket, fifo, …)
 *   4 = other stat() failure (permission denied, I/O error, …)
 */
export interface LspFilePathError {
  ok: false;
  message: string;
  errorCode: 1 | 2 | 4;
}

/** Success outcome of {@link validateLspFilePath} — carries the stat'd size. */
export interface LspFilePathOk {
  ok: true;
  sizeBytes: number;
}

/**
 * PARITY (additive): pre-flight existence + regular-file validation for an LSP
 * file operation, mirroring the reference's LSPTool.validateInput (LSPTool.ts
 * L175-208). AMI previously accepted any `file_path` and only distinguished
 * "no server" — a missing file, a directory, or an unreadable path were all
 * reported identically (or silently fell through the size check, which returns
 * `undefined` for a missing file). This classifies the failure precisely:
 *   - ENOENT                → "File does not exist" (errorCode 1)
 *   - stat ok, not a file   → "Path is not a file"  (errorCode 2)
 *   - any other stat error  → "Cannot access file"  (errorCode 4)
 * On success it returns the file size so the caller reuses this single stat()
 * for the {@link MAX_LSP_FILE_SIZE_BYTES} boundary (no redundant second stat).
 *
 * The caller is responsible for the UNC short-circuit ({@link isUncPath}) BEFORE
 * invoking this, matching the reference's ordering where a UNC path skips the stat.
 */
export function validateLspFilePath(filePath: string): LspFilePathOk | LspFilePathError {
  let stats: fs.Stats;
  try {
    stats = fs.statSync(filePath);
  } catch (err) {
    if ((err as NodeJS.ErrnoException)?.code === 'ENOENT') {
      return { ok: false, message: `File does not exist: ${filePath}`, errorCode: 1 };
    }
    const detail = errorMessage(err);
    return { ok: false, message: `Cannot access file: ${filePath}. ${detail}`, errorCode: 4 };
  }
  if (!stats.isFile()) {
    return { ok: false, message: `Path is not a file: ${filePath}`, errorCode: 2 };
  }
  return { ok: true, sizeBytes: stats.size };
}

export type DiagnosticSeverityName = (typeof DIAGNOSTIC_SEVERITY_NAMES)[number];

export interface DiagnosticPosition {
  line: number;
  character: number;
}

export interface DiagnosticRange {
  start: DiagnosticPosition;
  end: DiagnosticPosition;
}

export interface Diagnostic {
  message: string;
  severity: DiagnosticSeverityName;
  range: DiagnosticRange;
  source?: string;
  code?: string;
}

export interface DiagnosticFile {
  uri: string;
  diagnostics: Diagnostic[];
}

/** A raw LSP diagnostic as received over the wire (severity is numeric). */
export interface RawLspDiagnostic {
  message: string;
  severity?: number;
  range: DiagnosticRange;
  source?: string;
  code?: string | number;
}

/** Shape of a `textDocument/publishDiagnostics` notification payload. */
export interface PublishDiagnosticsParams {
  uri: string;
  diagnostics: RawLspDiagnostic[];
}

/**
 * Map the LSP numeric DiagnosticSeverity to a name. LSP enum:
 * 1 = Error, 2 = Warning, 3 = Information, 4 = Hint. Anything else (including
 * a missing severity, which the spec leaves to client discretion) is treated
 * as Error so it is never silently dropped.
 */
export function mapLspSeverity(severity: number | undefined): DiagnosticSeverityName {
  switch (severity) {
    case 1: return 'Error';
    case 2: return 'Warning';
    case 3: return 'Info';
    case 4: return 'Hint';
    default: return 'Error';
  }
}

/**
 * Convert a `textDocument/publishDiagnostics` payload into normalized
 * Diagnostics. `code` is coerced to a string (null/undefined → undefined),
 * matching the reference's `String(code)` coercion.
 *
 * FUNCTIONAL (robustness): a malformed/malicious server may publish a diagnostic
 * with a missing or partial `range` (the LSP spec requires it, but we never trust
 * untrusted wire data). Reading `d.range.start.line` on such a payload threw and
 * aborted the WHOLE conversion, dropping every other diagnostic for the file.
 * Missing range members now default to a zero position so one bad diagnostic can
 * no longer poison the batch.
 */
export function convertPublishDiagnostics(params: PublishDiagnosticsParams): Diagnostic[] {
  return (params.diagnostics ?? []).map((d) => ({
    // Untrusted server text: coerce defensively + truncate so one giant (or
    // non-string) message cannot flood context or abort the batch.
    message: truncateDiagnosticMessage(d.message),
    severity: mapLspSeverity(d.severity),
    range: {
      start: { line: d.range?.start?.line ?? 0, character: d.range?.start?.character ?? 0 },
      end: { line: d.range?.end?.line ?? 0, character: d.range?.end?.character ?? 0 },
    },
    source: d.source,
    code: d.code !== undefined && d.code !== null ? String(d.code) : undefined,
  }));
}

/**
 * PARITY (additive): the reference's virtual read-only filesystem scheme
 * (`_claude_fs_right:`), used when a diagnostic originates from its in-memory fs
 * rather than an on-disk `file://` URI (DiagnosticsDisplay.tsx L87). AMI emits
 * `file://` URIs, but a diagnostic SOURCE can be any editor/tool, so the header
 * formatter strips this scheme (and labels it) for cross-source parity.
 */
export const CLAUDE_VIRTUAL_FS_SCHEME = '_claude_fs_right:';

/**
 * PARITY (additive): the scheme-label suffix for a diagnostic file header,
 * mirroring the reference's 3-way branch (DiagnosticsDisplay.tsx L87):
 *   - `file://…`            → `file://`
 *   - `_claude_fs_right:…`  → `claude_fs_right`
 *   - any other scheme      → the generic scheme name (`uri.split(':')[0]`)
 * A bare path (no scheme), a single-letter Windows drive (`C:/…`), or an
 * empty/undefined URI yields the empty string so no `(…)` suffix is rendered.
 * The generic branch requires a real RFC-3986 scheme of length ≥ 2 so a drive
 * letter is never mistaken for a scheme.
 */
export function diagnosticSchemeLabel(uri: string | undefined): string {
  if (!uri) return '';
  if (uri.startsWith('file://')) return 'file://';
  if (uri.startsWith(CLAUDE_VIRTUAL_FS_SCHEME)) return 'claude_fs_right';
  const m = /^([A-Za-z][A-Za-z0-9+.-]+):/.exec(uri);
  return m ? m[1] : '';
}

/**
 * Strip the known/virtual scheme prefixes before path handling: the reference
 * virtual-fs scheme ({@link CLAUDE_VIRTUAL_FS_SCHEME}) is removed here, then
 * {@link uriToPath} handles the `file://` scheme (+ percent-decode + Windows
 * drive). A generic-scheme URI is left intact (the reference only `.replace`s the two
 * known schemes). Returns `null` for a missing/empty URI so callers can skip
 * filesystem work (symbol resolution) for an unknown location.
 */
export function diagnosticFsPath(uri: string | undefined): string | null {
  if (!uri) return null;
  const stripped = uri.startsWith(CLAUDE_VIRTUAL_FS_SCHEME)
    ? uri.slice(CLAUDE_VIRTUAL_FS_SCHEME.length)
    : uri;
  return uriToPath(stripped);
}

/** Strip a `file://` URI to a filesystem path (percent-decoded, platform-aware). */
export function uriToPath(uri: string): string {
  let p = uri;
  if (p.startsWith('file://')) {
    p = p.slice('file://'.length);
    // On Windows a file URI is file:///C:/… → /C:/… ; drop the leading slash.
    if (/^\/[A-Za-z]:/.test(p)) p = p.slice(1);
  }
  try { p = decodeURIComponent(p); } catch { /* malformed escapes → keep raw */ }
  return p;
}
