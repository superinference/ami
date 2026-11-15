/**
 * LSP diagnostic tracking — baseline capture + new-vs-current diff.
 *
 * Mirrors Claude Code's DiagnosticTrackingService (references
 * services/diagnosticTracking.ts): before an edit we snapshot a file's
 * diagnostics into a per-file baseline; after the edit we fetch the current
 * diagnostics and surface ONLY the ones that are not already in the baseline,
 * then roll the baseline forward. This suppresses pre-existing noise so the
 * model sees exactly the problems ITS edit introduced.
 *
 * This module is transport-agnostic: it reads diagnostics through a
 * `DiagnosticSource` (the LSPClient provides one), so it is unit-testable
 * without a real language server.
 */
import * as path from 'path';

/** LSP DiagnosticSeverity enum → human-readable name (1=Error…4=Hint). */
export const DIAGNOSTIC_SEVERITY_NAMES = ['Error', 'Warning', 'Info', 'Hint'] as const;
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
interface RawLspDiagnostic {
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
 * matching Claude's `String(code)` coercion.
 */
export function convertPublishDiagnostics(params: PublishDiagnosticsParams): Diagnostic[] {
  return (params.diagnostics ?? []).map((d) => ({
    message: d.message,
    severity: mapLspSeverity(d.severity),
    range: {
      start: { line: d.range.start.line, character: d.range.start.character },
      end: { line: d.range.end.line, character: d.range.end.character },
    },
    source: d.source,
    code: d.code !== undefined && d.code !== null ? String(d.code) : undefined,
  }));
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

/**
 * Platform-aware normalization for comparing paths as map keys. Resolves the
 * path and (on case-insensitive filesystems) lowercases it so a `file://` URI
 * and the requested path compare equal.
 */
export function normalizeDiagnosticPath(uriOrPath: string): string {
  const resolved = path.resolve(uriToPath(uriOrPath));
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

/** True when two paths (URIs or filesystem paths) refer to the same file. */
export function diagnosticPathsEqual(a: string, b: string): boolean {
  return normalizeDiagnosticPath(a) === normalizeDiagnosticPath(b);
}

/** Structural equality for two diagnostics (message/severity/source/code/range). */
export function areDiagnosticsEqual(a: Diagnostic, b: Diagnostic): boolean {
  return (
    a.message === b.message &&
    a.severity === b.severity &&
    a.source === b.source &&
    a.code === b.code &&
    a.range.start.line === b.range.start.line &&
    a.range.start.character === b.range.start.character &&
    a.range.end.line === b.range.end.line &&
    a.range.end.character === b.range.end.character
  );
}

/**
 * Source of truth for a file's current diagnostics. The LSPClient implements
 * this by returning whatever `textDocument/publishDiagnostics` last reported
 * for the file (after a bounded settle). Returns `null` when no diagnostics
 * could be obtained (no server, unsupported language, timeout).
 */
export interface DiagnosticSource {
  getDiagnostics(filePath: string): Promise<DiagnosticFile | null>;
}

/**
 * Baseline/diff tracker. One instance per client; stores a baseline keyed by
 * normalized path so lookups survive `file://` vs path and case differences.
 */
export class DiagnosticTracker {
  private baseline: Map<string, Diagnostic[]> = new Map();

  constructor(private readonly source: DiagnosticSource) {}

  /**
   * Snapshot a file's diagnostics into the baseline before it is edited.
   *  - path-mismatch guard: if the returned file is a DIFFERENT file than the
   *    one requested, bail rather than poison the baseline.
   *  - empty-baseline fallback: if no diagnostics come back, store an empty
   *    baseline so later edits still register new issues as "new".
   */
  async beforeFileEdited(filePath: string): Promise<void> {
    const key = normalizeDiagnosticPath(filePath);
    let file: DiagnosticFile | null;
    try {
      file = await this.source.getDiagnostics(filePath);
    } catch {
      // Server does not support diagnostics — fail silently, no baseline.
      return;
    }
    if (file) {
      if (!diagnosticPathsEqual(filePath, file.uri)) {
        // Wrong file returned — do not poison the baseline.
        return;
      }
      this.baseline.set(key, file.diagnostics);
    } else {
      this.baseline.set(key, []);
    }
  }

  /** True when a baseline has been captured for `filePath`. */
  hasBaseline(filePath: string): boolean {
    return this.baseline.has(normalizeDiagnosticPath(filePath));
  }

  /**
   * Fetch current diagnostics for the files that have a captured baseline and
   * return ONLY the diagnostics not present in each baseline. Rolls the
   * baseline forward to the current set so a subsequent call reports the next
   * delta. Only files with a baseline are considered (never-edited files are
   * ignored, matching Claude).
   */
  async getNewDiagnostics(filePaths?: string[]): Promise<DiagnosticFile[]> {
    const keys = filePaths
      ? filePaths.map(normalizeDiagnosticPath).filter((k) => this.baseline.has(k))
      : [...this.baseline.keys()];

    const out: DiagnosticFile[] = [];
    for (const key of keys) {
      let file: DiagnosticFile | null;
      try {
        file = await this.source.getDiagnostics(key);
      } catch {
        continue;
      }
      const current = file ? file.diagnostics : [];
      const base = this.baseline.get(key) ?? [];
      const fresh = current.filter((d) => !base.some((b) => areDiagnosticsEqual(d, b)));
      if (fresh.length > 0) {
        out.push({ uri: file ? file.uri : `file://${key}`, diagnostics: fresh });
      }
      // Roll the baseline forward.
      this.baseline.set(key, current);
    }
    return out;
  }

  /** Clear all tracked baselines (e.g. at the start of a new query loop). */
  reset(): void {
    this.baseline.clear();
  }
}

const SEVERITY_SYMBOL: Record<DiagnosticSeverityName, string> = {
  Error: '✖',
  Warning: '⚠',
  Info: 'ℹ',
  Hint: '☆',
};

/** Severity glyph for a diagnostic line. */
export function diagnosticSeveritySymbol(severity: DiagnosticSeverityName): string {
  return SEVERITY_SYMBOL[severity] ?? '•';
}

/**
 * One-line summary of the post-edit delta, matching Claude's wording exactly:
 * "Found N new diagnostic issue(s) in M file(s)" with correct singular/plural.
 * Returns the empty string when there are no new diagnostics.
 */
export function formatNewDiagnosticsSummary(files: DiagnosticFile[]): string {
  const totalIssues = files.reduce((sum, f) => sum + f.diagnostics.length, 0);
  if (totalIssues === 0) return '';
  const fileCount = files.length;
  const issueWord = totalIssues === 1 ? 'issue' : 'issues';
  const fileWord = fileCount === 1 ? 'file' : 'files';
  return `Found ${totalIssues} new diagnostic ${issueWord} in ${fileCount} ${fileWord}`;
}

/** Detailed, severity-annotated rendering of diagnostic files for tool output. */
export function formatDiagnosticsDetail(files: DiagnosticFile[]): string {
  return files
    .map((file) => {
      const name = uriToPath(file.uri);
      const lines = file.diagnostics
        .map((d) => {
          const sym = diagnosticSeveritySymbol(d.severity);
          const loc = `[Line ${d.range.start.line + 1}:${d.range.start.character + 1}]`;
          const code = d.code ? ` [${d.code}]` : '';
          const src = d.source ? ` (${d.source})` : '';
          return `  ${sym} ${loc} ${d.message}${code}${src}`;
        })
        .join('\n');
      return `${name}:\n${lines}`;
    })
    .join('\n\n');
}
