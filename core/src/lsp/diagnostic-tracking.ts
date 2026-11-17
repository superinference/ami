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
import * as fs from 'fs';
import * as path from 'path';

/** LSP DiagnosticSeverity enum → human-readable name (1=Error…4=Hint). */
export const DIAGNOSTIC_SEVERITY_NAMES = ['Error', 'Warning', 'Info', 'Hint'] as const;

/**
 * Max bytes of a file we will stream into a language server. Matches Claude
 * Code's `MAX_LSP_FILE_SIZE_BYTES` (10MB). A larger file can OOM or wedge the
 * server, so the open/change path skips reading it and the LSP tool reports it.
 */
export const MAX_LSP_FILE_SIZE_BYTES = 10_000_000;

/**
 * Per-file / total caps on untrusted diagnostics injected into the model's
 * context, matching Claude's LSPDiagnosticRegistry constants. A noisy or
 * malicious server cannot flood the conversation past these.
 */
export const MAX_DIAGNOSTICS_PER_FILE = 10;
export const MAX_TOTAL_DIAGNOSTICS = 30;

/**
 * PARITY (additive, resource-exhaustion boundary): max number of distinct files
 * whose baseline the {@link DiagnosticTracker} retains at once. Mirrors Claude
 * Code's `LRUCache({ max: MAX_DELIVERED_FILES })` (LSPDiagnosticRegistry.ts
 * L46-56) — "prevents unbounded memory growth in long sessions". AMI tracks a
 * per-file diagnostic baseline (an array of `Diagnostic` objects) keyed by
 * normalized path; without a bound, a long session that edits thousands of
 * distinct files would retain every baseline forever (the Map is only cleared by
 * an explicit `reset()`), growing memory without limit. The tracker evicts the
 * least-recently-used baseline once this many files are tracked.
 */
export const MAX_TRACKED_BASELINE_FILES = 500;

/** Char budget for a rendered diagnostics block (Claude's 4000) + marker. */
export const MAX_DIAGNOSTICS_SUMMARY_CHARS = 4000;

/**
 * True for a UNC path (`\\server\share` or `//server/share`). Touching one with
 * stat()/readFile() on Windows triggers an outbound SMB/NTLM auth, leaking
 * credentials — so the LSP entry points short-circuit before any filesystem
 * access. Mirrors Claude's validateInput UNC guard.
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
    message: d.message,
    severity: mapLspSeverity(d.severity),
    range: {
      start: { line: d.range?.start?.line ?? 0, character: d.range?.start?.character ?? 0 },
      end: { line: d.range?.end?.line ?? 0, character: d.range?.end?.character ?? 0 },
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

/**
 * PARITY (additive): display-oriented path formatter mirroring Claude Code's
 * `formatUri` (tools/LSPTool/formatters.ts). Strips the `file://` protocol
 * (Windows drive-letter aware) and percent-decodes via {@link uriToPath}, then —
 * when `cwd` is given — prefers a cwd-relative path ONLY if it is strictly
 * shorter than the absolute path AND does not escape upward past `../..`;
 * otherwise the absolute path is kept. Backslashes are normalized to `/` for
 * stable cross-platform display. A missing/empty URI degrades to
 * `<unknown location>` rather than throwing. Used by the LSP-backed tools so
 * their path output is consistent and concise (no verbose absolute paths when a
 * short relative one exists).
 */
export function formatLspDisplayPath(uriOrPath: string | undefined, cwd?: string): string {
  if (!uriOrPath) return '<unknown location>';
  const filePath = uriToPath(uriOrPath);
  if (cwd) {
    const relativePath = path.relative(cwd, filePath).replace(/\\/g, '/');
    if (relativePath.length < filePath.length && !relativePath.startsWith('../../')) {
      return relativePath;
    }
  }
  return filePath.replace(/\\/g, '/');
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
 * FUNCTIONAL (dedup): collapse structurally-identical diagnostics, preserving
 * first-seen order. A language server can legitimately publish the same
 * diagnostic twice (e.g. a rule reported by two overlapping passes, or a
 * retransmitted `publishDiagnostics`), which would otherwise surface as
 * duplicate "new" entries and inflate the delta count. Equality is the full
 * structural key (range + message + severity + source + code) via
 * `areDiagnosticsEqual`, matching the dedup Claude's registry applies before
 * presenting diagnostics. Input is not mutated.
 */
export function dedupeDiagnostics(diagnostics: Diagnostic[]): Diagnostic[] {
  const out: Diagnostic[] = [];
  for (const d of diagnostics) {
    if (!out.some((seen) => areDiagnosticsEqual(seen, d))) out.push(d);
  }
  return out;
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

  /**
   * Max distinct file baselines retained before least-recently-used eviction.
   * Overridable (constructor arg) so tests can exercise eviction deterministically
   * without inserting the production default ({@link MAX_TRACKED_BASELINE_FILES}).
   */
  private readonly maxFiles: number;

  constructor(private readonly source: DiagnosticSource, maxFiles: number = MAX_TRACKED_BASELINE_FILES) {
    // A non-finite or non-positive cap would disable the bound entirely (or wedge
    // the eviction loop); clamp to at least one tracked file.
    this.maxFiles = Number.isFinite(maxFiles) && maxFiles >= 1 ? Math.floor(maxFiles) : MAX_TRACKED_BASELINE_FILES;
  }

  /** Number of distinct files whose baseline is currently retained (introspection). */
  get trackedFileCount(): number {
    return this.baseline.size;
  }

  /**
   * PARITY (additive): store a baseline with LRU recency + bounded retention.
   * A re-store deletes then re-inserts the key so it becomes the most-recently
   * used (Map preserves insertion order); once the retained count exceeds
   * {@link maxFiles}, the least-recently-used baselines are evicted. This bounds
   * memory in long sessions exactly like Claude's `LRUCache` without changing the
   * delta semantics for the files that remain tracked.
   */
  private storeBaseline(key: string, diagnostics: Diagnostic[]): void {
    this.baseline.delete(key);
    this.baseline.set(key, diagnostics);
    while (this.baseline.size > this.maxFiles) {
      const lru = this.baseline.keys().next().value;
      if (lru === undefined) break;
      this.baseline.delete(lru);
    }
  }

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
      this.storeBaseline(key, file.diagnostics);
    } else {
      this.storeBaseline(key, []);
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
      // FUNCTIONAL (path-mismatch guard): `beforeFileEdited` already refuses to
      // baseline a wrong-file response; the post-edit path must be just as
      // defensive. If the source returns diagnostics for a DIFFERENT file than
      // the one requested, do NOT compute a delta against this key's baseline
      // (every foreign diagnostic would look "new") and do NOT roll that foreign
      // set forward (which would poison the baseline for all later edits). Skip
      // it and leave the baseline untouched.
      if (file && !diagnosticPathsEqual(key, file.uri)) {
        continue;
      }
      const current = file ? file.diagnostics : [];
      const base = this.baseline.get(key) ?? [];
      // FUNCTIONAL (dedup): a server may publish the same diagnostic twice; dedup
      // the fresh set so the delta count reflects distinct new problems.
      const fresh = dedupeDiagnostics(
        current.filter((d) => !base.some((b) => areDiagnosticsEqual(d, b))),
      );
      if (fresh.length > 0) {
        out.push({ uri: file ? file.uri : `file://${key}`, diagnostics: fresh });
      }
      // Roll the baseline forward (also refreshes LRU recency + bounds memory).
      this.storeBaseline(key, current);
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
  // FUNCTIONAL: count only files that actually carry a diagnostic. A file entry
  // with an empty `diagnostics` array (e.g. a cleared file left in the list)
  // contributes zero issues, so including it in the file count produced wrong
  // wording like "Found 1 new diagnostic issue in 2 files".
  const fileCount = files.filter((f) => f.diagnostics.length > 0).length;
  const issueWord = totalIssues === 1 ? 'issue' : 'issues';
  const fileWord = fileCount === 1 ? 'file' : 'files';
  return `Found ${totalIssues} new diagnostic ${issueWord} in ${fileCount} ${fileWord}`;
}

/** Severity sort rank — Error(0) highest priority … Hint(3). */
function severityRank(severity: DiagnosticSeverityName): number {
  return DIAGNOSTIC_SEVERITY_NAMES.indexOf(severity);
}

/**
 * Enforce volume caps on untrusted LSP diagnostics before they reach the model:
 * each file's diagnostics are sorted severity-first (so errors survive), capped
 * at MAX_DIAGNOSTICS_PER_FILE, then the running total is capped at
 * MAX_TOTAL_DIAGNOSTICS. Files that end up empty are dropped. Input is not
 * mutated. Mirrors Claude's LSPDiagnosticRegistry volume limiting.
 */
export function capDiagnosticVolume(files: DiagnosticFile[]): DiagnosticFile[] {
  return capDiagnosticVolumeReport(files).files;
}

/** Result of {@link capDiagnosticVolumeReport}: the capped files plus how many
 *  diagnostics volume limiting dropped (for the truncation affordance message). */
export interface CappedDiagnostics {
  files: DiagnosticFile[];
  /** Count of diagnostics removed by per-file / total volume limiting. */
  removed: number;
}

/**
 * PARITY (additive): {@link capDiagnosticVolume} that also reports how many
 * diagnostics were dropped, so a caller can surface Claude Code's explicit
 * "Volume limiting removed N diagnostic(s)" affordance (LSPDiagnosticRegistry
 * L256-288) instead of silently truncating. `capDiagnosticVolume` delegates here
 * for its files-only view — one canonical capping implementation, no duplication.
 */
export function capDiagnosticVolumeReport(files: DiagnosticFile[]): CappedDiagnostics {
  const out: DiagnosticFile[] = [];
  let total = 0;
  let original = 0;
  for (const file of files) {
    original += file.diagnostics.length;
    const sorted = [...file.diagnostics].sort((a, b) => severityRank(a.severity) - severityRank(b.severity));
    let capped = sorted.slice(0, MAX_DIAGNOSTICS_PER_FILE);
    const remaining = MAX_TOTAL_DIAGNOSTICS - total;
    if (capped.length > remaining) {
      capped = capped.slice(0, Math.max(0, remaining));
    }
    if (capped.length > 0) {
      out.push({ uri: file.uri, diagnostics: capped });
      total += capped.length;
    }
  }
  return { files: out, removed: original - total };
}

/**
 * File-header path for a diagnostics block. A missing/empty URI (a malformed
 * `publishDiagnostics` payload or a synthetic file with no uri) becomes
 * `<unknown location>` rather than throwing inside `uriToPath` — otherwise one
 * bad file would abort rendering and the tool's outer catch would drop EVERY
 * file's diagnostics. Mirrors the defensive `<unknown location>` guard used for
 * the other LSP tool outputs (tools/lsp.ts formatUri).
 */
export function diagnosticHeaderPath(uri: string | undefined): string {
  if (!uri) return '<unknown location>';
  return uriToPath(uri);
}

/** Detailed, severity-annotated rendering of diagnostic files for tool output. */
export function formatDiagnosticsDetail(files: DiagnosticFile[]): string {
  const rendered = files
    .map((file) => {
      const name = diagnosticHeaderPath(file.uri);
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

  // Hard char cap so a flood of diagnostics can't blow the model's context.
  if (rendered.length > MAX_DIAGNOSTICS_SUMMARY_CHARS) {
    const marker = '…[truncated]';
    return rendered.slice(0, MAX_DIAGNOSTICS_SUMMARY_CHARS - marker.length) + marker;
  }
  return rendered;
}
