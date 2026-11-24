// Split from lsp-diagnostics.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolInputSchema } from '../types';
import { MAX_DIAGNOSTICS_PER_FILE, MAX_TOTAL_DIAGNOSTICS, DIAGNOSTIC_SEVERITY_NAMES, type DiagnosticFile, type DiagnosticSeverityName } from '../lsp/diagnostic-tracking';

/**
 * Minimal view of the LSP client the diagnostics tool relies on. The real
 * client (core/core/src/lsp/client.ts) implements all of these.
 */
export interface LspDiagClient {
  getDiagnostics(filePath: string): Promise<DiagnosticFile | null>;
  getAllDiagnostics(): DiagnosticFile[];
  isConnected(): boolean;
  /**
   * Files still awaiting a `publishDiagnostics` from their language server, so the
   * empty-workspace branch can distinguish "clean" from "not yet indexed" instead
   * of guessing. Optional on this minimal view — the real client always provides it
   * (see `LSPClient.getPendingDiagnosticCount`); a bare test stub may omit it.
   */
  getPendingDiagnosticCount?(): number;
  diagnostics: {
    hasBaseline(filePath: string): boolean;
    getNewDiagnostics(filePaths?: string[]): Promise<DiagnosticFile[]>;
  };
}

/**
 * Indirection seam for the lazily-required LSP + workspace-indexer dependencies.
 *
 * Exported as a plain, mutable object (not a namespace re-export) so tests can
 * stub individual methods with node:test's `mock.method` under BOTH the CJS
 * (tsx) runner and the native-ESM coverage harness. ESM freezes namespace
 * bindings but not the properties of an ordinary exported object, so stubbing
 * these methods is safe in either mode. The default implementations preserve
 * the original lazy-require behaviour exactly.
 */
/** Max symbol matches returned by find_references (shared with the result cap note). */
export const SEARCH_SYMBOL_CAP = 30;

/**
 * PARITY (additive): default wall-clock budget for a single async diagnostics
 * fetch. A language server that is still indexing (or wedged) can leave
 * `getDiagnostics` / `getNewDiagnostics` pending indefinitely; without a bound
 * the `diagnostics` tool would hang the whole agent turn. the reference's LSP
 * surfacing settles diagnostics under a bounded wait, so we race every async
 * client call against this timeout and report a graceful "still indexing"
 * message instead of blocking. Stored on `_lspDiagHooks` (mutable) so tests can
 * shrink it deterministically without real delays.
 */
export const DEFAULT_LSP_DIAGNOSTICS_TIMEOUT_MS = 10_000;

export function lspTimeoutMessage(filePath: string, ms: number): string {
  return `LSP diagnostics timed out for ${filePath} after ${ms}ms (the language server may still be indexing); retry shortly.`;
}

/**
 * PARITY (additive): accepted `severity` filter aliases → canonical severity.
 * Lets a caller scope the tool to a minimum severity (e.g. errors-only after a
 * refactor) the way the reference lets you filter the Problems surface. The filter
 * is a FLOOR: the requested level and everything MORE severe is kept.
 */
export const LSP_SEVERITY_ALIASES: Readonly<Record<string, DiagnosticSeverityName>> = {
  error: 'Error',
  errors: 'Error',
  warning: 'Warning',
  warnings: 'Warning',
  warn: 'Warning',
  info: 'Info',
  information: 'Info',
  hint: 'Hint',
  hints: 'Hint',
};

/** Canonicalize a user-supplied severity filter, or `undefined` if unrecognized. */
export function normalizeSeverityFilter(raw: string): DiagnosticSeverityName | undefined {
  return LSP_SEVERITY_ALIASES[raw.trim().toLowerCase()];
}

/**
 * Keep only diagnostics at or above `min` severity (Error is most severe). Files
 * left with no diagnostics are dropped. Input is never mutated.
 */
export function filterDiagnosticsBySeverity(files: DiagnosticFile[], min: DiagnosticSeverityName): DiagnosticFile[] {
  const floor = DIAGNOSTIC_SEVERITY_NAMES.indexOf(min);
  const out: DiagnosticFile[] = [];
  for (const file of files) {
    const kept = file.diagnostics.filter((d) => DIAGNOSTIC_SEVERITY_NAMES.indexOf(d.severity) <= floor);
    if (kept.length > 0) out.push({ uri: file.uri, diagnostics: kept });
  }
  return out;
}

export const SEVERITY_LABELS: Record<DiagnosticSeverityName, [string, string]> = {
  Error: ['error', 'errors'],
  Warning: ['warning', 'warnings'],
  Info: ['info', 'info'],
  Hint: ['hint', 'hints'],
};

/**
 * PARITY (additive): per-severity breakdown string (e.g. "2 errors, 1 warning"),
 * in severity order, omitting zero counts. Empty string when there is nothing to
 * report. Mirrors the reference's Problems-panel count summary so the model sees
 * the error/warning mix at a glance, not just a flat total.
 */
export function summarizeDiagnosticSeverities(files: DiagnosticFile[]): string {
  const counts: Record<DiagnosticSeverityName, number> = { Error: 0, Warning: 0, Info: 0, Hint: 0 };
  for (const file of files) for (const d of file.diagnostics) counts[d.severity]++;
  const parts: string[] = [];
  for (const sev of DIAGNOSTIC_SEVERITY_NAMES) {
    const n = counts[sev];
    if (n > 0) parts.push(`${n} ${SEVERITY_LABELS[sev][n === 1 ? 0 : 1]}`);
  }
  return parts.join(', ');
}

/**
 * PARITY (additive): the reference's explicit volume-limit affordance
 * (LSPDiagnosticRegistry L256-288). Returns the "Volume limiting removed N …"
 * notice when capping dropped diagnostics, else the empty string, so the model
 * knows its diagnostics view was truncated and why rather than silently losing
 * entries.
 */
export function volumeLimitNote(removed: number): string {
  if (removed <= 0) return '';
  const word = removed === 1 ? 'diagnostic' : 'diagnostics';
  return `Volume limiting removed ${removed} ${word} (max ${MAX_DIAGNOSTICS_PER_FILE}/file, ${MAX_TOTAL_DIAGNOSTICS} total).`;
}

/** Append a non-empty note to `body` on its own line; return `body` unchanged otherwise. */
export function withNote(body: string, note: string): string {
  return note ? `${body}\n${note}` : body;
}

/** Total diagnostics across the given files. */
export function totalDiagnostics(files: DiagnosticFile[]): number {
  return files.reduce((sum, f) => sum + f.diagnostics.length, 0);
}

/**
 * PARITY (additive): structured result metadata mirroring the reference's LSPTool
 * `outputSchema` (operation / resultCount / fileCount), carried on
 * {@link ToolResult.metadata} so a programmatic caller can read counts without
 * re-parsing the text output.
 */
export function diagMetadata(operation: string, files: DiagnosticFile[]): Record<string, unknown> {
  return { operation, resultCount: totalDiagnostics(files), fileCount: files.length };
}

/** JSON-schema fragment shared by both LSP tools' structured-output contract. */
export const LSP_OUTPUT_SCHEMA: ToolInputSchema = {
  type: 'object',
  properties: {
    operation: { type: 'string', description: 'The LSP operation that was performed.' },
    resultCount: { type: 'number', description: 'Number of results (diagnostics / references).' },
    fileCount: { type: 'number', description: 'Number of files containing results.' },
  },
};
