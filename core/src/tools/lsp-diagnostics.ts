import { ToolDefinition, ToolResult, ToolContext, ToolInputSchema } from '../types';
import {
  capDiagnosticVolumeReport,
  formatDiagnosticsDetail,
  formatLspDisplayPath,
  formatNewDiagnosticsSummary,
  isUncPath,
  validateLspFilePath,
  truncateWithMarker,
  MAX_LSP_FILE_SIZE_BYTES,
  MAX_DIAGNOSTICS_PER_FILE,
  MAX_TOTAL_DIAGNOSTICS,
  MAX_DIAGNOSTICS_SUMMARY_CHARS,
  DIAGNOSTIC_SEVERITY_NAMES,
  type DiagnosticFile,
  type DiagnosticSeverityName,
} from '../lsp/diagnostic-tracking';
import { isLspEnabled } from './tool-gates';
import { logError } from '../logger';
import { toLoggableError } from '../credential-guard';
import * as path from 'path';

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
 * the `diagnostics` tool would hang the whole agent turn. Claude Code's LSP
 * surfacing settles diagnostics under a bounded wait, so we race every async
 * client call against this timeout and report a graceful "still indexing"
 * message instead of blocking. Stored on `_lspDiagHooks` (mutable) so tests can
 * shrink it deterministically without real delays.
 */
export const DEFAULT_LSP_DIAGNOSTICS_TIMEOUT_MS = 10_000;

/** Sentinel distinct from any DiagnosticFile[] result, returned on timeout. */
const LSP_TIMEOUT: unique symbol = Symbol('lsp-diagnostics-timeout');

/**
 * Resolve `p`, or `LSP_TIMEOUT` if it does not settle within `ms`. The timer is
 * `unref`'d (never keeps the process alive) and always cleared once `p` settles,
 * so no handle leaks even on the fast path.
 */
export async function raceDiagnosticsTimeout<T>(p: Promise<T>, ms: number): Promise<T | typeof LSP_TIMEOUT> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<typeof LSP_TIMEOUT>((resolve) => {
    timer = setTimeout(() => resolve(LSP_TIMEOUT), ms);
    if (typeof timer.unref === 'function') timer.unref();
  });
  try {
    return await Promise.race([p, timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function lspTimeoutMessage(filePath: string, ms: number): string {
  return `LSP diagnostics timed out for ${filePath} after ${ms}ms (the language server may still be indexing); retry shortly.`;
}

/**
 * PARITY (additive): accepted `severity` filter aliases → canonical severity.
 * Lets a caller scope the tool to a minimum severity (e.g. errors-only after a
 * refactor) the way Claude Code lets you filter the Problems surface. The filter
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

const SEVERITY_LABELS: Record<DiagnosticSeverityName, [string, string]> = {
  Error: ['error', 'errors'],
  Warning: ['warning', 'warnings'],
  Info: ['info', 'info'],
  Hint: ['hint', 'hints'],
};

/**
 * PARITY (additive): per-severity breakdown string (e.g. "2 errors, 1 warning"),
 * in severity order, omitting zero counts. Empty string when there is nothing to
 * report. Mirrors Claude Code's Problems-panel count summary so the model sees
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
 * PARITY (additive): health gate backing the LSP tools' `isEnabled`, mirroring
 * Claude Code's `isLspConnected()` → `LSPTool.isEnabled()` (manager.ts L96-110).
 *
 * Resolution: the env subsystem toggle ({@link isLspEnabled}) is the floor. On
 * top of it we detect the one connectivity edge case Claude also guards — every
 * language server that has been started is in the `'error'` state — and drop the
 * tools so the model is not advertised a capability that is wholly non-functional.
 *
 * Superset divergence (intentional, labelled): when NO server has been started
 * yet (`serverStates()` is undefined/empty) AMI keeps the tools advertised rather
 * than hiding them like Claude's zero-server case. AMI starts language servers
 * lazily on the first edit, so the tools must stay visible to bootstrap the
 * subsystem; hiding them pre-start would make LSP undiscoverable. We only gate on
 * the genuine all-servers-errored failure, which Claude says is "never detected"
 * in AMI today.
 */
export function lspDiagToolsEnabled(): boolean {
  if (!isLspEnabled()) return false;
  const states = _lspDiagHooks.serverStates();
  if (states && states.length > 0 && states.every((s) => s.state === 'error')) {
    return false;
  }
  return true;
}

/**
 * PARITY (additive): Claude Code's explicit volume-limit affordance
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
function withNote(body: string, note: string): string {
  return note ? `${body}\n${note}` : body;
}

/** Total diagnostics across the given files. */
function totalDiagnostics(files: DiagnosticFile[]): number {
  return files.reduce((sum, f) => sum + f.diagnostics.length, 0);
}

/**
 * PARITY (additive): structured result metadata mirroring Claude's LSPTool
 * `outputSchema` (operation / resultCount / fileCount), carried on
 * {@link ToolResult.metadata} so a programmatic caller can read counts without
 * re-parsing the text output.
 */
function diagMetadata(operation: string, files: DiagnosticFile[]): Record<string, unknown> {
  return { operation, resultCount: totalDiagnostics(files), fileCount: files.length };
}

/** JSON-schema fragment shared by both LSP tools' structured-output contract. */
const LSP_OUTPUT_SCHEMA: ToolInputSchema = {
  type: 'object',
  properties: {
    operation: { type: 'string', description: 'The LSP operation that was performed.' },
    resultCount: { type: 'number', description: 'Number of results (diagnostics / references).' },
    fileCount: { type: 'number', description: 'Number of files containing results.' },
  },
};

export const _lspDiagHooks = {
  /** Per-call async-fetch budget (ms); mutable so tests can shrink it. */
  diagnosticsTimeoutMs: DEFAULT_LSP_DIAGNOSTICS_TIMEOUT_MS,
  getLSPClient(): LspDiagClient {
    const { getLSPClient } = require('../lsp') as { getLSPClient: () => LspDiagClient };
    return getLSPClient();
  },
  /**
   * Snapshot of every language-server lifecycle state, or `undefined` when no
   * client has been instantiated yet. Non-creating (uses `peekLSPClient`) so the
   * `isEnabled` health gate never spins up the subsystem just to probe it.
   */
  serverStates(): Array<{ state: string }> | undefined {
    try {
      const { peekLSPClient } = require('../lsp') as {
        peekLSPClient: () => { getAllServerStatus(): Array<{ state: string }> } | null;
      };
      const client = peekLSPClient();
      return client ? client.getAllServerStatus() : undefined;
    } catch {
      return undefined;
    }
  },
  /**
   * Drain the async {@link LSPDiagnosticRegistry} for diagnostics not yet delivered
   * to the conversation this session (cross-turn dedup), flattening the single
   * drain result's files. Backs the `diagnostics` tool's `only_new` mode. Lazy
   * require keeps the LSP subsystem out of the tool's import graph until used.
   */
  drainNewDiagnostics(): DiagnosticFile[] {
    const { getLSPDiagnosticRegistry } = require('../lsp') as {
      getLSPDiagnosticRegistry: () => { checkForLSPDiagnostics: () => Array<{ files: DiagnosticFile[] }> };
    };
    return getLSPDiagnosticRegistry()
      .checkForLSPDiagnostics()
      .flatMap((d) => d.files);
  },
  searchSymbols(
    cwd: string,
    symbol: string,
  ): Array<{ name: string; type: string; filePath: string; line: number }> {
    const { getWorkspaceIndexer } = require('./search-symbols') as {
      getWorkspaceIndexer: (c: string) => {
        searchSymbols: (
          s: string,
          n: number,
        ) => Array<{ name: string; type: string; filePath: string; line: number }>;
      };
    };
    return getWorkspaceIndexer(cwd).searchSymbols(symbol, SEARCH_SYMBOL_CAP);
  },
};

export const lspDiagnosticsTool: ToolDefinition = {
  name: 'diagnostics',
  // PARITY (additive): per-tool gate = env subsystem toggle + live health check
  // (dropped only when every started language server has errored).
  isEnabled: () => lspDiagToolsEnabled(),
  // PARITY (additive): synonyms for tool_search / introspection discovery of the
  // code-intelligence surface (Claude's LSPTool searchHint).
  searchHint: 'code intelligence errors warnings problems type-check lint',
  // PARITY (additive): richer description with caveats (Claude's LSPTool prompt.ts).
  // Notes that diagnostics come from language servers started lazily on first edit,
  // so an empty result can mean "not yet indexed" rather than "clean", and that the
  // model should fall back to the project linter/compiler when none is available.
  description:
    'Get LSP diagnostics (errors, warnings, info, hints) for a file or the workspace. '
    + 'Best run right after an edit to catch issues it introduced; when a pre-edit baseline '
    + 'exists only the NEW diagnostics are returned. Language servers start lazily on the '
    + 'first edit and index asynchronously, so an empty result may mean "not yet indexed" '
    + 'rather than "clean"; when no server is available for the file type, run the project '
    + 'linter/compiler manually instead.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: { type: 'string', description: 'Optional file path. If omitted, returns workspace-wide diagnostics.' },
      severity: {
        type: 'string',
        description: 'Optional minimum severity floor (error | warning | info | hint). Keeps that level and anything more severe.',
      },
      verbose: {
        type: 'boolean',
        description: 'When false, return only a count summary (and severity breakdown) instead of the full per-diagnostic listing. Defaults to true (full detail).',
      },
      only_new: {
        type: 'boolean',
        description: 'Workspace scope only (no file_path). When true, return ONLY diagnostics not already surfaced this session (cross-turn deduplicated), so repeated calls do not re-report unchanged problems. Editing a file re-arms its diagnostics.',
      },
    },
    // PARITY (additive): strict-object gating (Claude's LSP `z.strictObject`) —
    // a hallucinated/typo'd parameter (e.g. `path`, `file`, `severty`) is a hard
    // InputValidationError via the shared tool-input validator, not silently
    // dropped. Keeps the three LSP tools' input contracts strict and consistent.
    additionalProperties: false,
  },
  outputSchema: LSP_OUTPUT_SCHEMA,
  isReadOnly: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, _context: ToolContext): Promise<ToolResult> {
    let client: LspDiagClient;
    try {
      client = _lspDiagHooks.getLSPClient();
    } catch {
      return { output: 'LSP not available. Run the project linter/compiler manually.' };
    }

    // PARITY (additive): runtime input validation before any work, mirroring
    // Claude's LSPTool.validateInput — reject a non-string file_path instead of
    // coercing garbage into a bogus URI.
    if (input.file_path !== undefined && typeof input.file_path !== 'string') {
      return { output: 'Invalid file_path: expected a string path.', isError: true };
    }
    const filePath = input.file_path as string | undefined;
    // PARITY (additive): `only_new` cross-turn-dedup drain is a WORKSPACE scope
    // concept (it mirrors Claude's registry, which delivers un-shown diagnostics
    // across the whole session). It is meaningless combined with a single
    // `file_path` (that path already has its own pre-edit baseline delta), so reject
    // the combination loudly instead of silently ignoring the flag.
    if (input.only_new !== undefined && typeof input.only_new !== 'boolean') {
      return { output: 'Invalid only_new: expected a boolean.', isError: true };
    }
    const onlyNew = input.only_new === true;
    if (onlyNew && filePath) {
      return { output: 'only_new applies to workspace scope; do not combine it with file_path.', isError: true };
    }
    // PARITY (additive): `verbose` toggle — full per-diagnostic listing (default)
    // vs a one-line count summary. Mirrors Claude's DiagnosticsDisplay verbose prop.
    const verbose = input.verbose !== false;
    // PARITY (additive): optional severity floor. Validate up front and, on an
    // unrecognized value, echo the valid set so the caller can self-correct.
    const severityRaw = input.severity as string | undefined;
    let minSeverity: DiagnosticSeverityName | undefined;
    if (severityRaw !== undefined) {
      minSeverity = normalizeSeverityFilter(severityRaw);
      if (!minSeverity) {
        return { output: `Invalid severity "${severityRaw}". Valid values: error, warning, info, hint.` };
      }
    }
    // SECURITY: skip UNC paths before any filesystem/LSP work (NTLM leak guard).
    if (filePath && isUncPath(filePath)) {
      return { output: `Skipping diagnostics for UNC path "${filePath}" (security: avoids NTLM credential leaks).` };
    }
    // PARITY (additive): pre-flight existence / regular-file validation with
    // distinct error codes (Claude's LSPTool.validateInput). A single stat()
    // classifies the failure (missing / not-a-file / unreadable) AND yields the
    // size for the MAX_LSP_FILE_SIZE_BYTES boundary below.
    if (filePath) {
      const validation = validateLspFilePath(filePath);
      if (!validation.ok) {
        return { output: validation.message, isError: true, metadata: { errorCode: validation.errorCode } };
      }
      // File-size boundary: never force a huge file through the language server.
      if (validation.sizeBytes > MAX_LSP_FILE_SIZE_BYTES) {
        return { output: `File too large for LSP analysis (${Math.ceil(validation.sizeBytes / 1_000_000)}MB exceeds 10MB limit).` };
      }
    }
    const timeoutMs = _lspDiagHooks.diagnosticsTimeoutMs;
    try {
      if (filePath) {
        // When a pre-edit baseline exists, surface ONLY the diagnostics this
        // edit introduced (the new-vs-baseline delta); otherwise report the
        // file's current diagnostics. Untrusted server output is volume-capped.
        if (client.diagnostics.hasBaseline(filePath)) {
          // PARITY (additive): bound the async fetch so a wedged server cannot
          // hang the turn.
          const raw = await raceDiagnosticsTimeout(client.diagnostics.getNewDiagnostics([filePath]), timeoutMs);
          if (raw === LSP_TIMEOUT) {
            return { output: lspTimeoutMessage(filePath, timeoutMs) };
          }
          const filtered = minSeverity ? filterDiagnosticsBySeverity(raw, minSeverity) : raw;
          const { files: fresh, removed } = capDiagnosticVolumeReport(filtered);
          if (fresh.length === 0) {
            return {
              output: minSeverity
                ? `No new ${minSeverity}-level (or higher) diagnostics introduced in ${filePath}.`
                : `No new diagnostics introduced in ${filePath}.`,
            };
          }
          const summary = formatNewDiagnosticsSummary(fresh);
          // PARITY (additive): verbose=false → summary line only (+ expand hint).
          const body = verbose
            ? withNote(`${summary}\n${formatDiagnosticsDetail(fresh)}`, volumeLimitNote(removed))
            : withNote(`${summary} (set verbose: true for the full per-diagnostic listing)`, volumeLimitNote(removed));
          return { output: body, metadata: diagMetadata('diagnostics', fresh) };
        }
        const current = await raceDiagnosticsTimeout(client.getDiagnostics(filePath), timeoutMs);
        if (current === LSP_TIMEOUT) {
          return { output: lspTimeoutMessage(filePath, timeoutMs) };
        }
        if (!current) {
          // PARITY (additive): name the exact file type so the caller knows which
          // language has no server (Claude's "No LSP server available for file
          // type: .rb"), while keeping the "No LSP server available" affordance.
          const ext = path.extname(filePath);
          const forType = ext ? ` for file type: ${ext}` : '';
          return { output: `No LSP server available${forType} (${filePath}). Run the relevant linter/compiler manually.` };
        }
        if (current.diagnostics.length === 0) {
          return { output: `No diagnostics for ${filePath}.` };
        }
        // PARITY (additive): apply the severity floor before capping/formatting.
        const filtered = minSeverity ? filterDiagnosticsBySeverity([current], minSeverity) : [current];
        const { files: capped, removed } = capDiagnosticVolumeReport(filtered);
        if (capped.length === 0) {
          // Reachable only when a severity floor removed every diagnostic.
          return {
            output: `No ${minSeverity}-level (or higher) diagnostics for ${filePath} (${current.diagnostics.length} lower-severity diagnostic(s) present).`,
          };
        }
        const breakdown = summarizeDiagnosticSeverities(capped);
        const fileSummary = `${totalDiagnostics(capped)} diagnostic issue(s) in ${filePath}${breakdown ? ` (${breakdown})` : ''}`;
        const body = verbose
          ? withNote(formatDiagnosticsDetail(capped), volumeLimitNote(removed))
          : withNote(`${fileSummary} (set verbose: true for the full per-diagnostic listing)`, volumeLimitNote(removed));
        return { output: body, metadata: diagMetadata('diagnostics', capped) };
      }

      // only_new mode drains the async registry (cross-turn dedup): already-shown
      // diagnostics are filtered, so repeated calls surface only genuinely-new ones.
      // The registry already deduped + volume-capped its drain, so we only apply the
      // optional severity floor here (no second cap).
      if (onlyNew) {
        let fresh = _lspDiagHooks.drainNewDiagnostics();
        if (minSeverity) fresh = filterDiagnosticsBySeverity(fresh, minSeverity);
        if (fresh.length === 0) {
          return {
            output: minSeverity
              ? `No new ${minSeverity}-level (or higher) diagnostics since the last check.`
              : 'No new diagnostics since the last check.',
            metadata: diagMetadata('diagnostics', []),
          };
        }
        const total = totalDiagnostics(fresh);
        const breakdown = summarizeDiagnosticSeverities(fresh);
        const breakdownSuffix = breakdown ? ` (${breakdown})` : '';
        const header = `${total} new diagnostic issue(s) across ${fresh.length} file(s)${breakdownSuffix}`;
        const body = verbose ? `${header}:\n${formatDiagnosticsDetail(fresh)}` : `${header} (set verbose: true for the full per-diagnostic listing)`;
        return { output: body, metadata: diagMetadata('diagnostics', fresh) };
      }

      let workspace = client.getAllDiagnostics();
      if (minSeverity) workspace = filterDiagnosticsBySeverity(workspace, minSeverity);
      const { files: all, removed } = capDiagnosticVolumeReport(workspace);
      if (all.length === 0) {
        if (!client.isConnected()) {
          return { output: 'No LSP server is currently connected; diagnostics appear once a server indexes an opened file.' };
        }
        // PARITY (additive): a connected server may still be indexing — surface how
        // many files are awaiting a publishDiagnostics so "no diagnostics" is not
        // mistaken for "clean" when results are merely not-yet-arrived (Claude's
        // getPendingLSPDiagnosticCount monitoring signal).
        const pending = client.getPendingDiagnosticCount?.() ?? 0;
        const pendingNote = pending > 0
          ? ` (${pending} file(s) still awaiting diagnostics from the language server)`
          : '';
        return {
          output: (minSeverity
            ? `No ${minSeverity}-level (or higher) diagnostics reported in the workspace.`
            : 'No diagnostics reported in the workspace.') + pendingNote,
        };
      }
      const total = totalDiagnostics(all);
      // PARITY (additive): append a per-severity breakdown to the flat total.
      const breakdown = summarizeDiagnosticSeverities(all);
      const breakdownSuffix = breakdown ? ` (${breakdown})` : '';
      const header = `${total} diagnostic issue(s) across ${all.length} file(s)${breakdownSuffix}`;
      // PARITY (additive): verbose=false → header (counts) only, no per-file detail.
      const body = verbose
        ? withNote(`${header}:\n${formatDiagnosticsDetail(all)}`, volumeLimitNote(removed))
        : withNote(`${header} (set verbose: true for the full per-diagnostic listing)`, volumeLimitNote(removed));
      return { output: body, metadata: diagMetadata('diagnostics', all) };
    } catch (e) {
      // PARITY (additive): error observability (Claude's LSPTool catch(e) → logError).
      // A failure DURING LSP work (a wedged client, a throwing getDiagnostics) is a
      // real fault, not the benign "no client" case — route it to the debug/error
      // logger (bounded, no untrusted diagnostic text) so it is distinguishable from
      // "LSP not available" in diagnostics/bug reports. Keep the same user-facing
      // fallback so model behaviour is unchanged.
      logError('lsp-diagnostics', toLoggableError(e));
      return { output: 'LSP not available. Run the project linter/compiler manually.' };
    }
  },
};

export const lspReferencesTool: ToolDefinition = {
  name: 'find_references',
  // PARITY (additive): per-tool gate = env subsystem toggle + live health check.
  isEnabled: () => lspDiagToolsEnabled(),
  // PARITY (additive): code-intelligence synonyms for tool_search discovery.
  searchHint: 'code intelligence symbol usages callers definitions navigate',
  // PARITY (additive): richer description with the workspace-indexer caveat
  // (Claude's LSPTool prompt.ts) — results rely on the background indexer and may
  // be empty until it has scanned the relevant files; grep is the fallback.
  description:
    'Find all references to a symbol in the codebase using the workspace indexer. '
    + 'Results depend on the background indexer, so they may be empty until it has '
    + 'scanned the relevant files (a workspace that was just opened may not be indexed '
    + 'yet); if you get no results for a symbol you expect to exist, retry shortly or '
    + 'use grep as a fallback.',
  inputSchema: {
    type: 'object',
    properties: {
      symbol: { type: 'string', description: 'Symbol name to find references for.' },
      file_path: { type: 'string', description: 'Optional file path to narrow the search.' },
    },
    required: ['symbol'],
    // PARITY (additive): strict-object gating (Claude's LSP `z.strictObject`) —
    // reject unknown keys (e.g. `line`, `character`, `symbolName`) so a caller
    // that meant the position-based `lsp` tool is told loudly instead of having
    // the stray argument silently ignored by the text-indexed search.
    additionalProperties: false,
  },
  outputSchema: LSP_OUTPUT_SCHEMA,
  isReadOnly: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // PARITY (additive): runtime input validation before touching the indexer —
    // reject a missing / non-string / empty-after-trim symbol instead of
    // searching with garbage (Claude's LSPTool.validateInput contract).
    if (typeof input.symbol !== 'string' || input.symbol.trim() === '') {
      return { output: 'Invalid symbol: expected a non-empty string.', isError: true };
    }
    if (input.file_path !== undefined && typeof input.file_path !== 'string') {
      return { output: 'Invalid file_path: expected a string path.', isError: true };
    }
    const symbol = input.symbol.trim();
    // PARITY (additive): abort boundary (Claude's LSPTool honors context's abort
    // signal before the lookup). A caller that already cancelled the turn must not
    // pay for a full workspace-index scan; short-circuit to the grep fallback.
    if (context.abortSignal?.aborted) {
      return { output: `Reference search for "${symbol}" aborted before it ran. Use grep as fallback.` };
    }
    try {
      const results = _lspDiagHooks.searchSymbols(context.cwd, symbol);
      if (results.length === 0) {
        return {
          output: `No references found for "${symbol}". This may occur if the symbol has no usages in the workspace, or if the indexer has not scanned the relevant files.`,
          metadata: { operation: 'find_references', resultCount: 0, fileCount: 0 },
        };
      }
      // Group by file (cwd-relative, shortened display path — Claude's formatUri
      // contract) with a count/file summary header, so a result spanning many
      // files is scannable and the model sees its magnitude.
      const byFile = new Map<string, typeof results>();
      for (const r of results) {
        const display = formatLspDisplayPath(r.filePath, context.cwd);
        const list = byFile.get(display);
        if (list) list.push(r);
        else byFile.set(display, [r]);
      }
      const refWord = results.length === 1 ? 'reference' : 'references';
      const fileWord = byFile.size === 1 ? 'file' : 'files';
      const lines = [`Found ${results.length} ${refWord} to "${symbol}" across ${byFile.size} ${fileWord}:`];
      for (const [file, refs] of byFile) {
        lines.push(`\n${file}:`);
        for (const r of refs) lines.push(`  ${r.name} (${r.type}) - Line ${r.line}`);
      }
      // SEARCH_SYMBOL_CAP matches the limit in _lspDiagHooks.searchSymbols; a full
      // page means the list was capped, so tell the model to narrow the query.
      if (results.length >= SEARCH_SYMBOL_CAP) {
        lines.push(`\n(showing first ${SEARCH_SYMBOL_CAP} matches — refine your query to narrow)`);
      }
      // PARITY (additive): output-size boundary (Claude caps rendered references).
      // The row cap bounds entry COUNT, but very long symbol/type/path strings can
      // still overflow the context budget, so cap total chars with the same shared
      // truncation marker used for the diagnostics block.
      const output = truncateWithMarker(lines.join('\n'), MAX_DIAGNOSTICS_SUMMARY_CHARS);
      return {
        output,
        metadata: { operation: 'find_references', resultCount: results.length, fileCount: byFile.size },
      };
    } catch (e) {
      // PARITY (additive): error observability — a real indexer failure is logged
      // (bounded message, no untrusted content) so it is distinguishable from the
      // benign empty-result case, matching Claude's LSPTool catch(e) → logError.
      logError('find_references', toLoggableError(e));
      return { output: `Error searching for "${symbol}". Use grep as fallback.`, isError: true };
    }
  },
};
