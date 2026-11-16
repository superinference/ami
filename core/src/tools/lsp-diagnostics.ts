import { ToolDefinition, ToolResult, ToolContext } from '../types';
import {
  capDiagnosticVolume,
  formatDiagnosticsDetail,
  formatNewDiagnosticsSummary,
  isUncPath,
  lspFileSizeBytes,
  MAX_LSP_FILE_SIZE_BYTES,
  DIAGNOSTIC_SEVERITY_NAMES,
  type DiagnosticFile,
  type DiagnosticSeverityName,
} from '../lsp/diagnostic-tracking';
import { isLspEnabled } from './tool-gates';

/**
 * Minimal view of the LSP client the diagnostics tool relies on. The real
 * client (core/core/src/lsp/client.ts) implements all of these.
 */
export interface LspDiagClient {
  getDiagnostics(filePath: string): Promise<DiagnosticFile | null>;
  getAllDiagnostics(): DiagnosticFile[];
  isConnected(): boolean;
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

export const _lspDiagHooks = {
  /** Per-call async-fetch budget (ms); mutable so tests can shrink it. */
  diagnosticsTimeoutMs: DEFAULT_LSP_DIAGNOSTICS_TIMEOUT_MS,
  getLSPClient(): LspDiagClient {
    const { getLSPClient } = require('../lsp') as { getLSPClient: () => LspDiagClient };
    return getLSPClient();
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
  // PARITY (additive): declarative per-tool gate for the LSP subsystem.
  isEnabled: () => isLspEnabled(),
  description: 'Get LSP diagnostics (errors, warnings) for a file or the workspace. Useful for finding issues after edits.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: { type: 'string', description: 'Optional file path. If omitted, returns workspace-wide diagnostics.' },
      severity: {
        type: 'string',
        description: 'Optional minimum severity floor (error | warning | info | hint). Keeps that level and anything more severe.',
      },
    },
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, _context: ToolContext): Promise<ToolResult> {
    let client: LspDiagClient;
    try {
      client = _lspDiagHooks.getLSPClient();
    } catch {
      return { output: 'LSP not available. Run the project linter/compiler manually.' };
    }

    const filePath = input.file_path as string | undefined;
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
    // File-size boundary: never force a huge file through the language server.
    if (filePath) {
      const sizeBytes = lspFileSizeBytes(filePath);
      if (sizeBytes !== undefined && sizeBytes > MAX_LSP_FILE_SIZE_BYTES) {
        return { output: `File too large for LSP analysis (${Math.ceil(sizeBytes / 1_000_000)}MB exceeds 10MB limit).` };
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
          let fresh = minSeverity ? filterDiagnosticsBySeverity(raw, minSeverity) : raw;
          fresh = capDiagnosticVolume(fresh);
          if (fresh.length === 0) {
            return {
              output: minSeverity
                ? `No new ${minSeverity}-level (or higher) diagnostics introduced in ${filePath}.`
                : `No new diagnostics introduced in ${filePath}.`,
            };
          }
          return { output: `${formatNewDiagnosticsSummary(fresh)}\n${formatDiagnosticsDetail(fresh)}` };
        }
        const current = await raceDiagnosticsTimeout(client.getDiagnostics(filePath), timeoutMs);
        if (current === LSP_TIMEOUT) {
          return { output: lspTimeoutMessage(filePath, timeoutMs) };
        }
        if (!current) {
          return { output: `No LSP server available for ${filePath}. Run the relevant linter/compiler manually.` };
        }
        if (current.diagnostics.length === 0) {
          return { output: `No diagnostics for ${filePath}.` };
        }
        // PARITY (additive): apply the severity floor before capping/formatting.
        const filtered = minSeverity ? filterDiagnosticsBySeverity([current], minSeverity) : [current];
        const capped = capDiagnosticVolume(filtered);
        if (capped.length === 0) {
          // Reachable only when a severity floor removed every diagnostic.
          return {
            output: `No ${minSeverity}-level (or higher) diagnostics for ${filePath} (${current.diagnostics.length} lower-severity diagnostic(s) present).`,
          };
        }
        return { output: formatDiagnosticsDetail(capped) };
      }

      let workspace = client.getAllDiagnostics();
      if (minSeverity) workspace = filterDiagnosticsBySeverity(workspace, minSeverity);
      const all = capDiagnosticVolume(workspace);
      if (all.length === 0) {
        if (!client.isConnected()) {
          return { output: 'No LSP server is currently connected; diagnostics appear once a server indexes an opened file.' };
        }
        return {
          output: minSeverity
            ? `No ${minSeverity}-level (or higher) diagnostics reported in the workspace.`
            : 'No diagnostics reported in the workspace.',
        };
      }
      const total = all.reduce((sum, f) => sum + f.diagnostics.length, 0);
      // PARITY (additive): append a per-severity breakdown to the flat total.
      const breakdown = summarizeDiagnosticSeverities(all);
      const breakdownSuffix = breakdown ? ` (${breakdown})` : '';
      return {
        output: `${total} diagnostic issue(s) across ${all.length} file(s)${breakdownSuffix}:\n${formatDiagnosticsDetail(all)}`,
      };
    } catch {
      return { output: 'LSP not available. Run the project linter/compiler manually.' };
    }
  },
};

export const lspReferencesTool: ToolDefinition = {
  name: 'find_references',
  // PARITY (additive): declarative per-tool gate for the LSP subsystem.
  isEnabled: () => isLspEnabled(),
  description: 'Find all references to a symbol in the codebase using the workspace indexer.',
  inputSchema: {
    type: 'object',
    properties: {
      symbol: { type: 'string', description: 'Symbol name to find references for.' },
      file_path: { type: 'string', description: 'Optional file path to narrow the search.' },
    },
    required: ['symbol'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const symbol = input.symbol as string;
    try {
      const results = _lspDiagHooks.searchSymbols(context.cwd, symbol);
      if (results.length === 0) {
        return {
          output: `No references found for "${symbol}". This may occur if the symbol has no usages in the workspace, or if the indexer has not scanned the relevant files.`,
        };
      }
      // Group by file with a count/file summary header (singular/plural), so a
      // result spanning many files is scannable and the model sees its magnitude.
      const byFile = new Map<string, typeof results>();
      for (const r of results) {
        const list = byFile.get(r.filePath);
        if (list) list.push(r);
        else byFile.set(r.filePath, [r]);
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
      return { output: lines.join('\n') };
    } catch {
      return { output: `Error searching for "${symbol}". Use grep as fallback.`, isError: true };
    }
  },
};
