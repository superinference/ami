import { ToolDefinition, ToolResult, ToolContext } from '../types';
import {
  capDiagnosticVolume,
  formatDiagnosticsDetail,
  formatNewDiagnosticsSummary,
  isUncPath,
  lspFileSizeBytes,
  MAX_LSP_FILE_SIZE_BYTES,
  type DiagnosticFile,
} from '../lsp/diagnostic-tracking';

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

export const _lspDiagHooks = {
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
  description: 'Get LSP diagnostics (errors, warnings) for a file or the workspace. Useful for finding issues after edits.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: { type: 'string', description: 'Optional file path. If omitted, returns workspace-wide diagnostics.' },
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
    try {
      if (filePath) {
        // When a pre-edit baseline exists, surface ONLY the diagnostics this
        // edit introduced (the new-vs-baseline delta); otherwise report the
        // file's current diagnostics. Untrusted server output is volume-capped.
        if (client.diagnostics.hasBaseline(filePath)) {
          const fresh = capDiagnosticVolume(await client.diagnostics.getNewDiagnostics([filePath]));
          if (fresh.length === 0) {
            return { output: `No new diagnostics introduced in ${filePath}.` };
          }
          return { output: `${formatNewDiagnosticsSummary(fresh)}\n${formatDiagnosticsDetail(fresh)}` };
        }
        const current = await client.getDiagnostics(filePath);
        if (!current) {
          return { output: `No LSP server available for ${filePath}. Run the relevant linter/compiler manually.` };
        }
        if (current.diagnostics.length === 0) {
          return { output: `No diagnostics for ${filePath}.` };
        }
        return { output: formatDiagnosticsDetail(capDiagnosticVolume([current])) };
      }

      const all = capDiagnosticVolume(client.getAllDiagnostics());
      if (all.length === 0) {
        return {
          output: client.isConnected()
            ? 'No diagnostics reported in the workspace.'
            : 'No LSP server is currently connected; diagnostics appear once a server indexes an opened file.',
        };
      }
      const total = all.reduce((sum, f) => sum + f.diagnostics.length, 0);
      return {
        output: `${total} diagnostic issue(s) across ${all.length} file(s):\n${formatDiagnosticsDetail(all)}`,
      };
    } catch {
      return { output: 'LSP not available. Run the project linter/compiler manually.' };
    }
  },
};

export const lspReferencesTool: ToolDefinition = {
  name: 'find_references',
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
