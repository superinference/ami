import { ToolDefinition, ToolResult, ToolContext } from '../types';
import {
  formatDiagnosticsDetail,
  formatNewDiagnosticsSummary,
  type DiagnosticFile,
} from '../lsp/diagnostic-tracking';

/**
 * Minimal view of the LSP client the diagnostics tool relies on. The real
 * client (core/core/src/lsp/client.ts) implements all of these.
 */
export interface LspDiagClient {
  getDiagnostics(filePath: string): Promise<DiagnosticFile | null>;
  getAllDiagnostics(): DiagnosticFile[];
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
    return getWorkspaceIndexer(cwd).searchSymbols(symbol, 30);
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
    try {
      if (filePath) {
        // When a pre-edit baseline exists, surface ONLY the diagnostics this
        // edit introduced (the new-vs-baseline delta); otherwise report the
        // file's current diagnostics.
        if (client.diagnostics.hasBaseline(filePath)) {
          const fresh = await client.diagnostics.getNewDiagnostics([filePath]);
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
        return { output: formatDiagnosticsDetail([current]) };
      }

      const all = client.getAllDiagnostics();
      if (all.length === 0) {
        return { output: 'No diagnostics reported in the workspace.' };
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
      if (results.length === 0) return { output: `No references found for "${symbol}".` };
      const formatted = results.map((r: any) => `${r.name} (${r.type}) in ${r.filePath}:${r.line}`).join('\n');
      return { output: `References for "${symbol}":\n${formatted}` };
    } catch {
      return { output: `Error searching for "${symbol}". Use grep as fallback.`, isError: true };
    }
  },
};
