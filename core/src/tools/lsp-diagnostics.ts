import { ToolDefinition, ToolResult, ToolContext } from '../types';

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
  getLSPClient(): unknown {
    const { getLSPClient } = require('../lsp') as { getLSPClient: () => unknown };
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
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    try {
      _lspDiagHooks.getLSPClient();
      const filePath = input.file_path as string | undefined;
      if (filePath) {
        return { output: `[Diagnostics for ${filePath}]: Check editor for live LSP diagnostics. Run the relevant linter/compiler to see issues.` };
      }
      return { output: '[Workspace diagnostics]: Check editor for live LSP diagnostics. Run project build/lint command to see all issues.' };
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
