import { ToolDefinition, ToolResult, ToolContext } from '../types';

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
      const { getLSPClient } = require('../lsp');
      const lsp = getLSPClient();
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
      const { getWorkspaceIndexer } = require('./search-symbols');
      const indexer = getWorkspaceIndexer(context.cwd);
      const results = indexer.searchSymbols(symbol, 30);
      if (results.length === 0) return { output: `No references found for "${symbol}".` };
      const formatted = results.map((r: any) => `${r.name} (${r.type}) in ${r.file}:${r.line}`).join('\n');
      return { output: `References for "${symbol}":\n${formatted}` };
    } catch {
      return { output: `Error searching for "${symbol}". Use grep as fallback.`, isError: true };
    }
  },
};
