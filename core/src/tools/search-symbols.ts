import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { WorkspaceIndexer, SymbolEntry } from '../workspace-indexer';
import * as path from 'path';
import { validateRequiredString } from './tool-utils';

// ---------------------------------------------------------------------------
// Module-level singleton
// ---------------------------------------------------------------------------

let _indexer: WorkspaceIndexer | null = null;
let _buildPromise: Promise<void> | null = null;

export function getWorkspaceIndexer(cwd: string): WorkspaceIndexer {
  if (!_indexer || _indexer.cwd !== cwd) {
    _indexer = new WorkspaceIndexer(cwd);
    // Fire-and-forget: start building the index in the background
    _buildPromise = _indexer.buildIndex().then(() => { _buildPromise = null; }).catch(() => { _buildPromise = null; });
  }
  return _indexer;
}

/** Wait for the current build to finish (used internally by the tool). */
async function ensureBuilt(cwd: string): Promise<WorkspaceIndexer> {
  const indexer = getWorkspaceIndexer(cwd);
  if (_buildPromise) {
    await _buildPromise;
  }
  return indexer;
}

// For testing: reset the singleton
export function _resetIndexer(): void {
  _indexer = null;
  _buildPromise = null;
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

export const searchSymbolsTool: ToolDefinition = {
  name: 'search_symbols',
  description:
    'Search for function, class, variable, or type definitions across the workspace. ' +
    'Returns symbol name, type, file path, and line number. ' +
    'Useful for finding where something is defined without knowing the exact file.',
  inputSchema: {
    type: 'object',
    properties: {
      query: {
        type: 'string',
        description: 'Symbol name to search for (supports partial matches)',
      },
      type: {
        type: 'string',
        description: 'Filter by symbol type',
        enum: ['function', 'class', 'interface', 'type', 'variable'],
      },
    },
    required: ['query'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const query = input.query as string;
    const typeFilter = input.type as SymbolEntry['type'] | undefined;

    const invalid = validateRequiredString(query, 'query');
    if (invalid) return invalid;

    const indexer = await ensureBuilt(context.cwd);
    let results = indexer.searchSymbols(query.trim(), 30);

    if (typeFilter) {
      results = results.filter(s => s.type === typeFilter);
    }

    if (results.length === 0) {
      const stats = indexer.getStats();
      return {
        output: `No symbols matching "${query}"${typeFilter ? ` (type: ${typeFilter})` : ''} found. Searched ${stats.fileCount} files with ${stats.symbolCount} symbols.`,
      };
    }

    const lines = results.map(s => {
      const relPath = path.relative(context.cwd, s.filePath);
      return `${s.name} (${s.type}) in ${relPath}:${s.line}`;
    });

    return { output: lines.join('\n') };
  },
};
