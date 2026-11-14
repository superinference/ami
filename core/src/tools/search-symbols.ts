import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { WorkspaceIndexer, SymbolEntry } from '../workspace-indexer';
import * as path from 'path';
import { extractQuery } from './tool-utils';

// ---------------------------------------------------------------------------
// Module-level singleton
// ---------------------------------------------------------------------------

let _indexer: WorkspaceIndexer | null = null;
let _buildPromise: Promise<void> | null = null;
/** Set when the last background index build rejected, so the next call retries
 *  instead of permanently serving an empty index (a transient fs/git error at
 *  startup would otherwise never recover). Mirrors Claude Code's tracked
 *  backend-failure state. */
let _buildFailed = false;

/** Default cap on how long a tool call waits for the background index build. */
const DEFAULT_INDEX_TIMEOUT_MS = 10_000;

/** Resolve the index-build wait timeout (ms) from the env flag, with a default. */
export function indexBuildTimeoutMs(env: NodeJS.ProcessEnv = process.env): number {
  const raw = env.AMI_SYMBOL_INDEX_TIMEOUT_MS;
  const n = raw !== undefined ? Number(raw) : NaN;
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_INDEX_TIMEOUT_MS;
}

/** Construction seam so tests can inject an indexer whose build hangs or rejects. */
export const indexerHooks: { create: (cwd: string) => WorkspaceIndexer } = {
  create: (cwd: string) => new WorkspaceIndexer(cwd),
};

export function getWorkspaceIndexer(cwd: string): WorkspaceIndexer {
  if (!_indexer || _indexer.cwd !== cwd || _buildFailed) {
    _indexer = indexerHooks.create(cwd);
    _buildFailed = false;
    // Fire-and-forget: start building the index in the background. A rejected
    // build records the failure so the next call rebuilds rather than hanging on
    // (or silently serving) a never-completed index.
    _buildPromise = _indexer.buildIndex()
      .then(() => { _buildPromise = null; })
      .catch(() => { _buildPromise = null; _buildFailed = true; });
  }
  return _indexer;
}

/** Wait for the current build to finish (used internally by the tool), bounded by
 *  a timeout so a slow/stuck index build degrades to a partial-index search
 *  instead of blocking the tool call forever. */
async function ensureBuilt(cwd: string): Promise<WorkspaceIndexer> {
  const indexer = getWorkspaceIndexer(cwd);
  if (_buildPromise) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      _buildPromise,
      new Promise<void>(resolve => { timer = setTimeout(resolve, indexBuildTimeoutMs()); }),
    ]);
    if (timer) clearTimeout(timer);
  }
  return indexer;
}

// For testing: reset the singleton
export function _resetIndexer(): void {
  _indexer = null;
  _buildPromise = null;
  _buildFailed = false;
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
    const typeFilter = input.type as SymbolEntry['type'] | undefined;
    const q = extractQuery(input);
    if (q.error) return q.error;
    const query = q.query;

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
