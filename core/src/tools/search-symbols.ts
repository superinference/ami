import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { WorkspaceIndexer, SymbolEntry } from '../workspace-indexer';
import * as path from 'path';
import { extractQuery } from './tool-utils';

// ---------------------------------------------------------------------------
// Module-level singleton
// ---------------------------------------------------------------------------

let _indexer: WorkspaceIndexer | null = null;
let _buildPromise: Promise<void> | null = null;
/** Resolves as soon as the background build has indexed its first chunk, so a
 *  tool call can search the partial index while the full build continues
 *  (progressive queryability — see WorkspaceIndexer.buildIndexAsync). */
let _queryablePromise: Promise<void> | null = null;
/** Set when the last background index build rejected, so the next call retries
 *  instead of permanently serving an empty index (a transient fs/git error at
 *  startup would otherwise never recover). Mirrors Claude Code's tracked
 *  backend-failure state. */
let _buildFailed = false;

/** Default cap on how long a tool call waits for the background index build. */
const DEFAULT_INDEX_TIMEOUT_MS = 10_000;

/** Max symbols returned to the model per search. */
const RESULT_LIMIT = 30;
/** When a `type` filter is active the result cap must apply AFTER filtering, so a
 *  wider ranked candidate set is pulled from the (type-agnostic) index and capped
 *  post-filter. Bounded so a broad query can't pull an unbounded set into memory. */
const TYPE_FILTER_CANDIDATE_LIMIT = RESULT_LIMIT * 10;

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
    // Fire-and-forget: start building the index in the background. The chunked
    // async build exposes a `queryable` signal (partial index ready) so a tool
    // call doesn't have to wait for the FULL build on a large repo, and a `done`
    // signal for completion. A rejected build records the failure so the next
    // call rebuilds rather than hanging on (or silently serving) a dead index.
    const { queryable, done } = _indexer.buildIndexAsync();
    _queryablePromise = queryable;
    _buildPromise = done
      .then(() => { _buildPromise = null; })
      .catch(() => { _buildPromise = null; _buildFailed = true; });
  }
  return _indexer;
}

/** Wait for the current build to finish (used internally by the tool), bounded by
 *  a timeout so a slow/stuck index build degrades to a partial-index search
 *  instead of blocking the tool call forever. The wait also stops early when the
 *  turn is aborted, so a cancelled call doesn't keep blocking on the build. */
async function ensureBuilt(cwd: string, abortSignal: AbortSignal): Promise<WorkspaceIndexer> {
  const indexer = getWorkspaceIndexer(cwd);
  // Wait only until the index is QUERYABLE (first chunk ready), not until the
  // full build completes: on a large repo this returns partial results quickly
  // while the background build fills in the rest, instead of blocking the whole
  // parse. For a small repo `queryable` and `done` settle together, so there is
  // no behavior change. Bounded by the timeout and stopped early on abort.
  if (_queryablePromise && !abortSignal.aborted) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let onAbort: (() => void) | undefined;
    await Promise.race([
      _queryablePromise,
      new Promise<void>(resolve => { timer = setTimeout(resolve, indexBuildTimeoutMs()); }),
      new Promise<void>(resolve => {
        onAbort = () => resolve();
        abortSignal.addEventListener('abort', onAbort, { once: true });
      }),
    ]);
    if (timer) clearTimeout(timer);
    if (onAbort) abortSignal.removeEventListener('abort', onAbort);
  }
  return indexer;
}

// For testing: reset the singleton
export function _resetIndexer(): void {
  _indexer = null;
  _buildPromise = null;
  _queryablePromise = null;
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
    // Honor turn cancellation: an already-aborted turn (or one aborted while the
    // index build is still being awaited) returns promptly instead of searching.
    const aborted = (): ToolResult | undefined =>
      context.abortSignal.aborted
        ? { output: 'Symbol search aborted.', isError: true }
        : undefined;

    const typeFilter = input.type as SymbolEntry['type'] | undefined;
    const q = extractQuery(input);
    if (q.error) return q.error;
    const query = q.query;

    let early = aborted();
    if (early) return early;

    const indexer = await ensureBuilt(context.cwd, context.abortSignal);

    early = aborted();
    if (early) return early;

    // The result cap must apply AFTER the type filter: fetching the top-N first and
    // filtering second would hide matching-type symbols ranked just past the cap.
    // With a filter active, pull a wider ranked candidate set and cap post-filter.
    const fetchLimit = typeFilter ? TYPE_FILTER_CANDIDATE_LIMIT : RESULT_LIMIT;
    let results = indexer.searchSymbols(query.trim(), fetchLimit);

    if (typeFilter) {
      results = results.filter(s => s.type === typeFilter).slice(0, RESULT_LIMIT);
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
