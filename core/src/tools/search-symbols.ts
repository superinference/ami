import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { WorkspaceIndexer, SymbolEntry } from '../workspace-indexer';
import * as path from 'path';
import { extractQuery, extractNonNegativeInt, plural } from './tool-utils';

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
 *  startup would otherwise never recover). Mirrors the reference's tracked
 *  backend-failure state. */
let _buildFailed = false;

/** Default cap on how long a tool call waits for the background index build. */
const DEFAULT_INDEX_TIMEOUT_MS = 10_000;

/** Default number of symbols returned to the model per search when the caller
 *  does not pass an explicit `limit`. */
const DEFAULT_RESULT_LIMIT = 30;
/** Hard ceiling on `limit` (and the meaning of `limit: 0` — "as many as allowed").
 *  Bounds the result volume pulled into context even when the model asks for
 *  "unlimited", mirroring the reference's capped head_limit behavior. */
const MAX_RESULT_LIMIT = 250;
/** When a `type` filter is active the result cap must apply AFTER filtering, so a
 *  wider ranked candidate set is pulled from the (type-agnostic) index and capped
 *  post-filter. Bounded so a broad query can't pull an unbounded set into memory. */
const TYPE_FILTER_CANDIDATE_MULTIPLIER = 10;

/** Every symbol kind the WorkspaceIndexer can emit (SymbolEntry['type']). The
 *  `type` filter enum MUST stay in sync with the data model so a caller can
 *  request any indexed kind (including 'method'/'import'/'export'), not just the
 *  historical five. */
const VALID_SYMBOL_TYPES: ReadonlyArray<SymbolEntry['type']> = [
  'function', 'class', 'interface', 'type', 'variable', 'method', 'import', 'export',
];

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
        description: 'Symbol name to search for (supports partial/fuzzy matches)',
      },
      type: {
        type: 'string',
        description: 'Filter by symbol kind',
        enum: [...VALID_SYMBOL_TYPES],
      },
      limit: {
        type: 'integer',
        description:
          `Maximum number of symbols to return (default ${DEFAULT_RESULT_LIMIT}, ` +
          `hard cap ${MAX_RESULT_LIMIT}). Pass 0 for "as many as allowed" (${MAX_RESULT_LIMIT}).`,
      },
      offset: {
        type: 'integer',
        description:
          'Skip the first N matches before returning results — page through a ' +
          'large result set (default 0).',
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
    // Validate the `type` filter against the kinds the indexer can actually emit.
    // A provider that doesn't strictly enforce the JSON-schema enum could pass an
    // arbitrary string; reject it with the valid options (per the project's
    // "invalid input must show valid options" rule) instead of silently filtering
    // to zero matches behind a misleading "no symbols found" message.
    if (typeFilter !== undefined && !VALID_SYMBOL_TYPES.includes(typeFilter)) {
      return {
        output: `Invalid "type" filter "${String(input.type)}". Valid types: ${VALID_SYMBOL_TYPES.join(', ')}.`,
        isError: true,
      };
    }

    const q = extractQuery(input);
    if (q.error) return q.error;
    const query = q.query;

    // Paging knobs: `limit` caps the returned set, `offset` skips the first N.
    const limitParsed = extractNonNegativeInt(input.limit, 'limit', DEFAULT_RESULT_LIMIT);
    if (limitParsed.error) return limitParsed.error;
    const offsetParsed = extractNonNegativeInt(input.offset, 'offset', 0);
    if (offsetParsed.error) return offsetParsed.error;
    // `limit: 0` means "as many as allowed"; any value is clamped to the hard cap.
    const limit = Math.min(limitParsed.value === 0 ? MAX_RESULT_LIMIT : limitParsed.value, MAX_RESULT_LIMIT);
    const offset = offsetParsed.value;

    let early = aborted();
    if (early) return early;

    const indexer = await ensureBuilt(context.cwd, context.abortSignal);

    early = aborted();
    if (early) return early;

    // Fetch enough ranked candidates to cover the requested page PLUS one extra, so
    // we can tell the caller whether more matches exist past the page (truncation
    // disclosure). With a `type` filter active the cap must apply AFTER filtering —
    // fetching the top-N first would hide matching-kind symbols ranked just past the
    // cap — so pull a wider candidate set and filter before paging.
    const pageEnd = offset + limit;
    const fetchLimit = typeFilter
      ? pageEnd * TYPE_FILTER_CANDIDATE_MULTIPLIER + 1
      : pageEnd + 1;
    let matched = indexer.searchSymbols(query.trim(), fetchLimit);
    if (typeFilter) matched = matched.filter(s => s.type === typeFilter);

    const page = matched.slice(offset, pageEnd);
    const hasMore = matched.length > pageEnd;

    const typeNote = typeFilter ? ` (type: ${typeFilter})` : '';

    if (page.length === 0) {
      const stats = indexer.getStats();
      // Actionable "why empty / what to try" guidance (mirrors the reference's
      // self-diagnosing empty-result messages) rather than just reporting counts.
      const offsetNote = offset > 0 && matched.length > 0
        ? ` ${matched.length} match(es) exist but offset ${offset} skipped past them — lower the offset.`
        : '';
      const hint = offsetNote
        ? offsetNote
        : ' Try a shorter or partial name, check casing (a lowercase query matches case-insensitively), or drop the type filter to broaden the search.';
      return {
        output: `No symbols matching "${query}"${typeNote} found. Searched ${stats.fileCount} files with ${stats.symbolCount} symbols.${hint}`,
      };
    }

    // Count header with singular/plural handling (so the model can immediately
    // gauge how many matches came back and whether to narrow the query).
    const header = `Found ${page.length} ${plural(page.length, 'symbol')} matching "${query}"${typeNote}:`;

    // Group results by file and print each path once as a header, then indented
    // `name (type) - Line N` entries — far more scannable than repeating the full
    // path on every line (mirrors the reference's formatWorkspaceSymbolResult).
    const byFile = new Map<string, SymbolEntry[]>();
    for (const s of page) {
      const relPath = path.relative(context.cwd, s.filePath);
      const bucket = byFile.get(relPath);
      if (bucket) bucket.push(s);
      else byFile.set(relPath, [s]);
    }

    const lines: string[] = [header];
    for (const [relPath, symbols] of byFile) {
      lines.push(`\n${relPath}:`);
      for (const s of symbols) lines.push(`  ${s.name} (${s.type}) - Line ${s.line}`);
    }

    // Truncation disclosure: when more matches exist past this page, tell the
    // caller so it knows the list was capped (and how to see more).
    if (hasMore) {
      lines.push(
        `\n(showing ${page.length} of more matches — raise "limit", set "offset" to page, or refine your query to narrow)`,
      );
    }

    return { output: lines.join('\n') };
  },
};
