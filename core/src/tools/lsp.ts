import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import {
  isUncPath,
  lspFileSizeBytes,
  uriToPath,
  MAX_LSP_FILE_SIZE_BYTES,
} from '../lsp/diagnostic-tracking';
import { isLspEnabled } from './tool-gates';

/**
 * Position-based semantic LSP tool — parity with Claude Code's LSPTool.
 *
 * A single `operation` enum dispatches nine language-server queries against the
 * real JSON-RPC client (which reads server responses and correlates them by id):
 * goToDefinition, findReferences, hover, documentSymbol, workspaceSymbol,
 * goToImplementation, prepareCallHierarchy, incomingCalls, outgoingCalls.
 * Positions are 1-based (as shown in editors) and converted to the 0-based LSP
 * protocol. Unknown file types return a graceful "no server" message instead of
 * throwing, matching Claude's undefined-result fallback.
 */

const OPERATIONS = [
  'goToDefinition',
  'findReferences',
  'hover',
  'documentSymbol',
  'workspaceSymbol',
  'goToImplementation',
  'prepareCallHierarchy',
  'incomingCalls',
  'outgoingCalls',
] as const;

type LspOperation = (typeof OPERATIONS)[number];

interface LspClientLike {
  request<T = unknown>(filePath: string, cwd: string, method: string, params: unknown): Promise<T | undefined>;
}

/** Indirection seam so tests can inject a fake client (mirrors lsp-diagnostics). */
export const _lspToolHooks = {
  getLSPClient(): LspClientLike {
    const { getLSPClient } = require('../lsp') as { getLSPClient: () => LspClientLike };
    return getLSPClient();
  },
};

interface Position { line: number; character: number }
interface Range { start: Position; end: Position }
interface Location { uri: string; range: Range }
interface LocationLink { targetUri: string; targetRange: Range }
interface DocumentSymbol { name: string; kind: number; detail?: string; range: Range; children?: DocumentSymbol[] }
interface SymbolInformation { name: string; kind: number; containerName?: string; location: Location }
interface CallHierarchyItem { name: string; kind?: number; detail?: string; uri: string; range: Range }
interface IncomingCall { from: CallHierarchyItem; fromRanges?: Range[] }
interface OutgoingCall { to: CallHierarchyItem; fromRanges?: Range[] }

/** Pluralize `word` for a count (Claude's `plural()` helper). */
function plural(n: number, word: string, pluralWord = `${word}s`): string {
  return n === 1 ? word : pluralWord;
}

/** LSP SymbolKind enum → readable label (1=File…26=TypeParameter). */
const SYMBOL_KINDS: Record<number, string> = {
  1: 'File', 2: 'Module', 3: 'Namespace', 4: 'Package', 5: 'Class',
  6: 'Method', 7: 'Property', 8: 'Field', 9: 'Constructor', 10: 'Enum',
  11: 'Interface', 12: 'Function', 13: 'Variable', 14: 'Constant', 15: 'String',
  16: 'Number', 17: 'Boolean', 18: 'Array', 19: 'Object', 20: 'Key',
  21: 'Null', 22: 'EnumMember', 23: 'Struct', 24: 'Event', 25: 'Operator',
  26: 'TypeParameter',
};
function symbolKindToString(kind: number | undefined): string {
  return SYMBOL_KINDS[kind ?? 0] ?? 'Unknown';
}

/**
 * Defensive URI render: a missing/undefined URI (malformed LSP payload) becomes
 * `<unknown location>` instead of printing "undefined"; otherwise reuse the
 * canonical `uriToPath` (handles percent-decode fallback + Windows drive-letter).
 */
function formatUri(uri: string | undefined): string {
  if (!uri) return '<unknown location>';
  return uriToPath(uri);
}

/** `line:character` (1-based) from a range, with a defensive 0:0 fallback. */
function linePos(range: Range | undefined): string {
  const line = range ? range.start.line + 1 : 0;
  const char = range ? range.start.character + 1 : 0;
  return `${line}:${char}`;
}

/** Normalize a Location | LocationLink into a Location shape. */
function toLocation(item: Location | LocationLink): Location {
  const uri = (item as Location).uri ?? (item as LocationLink).targetUri;
  const range = (item as Location).range ?? (item as LocationLink).targetRange;
  return { uri, range };
}

function formatLocation(loc: Location): string {
  return `${formatUri(loc.uri)}:${linePos(loc.range)}`;
}

/** Actionable, self-diagnosing empty-result message per operation. */
const NO_RESULT_MESSAGE: Record<LspOperation, string> = {
  goToDefinition: 'No definition found. This may occur if the cursor is not on a symbol, or if the definition is in an external library not indexed by the LSP server.',
  goToImplementation: 'No implementation found. This may occur if the symbol has no implementations, or if the LSP server has not fully indexed the workspace.',
  findReferences: 'No references found. This may occur if the symbol has no usages, or if the LSP server has not fully indexed the workspace.',
  hover: 'No hover information available. This may occur if the cursor is not on a symbol, or if the LSP server has not fully indexed the file.',
  documentSymbol: 'No symbols found in document. This may occur if the file is empty, not supported by the LSP server, or if the server has not fully indexed the file.',
  workspaceSymbol: 'No symbols found in workspace. This may occur if the workspace is empty, or if the LSP server has not finished indexing the project.',
  prepareCallHierarchy: 'No call hierarchy item found at this position.',
  incomingCalls: 'No incoming calls found (nothing calls this function).',
  outgoingCalls: 'No outgoing calls found (this function calls nothing).',
};

/** Group items by their rendered file path, preserving insertion order. */
function groupByFile<T>(items: T[], uriOf: (item: T) => string | undefined): Map<string, T[]> {
  const byFile = new Map<string, T[]>();
  for (const item of items) {
    const key = formatUri(uriOf(item));
    const existing = byFile.get(key);
    if (existing) existing.push(item);
    else byFile.set(key, [item]);
  }
  return byFile;
}

function methodFor(op: LspOperation): string {
  switch (op) {
    case 'goToDefinition': return 'textDocument/definition';
    case 'findReferences': return 'textDocument/references';
    case 'hover': return 'textDocument/hover';
    case 'documentSymbol': return 'textDocument/documentSymbol';
    case 'workspaceSymbol': return 'workspace/symbol';
    case 'goToImplementation': return 'textDocument/implementation';
    // Call-hierarchy ops all start by preparing the hierarchy item.
    case 'prepareCallHierarchy':
    case 'incomingCalls':
    case 'outgoingCalls':
      return 'textDocument/prepareCallHierarchy';
  }
}

function paramsFor(op: LspOperation, uri: string, position: Position): unknown {
  switch (op) {
    case 'findReferences':
      return { textDocument: { uri }, position, context: { includeDeclaration: true } };
    case 'documentSymbol':
      return { textDocument: { uri } };
    case 'workspaceSymbol':
      return { query: '' };
    default:
      return { textDocument: { uri }, position };
  }
}

/** goToDefinition / goToImplementation — operation-labeled location list. */
function formatLocationResult(op: 'goToDefinition' | 'goToImplementation', result: unknown): string {
  const arr = (Array.isArray(result) ? result : result == null ? [] : [result]) as (Location | LocationLink)[];
  const valid = arr.map(toLocation).filter((l) => l && l.uri);
  if (valid.length === 0) return NO_RESULT_MESSAGE[op];
  const noun = op === 'goToDefinition' ? 'definition' : 'implementation';
  const header = `Found ${valid.length} ${plural(valid.length, noun)}:`;
  return `${header}\n${valid.map((l) => `  ${formatLocation(l)}`).join('\n')}`;
}

/** findReferences — count + per-file grouped output with 1-based Line L:C. */
function formatReferencesResult(result: unknown): string {
  const arr = (Array.isArray(result) ? result : []) as Location[];
  const valid = arr.filter((l) => l && l.uri);
  if (valid.length === 0) return NO_RESULT_MESSAGE.findReferences;
  if (valid.length === 1) return `Found 1 reference:\n  ${formatLocation(valid[0])}`;
  const byFile = groupByFile(valid, (l) => l.uri);
  const lines = [`Found ${valid.length} references across ${byFile.size} ${plural(byFile.size, 'file')}:`];
  for (const [filePath, locs] of byFile) {
    lines.push(`\n${filePath}:`);
    for (const loc of locs) lines.push(`  Line ${linePos(loc.range)}`);
  }
  return lines.join('\n');
}

/** Recursively render a DocumentSymbol node with 2-space-per-level indent. */
function formatDocumentSymbolNode(symbol: DocumentSymbol, indent = 0): string[] {
  const prefix = '  '.repeat(indent);
  const detail = symbol.detail ? ` ${symbol.detail}` : '';
  const out = [`${prefix}${symbol.name} (${symbolKindToString(symbol.kind)})${detail} - Line ${symbol.range.start.line + 1}`];
  for (const child of symbol.children ?? []) out.push(...formatDocumentSymbolNode(child, indent + 1));
  return out;
}

/** workspaceSymbol (flat SymbolInformation[]) — count + per-file grouped. */
function formatWorkspaceSymbolResult(result: SymbolInformation[]): string {
  const valid = result.filter((s) => s && s.location && s.location.uri);
  if (valid.length === 0) return NO_RESULT_MESSAGE.workspaceSymbol;
  const lines = [`Found ${valid.length} ${plural(valid.length, 'symbol')} in workspace:`];
  const byFile = groupByFile(valid, (s) => s.location.uri);
  for (const [filePath, symbols] of byFile) {
    lines.push(`\n${filePath}:`);
    for (const s of symbols) {
      const container = s.containerName ? ` in ${s.containerName}` : '';
      lines.push(`  ${s.name} (${symbolKindToString(s.kind)}) - Line ${s.location.range.start.line + 1}${container}`);
    }
  }
  return lines.join('\n');
}

function formatDocumentSymbolResult(result: unknown): string {
  if (!Array.isArray(result)) return NO_RESULT_MESSAGE.documentSymbol;
  // FUNCTIONAL PARITY: drop null / non-object entries from a malformed payload
  // BEFORE the hierarchical-vs-flat shape detection. A null element otherwise
  // made `'location' in arr[0]` throw a TypeError (surfaced as an "Error
  // performing documentSymbol"); now it degrades to the actionable empty-result
  // message, matching Claude Code's defensive result handling.
  const arr = (result as unknown[]).filter(
    (x): x is DocumentSymbol | SymbolInformation => x != null && typeof x === 'object',
  );
  if (arr.length === 0) return NO_RESULT_MESSAGE.documentSymbol;
  // Per LSP spec, the server may return hierarchical DocumentSymbol[] (has
  // `range`) or flat SymbolInformation[] (has `location`). Detect + delegate.
  if ('location' in arr[0]) return formatWorkspaceSymbolResult(arr as SymbolInformation[]);
  const lines = ['Document symbols:'];
  for (const symbol of arr as DocumentSymbol[]) lines.push(...formatDocumentSymbolNode(symbol));
  return lines.join('\n');
}

function formatCallHierarchyItem(item: CallHierarchyItem): string {
  if (!item.uri) return `${item.name} (${symbolKindToString(item.kind)}) - <unknown location>`;
  const detail = item.detail ? ` [${item.detail}]` : '';
  return `${item.name} (${symbolKindToString(item.kind)}) - ${formatUri(item.uri)}:${item.range.start.line + 1}${detail}`;
}

function formatPrepareCallHierarchyResult(result: unknown): string {
  const arr = result as CallHierarchyItem[];
  if (!Array.isArray(arr) || arr.length === 0) return NO_RESULT_MESSAGE.prepareCallHierarchy;
  if (arr.length === 1) return `Call hierarchy item: ${formatCallHierarchyItem(arr[0])}`;
  return [`Found ${arr.length} call hierarchy items:`, ...arr.map((i) => `  ${formatCallHierarchyItem(i)}`)].join('\n');
}

/** Shared incoming/outgoing call formatter (direction picks the item + label). */
function formatCallsResult(
  op: 'incomingCalls' | 'outgoingCalls',
  result: unknown,
): string {
  const arr = (Array.isArray(result) ? result : []) as Array<IncomingCall | OutgoingCall>;
  const itemOf = (c: IncomingCall | OutgoingCall) =>
    op === 'incomingCalls' ? (c as IncomingCall).from : (c as OutgoingCall).to;
  const valid = arr.filter((c) => itemOf(c));
  if (valid.length === 0) return NO_RESULT_MESSAGE[op];
  const dir = op === 'incomingCalls' ? 'incoming' : 'outgoing';
  const siteLabel = op === 'incomingCalls' ? 'calls at' : 'called from';
  const lines = [`Found ${valid.length} ${dir} ${plural(valid.length, 'call')}:`];
  const byFile = groupByFile(valid, (c) => itemOf(c).uri);
  for (const [filePath, calls] of byFile) {
    lines.push(`\n${filePath}:`);
    for (const call of calls) {
      const item = itemOf(call);
      let line = `  ${item.name} (${symbolKindToString(item.kind)}) - Line ${item.range.start.line + 1}`;
      const ranges = call.fromRanges;
      if (ranges && ranges.length > 0) {
        line += ` [${siteLabel}: ${ranges.map((r) => `${r.start.line + 1}:${r.start.character + 1}`).join(', ')}]`;
      }
      lines.push(line);
    }
  }
  return lines.join('\n');
}

function formatResult(op: LspOperation, result: unknown): string {
  switch (op) {
    case 'goToDefinition':
    case 'goToImplementation':
      return formatLocationResult(op, result);
    case 'findReferences':
      return formatReferencesResult(result);
    case 'hover': {
      // FUNCTIONAL PARITY: a hover payload that renders to an empty / whitespace-
      // only string (e.g. `contents: ''`, `contents: []`, or all-empty array
      // entries) must fall back to the actionable "no hover information" message
      // rather than emitting a blank tool output. Matches Claude Code's hover
      // empty-result handling.
      const rendered = formatHover((result as { contents?: unknown } | null | undefined)?.contents);
      return rendered.trim() === '' ? NO_RESULT_MESSAGE.hover : rendered;
    }
    case 'documentSymbol':
      return formatDocumentSymbolResult(result);
    case 'workspaceSymbol':
      return Array.isArray(result) ? formatWorkspaceSymbolResult(result as SymbolInformation[]) : NO_RESULT_MESSAGE.workspaceSymbol;
    case 'prepareCallHierarchy':
      return formatPrepareCallHierarchyResult(result);
    case 'incomingCalls':
    case 'outgoingCalls':
      return formatCallsResult(op, result);
  }
}

function formatHover(contents: unknown): string {
  if (contents === null || contents === undefined) return NO_RESULT_MESSAGE.hover;
  if (typeof contents === 'string') return contents;
  if (Array.isArray(contents)) return contents.map(formatHover).join('\n');
  const c = contents as { value?: string; language?: string };
  if (typeof c.value === 'string') {
    // FUNCTIONAL PARITY: an LSP `MarkedString` carries a `{ language, value }`
    // pair whose value is a raw code snippet. Render it as a fenced code block
    // tagged with the language (as an editor hover popup would) instead of
    // silently dropping the language. A plain `MarkupContent` `{ kind, value }`
    // has no `language`, so it still renders as its bare value (unchanged).
    if (typeof c.language === 'string' && c.language !== '') {
      return `\`\`\`${c.language}\n${c.value}\n\`\`\``;
    }
    return c.value;
  }
  return JSON.stringify(contents);
}

export const lspTool: ToolDefinition = {
  name: 'lsp',
  // PARITY (additive): declarative per-tool gate for the LSP subsystem.
  isEnabled: () => isLspEnabled(),
  description:
    'Query a language server for precise, compiler-accurate code intelligence at a ' +
    'cursor position: goToDefinition, findReferences, hover, documentSymbol, ' +
    'workspaceSymbol, goToImplementation, prepareCallHierarchy, incomingCalls, outgoingCalls.',
  searchHint: 'language server definition references hover call hierarchy implementation symbols',
  inputSchema: {
    type: 'object',
    properties: {
      operation: {
        type: 'string',
        enum: [...OPERATIONS],
        description: 'The LSP operation to perform.',
      },
      file_path: { type: 'string', description: 'Path to the file (absolute or relative to cwd).' },
      line: { type: 'number', description: 'Line number (1-based, as shown in editors).' },
      character: { type: 'number', description: 'Character offset (1-based, as shown in editors).' },
    },
    required: ['operation', 'file_path'],
    // PARITY (additive): strict-object gating — Claude Code's LSP schemas use
    // `z.strictObject` (schemas.ts), which rejects any key outside the declared
    // set so a hallucinated/typo'd parameter (e.g. `operaton`, `filePath`,
    // `position`) is a hard InputValidationError, not a silently-dropped field.
    // The shared tool-input validator enforces this once the schema opts in.
    additionalProperties: false,
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const operation = input.operation as LspOperation;
    if (!OPERATIONS.includes(operation)) {
      return { output: `Invalid LSP operation "${String(input.operation)}". Valid: ${OPERATIONS.join(', ')}.`, isError: true };
    }
    const filePath = input.file_path as string;
    const absPath = path.isAbsolute(filePath) ? filePath : path.resolve(context.cwd, filePath);
    const uri = `file://${absPath}`;

    // SECURITY: never touch a UNC path — stat/read on \\server\share triggers an
    // outbound SMB/NTLM auth (credential leak). Skip before any filesystem work.
    if (isUncPath(absPath)) {
      return { output: `Skipping LSP for UNC path "${filePath}" (security: avoids NTLM credential leaks).` };
    }

    // File-size boundary: refuse to force a huge file through the language server.
    const sizeBytes = lspFileSizeBytes(absPath);
    if (sizeBytes !== undefined && sizeBytes > MAX_LSP_FILE_SIZE_BYTES) {
      return { output: `File too large for LSP analysis (${Math.ceil(sizeBytes / 1_000_000)}MB exceeds 10MB limit).` };
    }

    // workspaceSymbol/documentSymbol do not need a position; the others do.
    const needsPosition = operation !== 'workspaceSymbol' && operation !== 'documentSymbol';
    const line = typeof input.line === 'number' ? input.line : 1;
    const character = typeof input.character === 'number' ? input.character : 1;
    if (needsPosition && (!Number.isInteger(input.line) || !Number.isInteger(input.character) || line < 1 || character < 1)) {
      return { output: `Operation "${operation}" requires 1-based positive integer "line" and "character".`, isError: true };
    }
    // Convert 1-based (editor) → 0-based (LSP protocol).
    const position: Position = { line: line - 1, character: character - 1 };

    try {
      const client = _lspToolHooks.getLSPClient();
      let result = await client.request(absPath, context.cwd, methodFor(operation), paramsFor(operation, uri, position));

      if (result === undefined) {
        return { output: `No LSP server available for file type: ${path.extname(absPath) || '(none)'}` };
      }

      // Two-step call hierarchy: prepare → incoming/outgoing with the item.
      if (operation === 'incomingCalls' || operation === 'outgoingCalls') {
        const items = result as CallHierarchyItem[];
        if (!Array.isArray(items) || items.length === 0) {
          return { output: 'No call hierarchy item found at this position.' };
        }
        const callMethod = operation === 'incomingCalls' ? 'callHierarchy/incomingCalls' : 'callHierarchy/outgoingCalls';
        result = await client.request(absPath, context.cwd, callMethod, { item: items[0] });
      }

      return { output: formatResult(operation, result) };
    } catch (e) {
      return { output: `Error performing ${operation}: ${e instanceof Error ? e.message : String(e)}`, isError: true };
    }
  },
};
