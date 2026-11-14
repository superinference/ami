import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { uriToPath } from '../lsp/diagnostic-tracking';

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
interface DocumentSymbol { name: string; kind: number; range: Range; children?: DocumentSymbol[] }
interface SymbolInformation { name: string; kind: number; location: Location }
interface CallHierarchyItem { name: string; uri: string; range: Range }
interface IncomingCall { from: CallHierarchyItem }
interface OutgoingCall { to: CallHierarchyItem }

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

function locText(loc: Location | LocationLink): string {
  const uri = (loc as Location).uri ?? (loc as LocationLink).targetUri;
  const range = (loc as Location).range ?? (loc as LocationLink).targetRange;
  const line = range ? range.start.line + 1 : 0;
  const char = range ? range.start.character + 1 : 0;
  return `${uriToPath(uri)}:${line}:${char}`;
}

function flattenSymbols(symbols: DocumentSymbol[], depth = 0): string[] {
  const out: string[] = [];
  for (const s of symbols) {
    out.push(`${'  '.repeat(depth)}${s.name} (${s.range.start.line + 1}:${s.range.start.character + 1})`);
    if (s.children?.length) out.push(...flattenSymbols(s.children, depth + 1));
  }
  return out;
}

function formatResult(op: LspOperation, result: unknown): string {
  if (result === null || result === undefined) return `No result for ${op}.`;

  switch (op) {
    case 'goToDefinition':
    case 'goToImplementation':
    case 'findReferences': {
      const arr = (Array.isArray(result) ? result : [result]) as (Location | LocationLink)[];
      if (arr.length === 0) return `No ${op} results.`;
      return arr.map(locText).join('\n');
    }
    case 'hover': {
      const h = result as { contents?: unknown };
      return formatHover(h.contents);
    }
    case 'documentSymbol': {
      const arr = result as DocumentSymbol[] | SymbolInformation[];
      if (!Array.isArray(arr) || arr.length === 0) return 'No symbols found.';
      if ('location' in arr[0]) {
        return (arr as SymbolInformation[]).map((s) => `${s.name} — ${locText(s.location)}`).join('\n');
      }
      return flattenSymbols(arr as DocumentSymbol[]).join('\n');
    }
    case 'workspaceSymbol': {
      const arr = result as SymbolInformation[];
      if (!Array.isArray(arr) || arr.length === 0) return 'No workspace symbols found.';
      return arr.map((s) => `${s.name} — ${locText(s.location)}`).join('\n');
    }
    case 'prepareCallHierarchy': {
      const arr = result as CallHierarchyItem[];
      if (!Array.isArray(arr) || arr.length === 0) return 'No call hierarchy item found at this position.';
      return arr.map((i) => `${i.name} — ${uriToPath(i.uri)}:${i.range.start.line + 1}`).join('\n');
    }
    case 'incomingCalls': {
      const arr = result as IncomingCall[];
      if (!Array.isArray(arr) || arr.length === 0) return 'No incoming calls.';
      return arr.map((c) => `${c.from.name} — ${uriToPath(c.from.uri)}:${c.from.range.start.line + 1}`).join('\n');
    }
    case 'outgoingCalls': {
      const arr = result as OutgoingCall[];
      if (!Array.isArray(arr) || arr.length === 0) return 'No outgoing calls.';
      return arr.map((c) => `${c.to.name} — ${uriToPath(c.to.uri)}:${c.to.range.start.line + 1}`).join('\n');
    }
  }
}

function formatHover(contents: unknown): string {
  if (contents === null || contents === undefined) return 'No hover information.';
  if (typeof contents === 'string') return contents;
  if (Array.isArray(contents)) return contents.map(formatHover).join('\n');
  const c = contents as { value?: string; language?: string };
  if (typeof c.value === 'string') return c.value;
  return JSON.stringify(contents);
}

export const lspTool: ToolDefinition = {
  name: 'lsp',
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

    // workspaceSymbol/documentSymbol do not need a position; the others do.
    const needsPosition = operation !== 'workspaceSymbol' && operation !== 'documentSymbol';
    const line = typeof input.line === 'number' ? input.line : 1;
    const character = typeof input.character === 'number' ? input.character : 1;
    if (needsPosition && (typeof input.line !== 'number' || typeof input.character !== 'number')) {
      return { output: `Operation "${operation}" requires 1-based "line" and "character".`, isError: true };
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
