// Split from lsp.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { uriToPath } from '../lsp/diagnostic-tracking';

/**
 * Position-based semantic LSP tool — parity with the reference's LSPTool.
 *
 * A single `operation` enum dispatches nine language-server queries against the
 * real JSON-RPC client (which reads server responses and correlates them by id):
 * goToDefinition, findReferences, hover, documentSymbol, workspaceSymbol,
 * goToImplementation, prepareCallHierarchy, incomingCalls, outgoingCalls.
 * Positions are 1-based (as shown in editors) and converted to the 0-based LSP
 * protocol. Unknown file types return a graceful "no server" message instead of
 * throwing, matching the reference's undefined-result fallback.
 */

export const OPERATIONS = [
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

export type LspOperation = (typeof OPERATIONS)[number];

export interface LspClientLike {
  request<T = unknown>(filePath: string, cwd: string, method: string, params: unknown): Promise<T | undefined>;
}

export interface Position { line: number; character: number }

export interface Range { start: Position; end: Position }

export interface Location { uri: string; range: Range }

export interface LocationLink { targetUri: string; targetRange: Range; targetSelectionRange?: Range }

export interface DocumentSymbol { name: string; kind: number; detail?: string; range: Range; children?: DocumentSymbol[] }

export interface SymbolInformation { name: string; kind: number; containerName?: string; location: Location }

export interface CallHierarchyItem { name: string; kind?: number; detail?: string; uri: string; range: Range }

export interface IncomingCall { from: CallHierarchyItem; fromRanges?: Range[] }

export interface OutgoingCall { to: CallHierarchyItem; fromRanges?: Range[] }

/** LSP SymbolKind enum → readable label (1=File…26=TypeParameter). */
export const SYMBOL_KINDS: Record<number, string> = {
  1: 'File', 2: 'Module', 3: 'Namespace', 4: 'Package', 5: 'Class',
  6: 'Method', 7: 'Property', 8: 'Field', 9: 'Constructor', 10: 'Enum',
  11: 'Interface', 12: 'Function', 13: 'Variable', 14: 'Constant', 15: 'String',
  16: 'Number', 17: 'Boolean', 18: 'Array', 19: 'Object', 20: 'Key',
  21: 'Null', 22: 'EnumMember', 23: 'Struct', 24: 'Event', 25: 'Operator',
  26: 'TypeParameter',
};

export function symbolKindToString(kind: number | undefined): string {
  return SYMBOL_KINDS[kind ?? 0] ?? 'Unknown';
}

/**
 * Defensive URI render: a missing/undefined URI (malformed LSP payload) becomes
 * `<unknown location>` instead of printing "undefined"; otherwise reuse the
 * canonical `uriToPath` (handles percent-decode fallback + Windows drive-letter).
 */
export function formatUri(uri: string | undefined): string {
  if (!uri) return '<unknown location>';
  return uriToPath(uri);
}

/** `line:character` (1-based) from a range, with a defensive 0:0 fallback. */
export function linePos(range: Range | undefined): string {
  const line = range ? range.start.line + 1 : 0;
  const char = range ? range.start.character + 1 : 0;
  return `${line}:${char}`;
}
