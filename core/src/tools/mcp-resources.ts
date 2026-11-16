import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { spillToolText } from './spill-output';
import { getBinaryBlobSavedMessage } from './mcp-output-storage';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { createHash } from 'crypto';

/**
 * Conservative mime → file-extension allowlist for persisting binary resource
 * blobs, mirroring Claude's `extensionForMimeType`. Known types get their proper
 * extension (the extension matters because the Read tool dispatches on it — PDFs,
 * images, office docs, etc. need the right ext); EVERYTHING else is forced to
 * `bin`. We never derive an extension from an untrusted mimeType subtype, which
 * would both leak garbage (`vndopenxmlformats…`) and let the server influence the
 * on-disk filename extension.
 */
const MIME_EXT: Record<string, string> = {
  'application/pdf': 'pdf',
  'application/json': 'json',
  'text/csv': 'csv',
  'text/plain': 'txt',
  'text/html': 'html',
  'text/markdown': 'md',
  'application/zip': 'zip',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
  'application/msword': 'doc',
  'application/vnd.ms-excel': 'xls',
  'audio/mpeg': 'mp3',
  'audio/wav': 'wav',
  'audio/ogg': 'ogg',
  'video/mp4': 'mp4',
  'video/webm': 'webm',
  'image/png': 'png',
  'image/jpeg': 'jpg',
  'image/gif': 'gif',
  'image/webp': 'webp',
  'image/svg+xml': 'svg',
};

function extForMime(mimeType: string | undefined): string {
  if (!mimeType) return 'bin';
  // Strip any charset/boundary parameter before matching the allowlist.
  const base = (mimeType.split(';')[0] ?? '').trim().toLowerCase();
  return MIME_EXT[base] ?? 'bin';
}

/** A single normalized entry of a resource read (text OR a persisted blob path). */
interface ResourceContent { uri?: string; mimeType?: string; text?: string; blobSavedTo?: string }

/** Minimal view of a server-status row needed for pre-flight validation. */
interface ServerLike { name: string; state?: string }

/**
 * Pre-flight validation shared by both resource tools. Returns an actionable
 * error message when the named server is unknown or present-but-not-connected,
 * or null when the server is usable (or when we have no inventory to validate
 * against, in which case we defer to the manager's own downstream error).
 *
 * This discriminates two precondition branches the model can self-correct from:
 *   - unknown server      → lists the server names that DO exist
 *   - not-connected server → reports its state + lists the connected servers,
 *     instead of letting an opaque transport / not-ready error bubble up.
 */
function preflightServerError(
  mcpManager: { listServers?: () => ServerLike[] } | undefined,
  server: string,
): string | null {
  const servers = mcpManager?.listServers?.() ?? [];
  if (!servers.length) return null; // no inventory — let the manager report.
  const names = servers.map((s) => s.name);
  if (!names.includes(server)) {
    return `Unknown MCP server "${server}". Available servers: ${names.join(', ')}.`;
  }
  const match = servers.find((s) => s.name === server)!;
  // A server present in the inventory but not in the `ready` state cannot serve
  // a resources request — surface a distinct, actionable "not connected" error.
  if (match.state !== undefined && match.state !== 'ready') {
    const ready = servers.filter((s) => s.state === 'ready').map((s) => s.name);
    const hint = ready.length ? ` Connected servers: ${ready.join(', ')}.` : '';
    return `MCP server "${server}" is not connected (state: ${match.state}).${hint}`;
  }
  return null;
}

export const listMcpResourcesTool: ToolDefinition = {
  name: 'ListMcpResources',
  description:
    'List resources exposed by connected MCP servers. With no `server`, lists across ALL servers ' +
    '(each row is prefixed with its server for disambiguation); pass `server` to scope to one. ' +
    'Example: {"server": "github"}. Resources are read via ReadMcpResource.',
  // Synonyms NOT already in the name/description so the tool is findable by intent
  // via tool_search. Parity: Claude's ListMcpResourcesTool.searchHint.
  searchHint: 'enumerate discover available resource catalog inventory',
  // Declared result-size knob driving the spill cap below. Parity: Claude's
  // ListMcpResourcesTool.maxResultSizeChars (100_000).
  maxResultSizeChars: 100_000,
  inputSchema: {
    type: 'object',
    properties: {
      server: { type: 'string', description: 'Optional MCP server name. Omit to list across all connected servers.' },
    },
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const mcpManager = context._mcpManager;
    if (!mcpManager) return { output: 'Error: MCP is not initialized.', isError: true };
    const serverFilter = input.server as string | undefined;
    // Pre-flight: distinguish an unknown or not-connected filter from a genuinely
    // empty catalog, telling the model which servers actually exist / are ready.
    if (serverFilter) {
      const preErr = preflightServerError(mcpManager, serverFilter);
      if (preErr) return { output: preErr, isError: true };
    }
    try {
      const resources = await mcpManager.listResources(serverFilter);
      if (!resources || resources.length === 0) {
        return { output: 'No MCP resources available. (Servers may still provide tools or prompts — this lists resources only.)' };
      }
      const showServer = !serverFilter; // disambiguate only when listing across servers
      const lines = resources.map((r: { uri: string; name?: string; title?: string; mimeType?: string; description?: string; server?: string }) => {
        const label = r.title || r.name || '';
        const prefix = showServer && r.server ? `[${r.server}] ` : '';
        // Surface the server-provided description (when present) — it is the main
        // affordance the model uses to pick the right resource to read.
        const desc = r.description ? `: ${r.description}` : '';
        return `${prefix}${r.uri} — ${label} (${r.mimeType ?? 'unknown'})${desc}`;
      });
      // Cap the joined listing the same way the read tool caps its body, so a
      // server advertising a very large resource catalog cannot return an
      // unbounded string into the model context (Claude's maxResultSizeChars).
      const cap = listMcpResourcesTool.maxResultSizeChars ?? 100_000;
      const output = spillToolText(context.cwd, 'mcp-resources', lines.join('\n'), cap);
      return { output, metadata: { count: resources.length, server: serverFilter } };
    } catch (err) {
      return { output: `Error listing MCP resources: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
  },
};

/** Normalize a raw resources/read result into text/blob entries, persisting blobs. */
function normalizeResourceResult(result: unknown, dir: string, server: string): ResourceContent[] {
  const raw = (result && typeof result === 'object' ? (result as { contents?: unknown }).contents : undefined);
  if (!Array.isArray(raw)) {
    // Not the standard { contents: [...] } shape — fall back to a single text entry.
    const text = typeof result === 'string' ? result : JSON.stringify(result);
    return [{ text }];
  }
  return raw.map((it: { uri?: string; mimeType?: string; text?: string; blob?: string }) => {
    if (typeof it.blob === 'string' && it.blob.length) {
      const bytes = Buffer.from(it.blob, 'base64');
      try {
        fs.mkdirSync(dir, { recursive: true });
        const ext = extForMime(it.mimeType);
        const name = `${createHash('sha256').update(it.blob).digest('hex').slice(0, 16)}.${ext}`;
        const file = path.join(dir, name);
        fs.writeFileSync(file, bytes);
        // Keep both the structured path AND a legible saved-to-disk message so the
        // base64 is never inlined into the context (Claude-parity blob interception).
        const source = `[Resource from ${server} at ${it.uri ?? ''}] `;
        return { uri: it.uri, mimeType: it.mimeType, blobSavedTo: file, text: getBinaryBlobSavedMessage(file, it.mimeType, bytes.length, source) };
      } catch (err) {
        // Persistence failed — degrade to a text note rather than throwing.
        const reason = err instanceof Error ? err.message : String(err);
        return { uri: it.uri, mimeType: it.mimeType, text: `Binary content could not be saved to disk: ${reason}` };
      }
    }
    return { uri: it.uri, mimeType: it.mimeType, text: typeof it.text === 'string' ? it.text : '' };
  });
}

export const readMcpResourceTool: ToolDefinition = {
  name: 'ReadMcpResource',
  description:
    'Read one resource from an MCP server by URI. Both `server` and `uri` are required. ' +
    'Example: {"server": "github", "uri": "repo://owner/name/README.md"}. ' +
    'Text contents are returned inline (pretty-printed); binary blobs are decoded and saved to disk, ' +
    'with the file path returned as `blobSavedTo`.',
  // Synonyms NOT already in the name/description so the tool is findable by intent
  // via tool_search. Parity: Claude's ReadMcpResourceTool.searchHint.
  searchHint: 'fetch retrieve download a specific resource by uri',
  // Declared result-size knob driving the spill cap below. Parity: Claude's
  // ReadMcpResourceTool.maxResultSizeChars (100_000).
  maxResultSizeChars: 100_000,
  inputSchema: {
    type: 'object',
    properties: {
      server: { type: 'string', description: 'MCP server name (required).' },
      uri: { type: 'string', description: 'Resource URI to read (required).' },
    },
    required: ['server', 'uri'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const mcpManager = context._mcpManager;
    if (!mcpManager) return { output: 'Error: MCP is not initialized.', isError: true };
    const server = input.server as string;
    // Pre-flight: validate the server exists and is connected before issuing the
    // resources/read, returning a distinct actionable error for each case.
    const preErr = preflightServerError(mcpManager, server);
    if (preErr) return { output: preErr, isError: true };
    try {
      const result = await mcpManager.readResource(server, input.uri as string, { signal: context.abortSignal });
      const dir = path.join(os.tmpdir(), 'ami-mcp-resources');
      const contents = normalizeResourceResult(result, dir, server);
      if (!contents.length || contents.every((c) => !c.text && !c.blobSavedTo)) {
        return { output: `MCP resource "${input.uri as string}" returned no content.`, metadata: { contents } };
      }
      const rendered = JSON.stringify({ contents }, null, 2);
      const cap = readMcpResourceTool.maxResultSizeChars ?? 100_000;
      return { output: spillToolText(context.cwd, 'mcp-resource', rendered, cap), metadata: { contents } };
    } catch (err) {
      return { output: `Error reading MCP resource: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
  },
};
