import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { spillToolText } from './spill-output';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { createHash } from 'crypto';

/** Minimal mime → file-extension map for persisting binary resource blobs. */
const MIME_EXT: Record<string, string> = {
  'image/png': 'png', 'image/jpeg': 'jpg', 'image/gif': 'gif', 'image/webp': 'webp', 'image/svg+xml': 'svg',
  'application/pdf': 'pdf', 'application/zip': 'zip', 'application/json': 'json', 'application/octet-stream': 'bin',
  'text/plain': 'txt', 'text/csv': 'csv', 'text/html': 'html',
};

function extForMime(mimeType: string | undefined): string {
  if (!mimeType) return 'bin';
  const base = mimeType.split(';')[0].trim().toLowerCase();
  return MIME_EXT[base] ?? (base.includes('/') ? base.split('/')[1].replace(/[^a-z0-9]/g, '') || 'bin' : 'bin');
}

/** A single normalized entry of a resource read (text OR a persisted blob path). */
interface ResourceContent { uri?: string; mimeType?: string; text?: string; blobSavedTo?: string }

/** Append an available-servers affordance to a not-found error, when we can list them. */
function withAvailableServers(message: string, mcpManager: { listServers?: () => Array<{ name: string }> } | undefined): string {
  const names = mcpManager?.listServers?.().map((s) => s.name) ?? [];
  return names.length ? `${message} Available servers: ${names.join(', ')}.` : `${message} No MCP servers are currently connected.`;
}

export const listMcpResourcesTool: ToolDefinition = {
  name: 'ListMcpResources',
  description:
    'List resources exposed by connected MCP servers. With no `server`, lists across ALL servers ' +
    '(each row is prefixed with its server for disambiguation); pass `server` to scope to one. ' +
    'Example: {"server": "github"}. Resources are read via ReadMcpResource.',
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
    // Unknown server filter → tell the model which servers actually exist.
    if (serverFilter) {
      const names = mcpManager.listServers?.().map((s: { name: string }) => s.name) ?? [];
      if (names.length && !names.includes(serverFilter)) {
        return { output: withAvailableServers(`Unknown MCP server "${serverFilter}".`, mcpManager), isError: true };
      }
    }
    try {
      const resources = await mcpManager.listResources(serverFilter);
      if (!resources || resources.length === 0) {
        return { output: 'No MCP resources available. (Servers may still provide tools or prompts — this lists resources only.)' };
      }
      const showServer = !serverFilter; // disambiguate only when listing across servers
      const lines = resources.map((r: { uri: string; name?: string; title?: string; mimeType?: string; server?: string }) => {
        const label = r.title || r.name || '';
        const prefix = showServer && r.server ? `[${r.server}] ` : '';
        return `${prefix}${r.uri} — ${label} (${r.mimeType ?? 'unknown'})`;
      });
      return { output: lines.join('\n'), metadata: { count: resources.length, server: serverFilter } };
    } catch (err) {
      return { output: `Error listing MCP resources: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
  },
};

/** Normalize a raw resources/read result into text/blob entries, persisting blobs. */
function normalizeResourceResult(result: unknown, dir: string): ResourceContent[] {
  const raw = (result && typeof result === 'object' ? (result as { contents?: unknown }).contents : undefined);
  if (!Array.isArray(raw)) {
    // Not the standard { contents: [...] } shape — fall back to a single text entry.
    const text = typeof result === 'string' ? result : JSON.stringify(result);
    return [{ text }];
  }
  return raw.map((item) => {
    const it = item as { uri?: string; mimeType?: string; text?: string; blob?: string };
    if (typeof it.blob === 'string' && it.blob.length) {
      try {
        fs.mkdirSync(dir, { recursive: true });
        const ext = extForMime(it.mimeType);
        const name = `${createHash('sha256').update(it.blob).digest('hex').slice(0, 16)}.${ext}`;
        const file = path.join(dir, name);
        fs.writeFileSync(file, Buffer.from(it.blob, 'base64'));
        return { uri: it.uri, mimeType: it.mimeType, blobSavedTo: file };
      } catch {
        // Persistence failed — degrade to a text note rather than throwing.
        return { uri: it.uri, mimeType: it.mimeType, text: '[binary blob could not be saved to disk]' };
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
    const names = mcpManager.listServers?.().map((s: { name: string }) => s.name) ?? [];
    if (names.length && !names.includes(server)) {
      return { output: withAvailableServers(`Unknown MCP server "${server}".`, mcpManager), isError: true };
    }
    try {
      const result = await mcpManager.readResource(server, input.uri as string, { signal: context.abortSignal });
      const dir = path.join(os.tmpdir(), 'ami-mcp-resources');
      const contents = normalizeResourceResult(result, dir);
      if (!contents.length || contents.every((c) => !c.text && !c.blobSavedTo)) {
        return { output: `MCP resource "${input.uri as string}" returned no content.`, metadata: { contents } };
      }
      const rendered = JSON.stringify({ contents }, null, 2);
      return { output: spillToolText(context.cwd, 'mcp-resource', rendered, 100_000), metadata: { contents } };
    } catch (err) {
      return { output: `Error reading MCP resource: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
  },
};
