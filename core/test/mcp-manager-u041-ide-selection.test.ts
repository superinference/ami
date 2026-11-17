/**
 * mcp-manager-u041-ide-selection.test.ts — the MCP manager re-emits the per-client
 * IDE editor-context events on its public bus (U041).
 *
 * Parity wiring: an IDE-as-MCP-server's normalized `ide-selection` / `ide-at-mentioned`
 * events (from McpClient) must surface on the manager's public event bus keyed by
 * server name, exactly like `resource-updated` / `tool-progress`, so the engine /
 * frontend can fold the user's editor context into the turn.
 *
 * ESM-SAFE: a minimal in-memory fake client (EventEmitter) injected via the
 * manager's `_createClient` seam; no spawn, no source-text reads.
 */

import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { McpManager } from '../src/mcp/manager';
import type { McpIdeSelection, McpIdeAtMention } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

class FakeClient extends EventEmitter {
  state = 'ready';
  serverInfo = { name: 'ide', version: '1' };
  tools: unknown[] = [];
  resources: unknown[] = [];
  uptime: number | null = 1;
  _tools: unknown[] = [];
  behavior: Record<string, unknown> = {};
  onElicitation?: (req: unknown) => Promise<unknown>;
  onSampling?: (req: unknown) => Promise<unknown>;
  async connect() {}
  async connectHTTP() {}
  async connectSSE() {}
  async listTools() { return this._tools; }
  async listResources() { return this.resources; }
  async callTool() { return { ok: true }; }
  async listPrompts() { return []; }
  async ping() {}
  async reconnect() {}
  disconnect() {}
}

describe('McpManager — IDE editor-context event forwarding', () => {
  it('re-emits ide-selection keyed by server name', () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake as never);
    mgr.addServer('ide', { command: 'x' });

    const seen: Array<{ name: string; sel: McpIdeSelection }> = [];
    mgr.on('ide-selection', (name: string, sel: McpIdeSelection) => seen.push({ name, sel }));

    const sel: McpIdeSelection = { lineCount: 3, lineStart: 2, text: 'hi', filePath: '/a.ts' };
    fake.emit('ide-selection', sel);

    assert.deepEqual(seen, [{ name: 'ide', sel }]);
  });

  it('re-emits ide-at-mentioned keyed by server name', () => {
    const mgr = new McpManager();
    const fake = new FakeClient();
    mgr._createClient = (() => fake as never);
    mgr.addServer('ide', { command: 'x' });

    const seen: Array<{ name: string; m: McpIdeAtMention }> = [];
    mgr.on('ide-at-mentioned', (name: string, m: McpIdeAtMention) => seen.push({ name, m }));

    const mention: McpIdeAtMention = { filePath: '/a.ts', lineStart: 4, lineEnd: 7 };
    fake.emit('ide-at-mentioned', mention);

    assert.deepEqual(seen, [{ name: 'ide', m: mention }]);
  });
});
