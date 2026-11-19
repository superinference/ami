/**
 * UNS005 integration — bounded (soft-fail) MCP tool re-enumeration.
 *
 * Drives a REAL McpManager against a REAL spawned stdio MCP server whose FIRST
 * `tools/list` (at connect) answers immediately with one tool, then fires a
 * `tools/list_changed` notification, and whose SECOND `tools/list` (the refresh)
 * is DELAYED well past the configured enumeration budget before returning a
 * larger tool set.
 *
 * With the soft-fail timeout wiring, the refresh times out and keeps the
 * originally-advertised tool (length stays 1). WITHOUT it (revert the
 * softFailTimedEnumeration call in refreshToolsForServer), the refresh awaits the
 * delayed response and ingests the second (larger) list → length 2. This test is
 * therefore red-green discriminating.
 */
import { describe, it, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';
import { TOOL_ENUM_TIMEOUT_ENV } from '../src/tools/tool-gates';

after(() => { setTimeout(() => process.exit(0), 200).unref(); });

// A stdio MCP server that answers the connect-time tools/list fast, then
// delays the refresh tools/list past the budget while adding a second tool.
function slowRefreshServerArgs(delayMs: number): string[] {
  const script = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    const send = (o) => process.stdout.write(JSON.stringify(o) + '\\n');
    let listCount = 0;
    rl.on('line', (line) => {
      let msg; try { msg = JSON.parse(line); } catch { return; }
      const m = msg.method;
      if (m === 'initialize') {
        send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05',
          serverInfo: { name: 'srv', version: '1.0' },
          capabilities: { tools: {}, resources: {}, prompts: {} } } });
      } else if (m === 'tools/list') {
        listCount++;
        if (listCount === 1) {
          send({ jsonrpc: '2.0', id: msg.id, result: { tools: [
            { name: 'toolA', description: 'first', inputSchema: { type: 'object' } },
          ] } });
          // Trigger a refresh once the initial advertisement is in place.
          setTimeout(() => send({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' }), 10);
        } else {
          // The refresh enumeration — answer only after the delay.
          setTimeout(() => send({ jsonrpc: '2.0', id: msg.id, result: { tools: [
            { name: 'toolA', description: 'first', inputSchema: { type: 'object' } },
            { name: 'toolB', description: 'second', inputSchema: { type: 'object' } },
          ] } }), ${delayMs});
        }
      } else if (m === 'ping') {
        send({ jsonrpc: '2.0', id: msg.id, result: {} });
      } else if (msg.id !== undefined) {
        send({ jsonrpc: '2.0', id: msg.id, result: {} });
      }
    });
  `;
  return ['-e', script];
}

let mgr: McpManager;
let savedEnv: string | undefined;
afterEach(() => {
  mgr?.disconnectAll();
  if (savedEnv === undefined) delete process.env[TOOL_ENUM_TIMEOUT_ENV];
  else process.env[TOOL_ENUM_TIMEOUT_ENV] = savedEnv;
});

describe('UNS005 — refreshToolsForServer soft-fail enumeration timeout', () => {
  it('keeps existing tools when the refresh tools/list exceeds the budget', async () => {
    savedEnv = process.env[TOOL_ENUM_TIMEOUT_ENV];
    process.env[TOOL_ENUM_TIMEOUT_ENV] = '80'; // budget << the 600ms refresh delay

    mgr = new McpManager();
    mgr.addServer('slow', { command: process.execPath, args: slowRefreshServerArgs(600) });
    await mgr.ensureConnected('slow');

    // Connect-time advertisement: exactly one tool.
    assert.equal(mgr.getToolsForServer('slow').length, 1);

    // Wait well past BOTH the 80ms budget AND the 600ms delayed refresh response.
    // With the fix: the refresh timed out at ~80ms and toolB is never ingested.
    // Without the fix: the refresh would await 600ms and ingest toolB (length 2).
    await new Promise((r) => setTimeout(r, 900));

    const names = mgr.getToolsForServer('slow').map((t) => t.name).sort();
    assert.deepEqual(names, ['toolA'], 'toolB must not be ingested after a timed-out refresh');
  });
});
