import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager, stripJsonBom } from '../src/mcp/manager';
import { createMcpTool } from '../src/tools/mcp-tool';

// ===========================================================================
// U018 (w68) — MCP manager parity additions:
//  - stripJsonBom: strip a leading UTF-8 BOM before JSON.parse so a PowerShell /
//    Windows-generated `.mcp.json` (BOM-prefixed) loads instead of silently
//    dropping every server (Claude-parity utils/json.ts stripBOM,
//    remoteManagedSettings/syncCacheState.ts).
//  - disabled-server hint no longer references the non-existent `/mcp start`
//    subcommand (Claude's real affordance is the /mcp panel / `/mcp enable`).
// ===========================================================================

// Minimal fake MCP client (mirrors the other mcp-*.test.ts fakes).
class FakeClient extends EventEmitter {
  onElicitation: unknown;
  onSampling: unknown;
  state = 'ready';
  serverInfo = { name: 'fake', version: '1' };
  tools: unknown[] = [];
  resources: unknown[] = [];
  prompts: unknown[] = [];
  uptime: number | null = 0;
  setHeaders(): void {}
  async connect(): Promise<void> {}
  async connectHTTP(): Promise<void> {}
  async connectSSE(): Promise<void> {}
  async listTools(): Promise<unknown[]> { return []; }
  async listResources(): Promise<unknown[]> { return []; }
  async reconnect(): Promise<void> { this.state = 'ready'; }
  async revokeAuth(): Promise<void> {}
  disconnect(): void {}
  ping(): void {}
}

const dirs: string[] = [];
function writeRaw(contents: string): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-u018-w68-'));
  dirs.push(d);
  const p = path.join(d, 'mcp.json');
  fs.writeFileSync(p, contents);
  return p;
}
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('stripJsonBom', () => {
  it('removes a single leading UTF-8 BOM and leaves the rest intact', () => {
    assert.equal(stripJsonBom('﻿{"a":1}'), '{"a":1}');
  });
  it('is a no-op when there is no BOM', () => {
    assert.equal(stripJsonBom('{"a":1}'), '{"a":1}');
  });
  it('only strips the FIRST BOM (an interior BOM is preserved)', () => {
    assert.equal(stripJsonBom('﻿a﻿b'), 'a﻿b');
  });
  it('handles an empty string without throwing', () => {
    assert.equal(stripJsonBom(''), '');
  });
});

describe('McpManager.loadFromConfig — BOM-prefixed config (Windows/PowerShell)', () => {
  it('loads a .mcp.json that begins with a UTF-8 BOM (RED without stripJsonBom)', () => {
    // A PowerShell-written UTF-8 file begins with U+FEFF; without stripping it,
    // JSON.parse throws and the whole file is dropped as a fatal config error.
    const p = writeRaw('﻿' + JSON.stringify({ mcpServers: { srv: { command: 'run' } } }));
    const mgr = new McpManager();
    const errs: unknown[] = [];
    mgr.on('config-error', (d: unknown) => errs.push(d));
    mgr.loadFromConfig(p);

    assert.deepEqual(mgr.getConfigErrors(), [], 'a BOM-prefixed config must NOT be a parse error');
    assert.equal(errs.length, 0);
    const names = mgr.listAllServers().map((s) => s.name);
    assert.ok(names.includes('srv'), 'the server declared in the BOM-prefixed file must load');
  });

  it('still reports a genuinely malformed BOM-prefixed config as a fatal error', () => {
    const p = writeRaw('﻿{ "mcpServers": { "srv": { "command": "run" }, '); // truncated
    const mgr = new McpManager();
    mgr.loadFromConfig(p);
    assert.equal(mgr.getConfigErrors().length, 1, 'BOM removal must not mask a real JSON syntax error');
  });
});

describe('disabled-server hint — no fabricated /mcp start subcommand', () => {
  function newManager(): McpManager {
    const mgr = new McpManager();
    (mgr as unknown as { _createClient: () => FakeClient })._createClient = () => new FakeClient();
    return mgr;
  }

  it('ensureConnected on a disabled server rejects WITHOUT referencing /mcp start', async () => {
    const mgr = newManager();
    mgr.addServer('s1', { command: 'run' });
    await mgr.stopServer('s1', true); // -> disabled
    assert.equal(mgr.isDisabled('s1'), true);

    await assert.rejects(
      () => mgr.ensureConnected('s1'),
      (err: Error) => {
        assert.ok(/disabled/.test(err.message), 'message still says the server is disabled');
        assert.ok(!/\/mcp start/.test(err.message), 'must NOT reference the non-existent /mcp start subcommand');
        assert.ok(/\/mcp/.test(err.message), 'must point at the real /mcp affordance');
        return true;
      },
    );
  });

  it('the MCP tool reports a disabled server WITHOUT referencing /mcp start', async () => {
    const tool = createMcpTool('s1', 'do_thing', 'desc', {});
    const ctx = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      filesRead: new Set<string>(),
      _mcpManager: { isDisabled: (n: string) => n === 's1' },
    } as unknown as Parameters<typeof tool.execute>[1];
    const result = await tool.execute({}, ctx);
    assert.equal(result.isError, true);
    assert.ok(!/\/mcp start/.test(result.output), 'tool hint must not reference /mcp start');
    assert.ok(/\/mcp/.test(result.output), 'tool hint points at the real /mcp affordance');
  });
});
