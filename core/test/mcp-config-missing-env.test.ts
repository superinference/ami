import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// H35: env vars a server config references but that are unset are detected and
// surfaced (per-server diagnostics + a 'config-warning' event) instead of being
// silently blanked.
// ---------------------------------------------------------------------------

const dirs: string[] = [];
function writeConfig(obj: unknown): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-missing-env-'));
  dirs.push(d);
  const p = path.join(d, 'mcp.json');
  fs.writeFileSync(p, JSON.stringify(obj));
  return p;
}
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
  delete process.env.__MCP_PRESENT_VAR;
});

describe('missing env-var detection in MCP config (H35)', () => {
  it('records and emits a warning for unset referenced vars', () => {
    const cfg = writeConfig({
      mcpServers: {
        srv: { command: 'run', args: ['--token', '${__MCP_MISSING_VAR}'], env: { KEY: '${__MCP_ALSO_MISSING}' } },
      },
    });
    const mgr = new McpManager();
    const events: Array<[string, string[]]> = [];
    mgr.on('config-warning', (name: string, vars: string[]) => events.push([name, vars]));
    mgr.loadFromConfig(cfg);

    const warnings = mgr.getConfigWarnings('srv');
    assert.deepEqual(warnings.sort(), ['__MCP_ALSO_MISSING', '__MCP_MISSING_VAR']);
    assert.equal(events.length, 1);
    assert.equal(events[0][0], 'srv');
    assert.ok(mgr.getAllConfigWarnings().srv);
  });

  it('records no warning when all referenced vars are set (or defaulted)', () => {
    process.env.__MCP_PRESENT_VAR = 'v';
    const cfg = writeConfig({
      mcpServers: {
        srv: { command: 'run', args: ['${__MCP_PRESENT_VAR}', '${__MCP_X:-default}'] },
      },
    });
    const mgr = new McpManager();
    mgr.loadFromConfig(cfg);
    assert.deepEqual(mgr.getConfigWarnings('srv'), []);
  });
});
