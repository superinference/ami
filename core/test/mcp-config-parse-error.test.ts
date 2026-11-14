import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager } from '../src/mcp/manager';
import type { McpConfigError } from '../src/mcp/manager';

// ---------------------------------------------------------------------------
// H (MCP Config Diagnostics): a config file that EXISTS but is malformed (invalid
// JSON) is surfaced as a fatal diagnostic + a 'config-error' event instead of
// being silently swallowed (which dropped every server the file declared). A
// MISSING file stays silent (expected).
// ---------------------------------------------------------------------------

const dirs: string[] = [];
function writeRaw(contents: string): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-parse-err-'));
  dirs.push(d);
  const p = path.join(d, 'mcp.json');
  fs.writeFileSync(p, contents);
  return p;
}
afterEach(() => {
  for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('malformed MCP config surfaced as a fatal diagnostic', () => {
  it('records + emits config-error for invalid JSON (not swallowed)', () => {
    const p = writeRaw('{ "mcpServers": { "srv": { "command": "run" }, '); // truncated / invalid
    const mgr = new McpManager();
    const events: McpConfigError[] = [];
    mgr.on('config-error', (diag: McpConfigError) => events.push(diag));
    mgr.loadFromConfig(p);

    const errors = mgr.getConfigErrors();
    assert.equal(errors.length, 1);
    assert.equal(errors[0].path, p);
    assert.equal(errors[0].severity, 'fatal');
    assert.ok(errors[0].message.length > 0);
    assert.equal(events.length, 1);
    assert.equal(events[0].path, p);
  });

  it('does NOT record an error for a missing config file (expected)', () => {
    const mgr = new McpManager();
    const events: McpConfigError[] = [];
    mgr.on('config-error', (d: McpConfigError) => events.push(d));
    mgr.loadFromConfig(path.join(os.tmpdir(), 'definitely-absent-mcp-' + Date.now() + '.json'));
    assert.deepEqual(mgr.getConfigErrors(), []);
    assert.equal(events.length, 0);
  });

  it('clears a stale parse error once the file parses cleanly', () => {
    const p = writeRaw('{ invalid');
    const mgr = new McpManager();
    mgr.loadFromConfig(p);
    assert.equal(mgr.getConfigErrors().length, 1);
    fs.writeFileSync(p, JSON.stringify({ mcpServers: { srv: { command: 'run' } } }));
    mgr.loadFromConfig(p);
    assert.deepEqual(mgr.getConfigErrors(), []);
  });

  it('clears the error when the file is later removed (missing is not an error)', () => {
    const p = writeRaw('{ nope');
    const mgr = new McpManager();
    mgr.loadFromConfig(p);
    assert.equal(mgr.getConfigErrors().length, 1);
    fs.rmSync(p);
    mgr.loadFromConfig(p);
    assert.deepEqual(mgr.getConfigErrors(), []);
  });
});
