/**
 * Parity unit U017 — core/core/src/mcp/manager.ts.
 *
 * MCP server-status introspection fields, surfacing config detail the `/mcp` drill
 * previously could not show (parity with Claude Code's MCPStdioServerMenu and
 * `mcp xaa show`):
 *
 *   1. `command` / `args` on `McpServerStatus` — the resolved launch command + args of
 *      a stdio server (MCPStdioServerMenu.tsx:L115-123). A remote (http/sse/ws) server
 *      is URL-backed and surfaces neither.
 *   2. `oauth` credential-state on `McpServerStatus` — the read-only counterpart of
 *      `claude mcp xaa show` (xaaIdpCommand.tsx:L220-240): resolved clientId/scope/
 *      callbackPort, whether a client secret is stored (vs PKCE-only) and whether an
 *      OAuth token file already exists on disk (logged in), WITHOUT exposing any secret
 *      or token value.
 *
 * Run with: node --import tsx --test core/core/test/mcp-manager-u017-launch-oauth-fields.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { McpManager, type McpServerConfig } from '../src/mcp/manager';
import { mcpCredentialKey, mcpTokenFileName } from '../src/mcp/oauth';

describe('McpManager U017 — command/args status fields', () => {
  let mgr: McpManager;
  beforeEach(() => { mgr = new McpManager(); });

  it('surfaces the resolved command + args of a stdio server', () => {
    mgr.addPendingServer('local', { command: 'node', args: ['server.js', '--port', '3000'] });
    const status = mgr.listAllServers().find(s => s.name === 'local');
    assert.ok(status);
    assert.equal(status.command, 'node');
    assert.deepEqual(status.args, ['server.js', '--port', '3000']);
  });

  it('defaults an untransported server to stdio and surfaces its command', () => {
    mgr.addPendingServer('implied', { command: 'mcp-bin' });
    const status = mgr.listAllServers().find(s => s.name === 'implied');
    assert.ok(status);
    assert.equal(status.command, 'mcp-bin');
    // No args configured → the field is omitted entirely.
    assert.ok(!('args' in status) || status.args === undefined);
  });

  it('omits command/args for a remote (http) server', () => {
    const cfg: McpServerConfig = { command: '', transport: 'http', url: 'https://api.example.com/mcp' };
    mgr.addPendingServer('remote', cfg);
    const status = mgr.listAllServers().find(s => s.name === 'remote');
    assert.ok(status);
    assert.ok(!('command' in status) || status.command === undefined);
    assert.ok(!('args' in status) || status.args === undefined);
  });

  it('omits an empty args array', () => {
    mgr.addPendingServer('noargs', { command: 'node', args: [] });
    const status = mgr.listAllServers().find(s => s.name === 'noargs');
    assert.ok(status);
    assert.ok(!('args' in status) || status.args === undefined);
  });
});

describe('McpManager U017 — oauth credential-state status field', () => {
  let mgr: McpManager;
  let tmp: string;
  let prevData: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u017-oauth-'));
    prevData = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
    mgr = new McpManager();
  });
  afterEach(() => {
    if (prevData === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevData;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  it('surfaces resolved identity + hasClientSecret (confidential client, not logged in)', () => {
    mgr.addPendingServer('oauthed', {
      command: '', transport: 'sse', url: 'https://sse.example.com/mcp',
      oauth: { clientId: 'cid-123', scope: 'read write', callbackPort: 7777, clientSecret: 's3cr3t' },
    });
    const status = mgr.listAllServers().find(s => s.name === 'oauthed');
    assert.ok(status);
    assert.ok(status.oauth);
    assert.equal(status.oauth.clientId, 'cid-123');
    assert.equal(status.oauth.scope, 'read write');
    assert.equal(status.oauth.callbackPort, 7777);
    assert.equal(status.oauth.hasClientSecret, true);
    assert.equal(status.oauth.loggedIn, false);
    // The secret VALUE is never surfaced anywhere on the status.
    assert.ok(!JSON.stringify(status).includes('s3cr3t'));
  });

  it('reports hasClientSecret=false for a PKCE-only client', () => {
    mgr.addPendingServer('pkce', {
      command: '', transport: 'http', url: 'https://pkce.example.com/mcp',
      oauth: { clientId: 'pub-1' },
    });
    const status = mgr.listAllServers().find(s => s.name === 'pkce');
    assert.ok(status?.oauth);
    assert.equal(status.oauth.hasClientSecret, false);
  });

  it('omits the oauth field entirely for a server with no oauth config', () => {
    mgr.addPendingServer('plain', { command: 'node' });
    const status = mgr.listAllServers().find(s => s.name === 'plain');
    assert.ok(status);
    assert.ok(!('oauth' in status) || status.oauth === undefined);
  });

  it('reports loggedIn=true once a persisted token file exists', () => {
    const cfg: McpServerConfig = {
      command: '', transport: 'http', url: 'https://tok.example.com/mcp',
      oauth: { clientId: 'cid' },
    };
    mgr.addPendingServer('tokd', cfg);
    // Materialize the exact token file the manager looks for.
    const key = mcpCredentialKey('tokd', { type: cfg.transport, url: cfg.url, headers: cfg.headers });
    const tokDir = path.join(tmp, 'mcp-tokens');
    fs.mkdirSync(tokDir, { recursive: true });
    fs.writeFileSync(path.join(tokDir, mcpTokenFileName(key)), JSON.stringify({ accessToken: 'x' }));
    const status = mgr.listAllServers().find(s => s.name === 'tokd');
    assert.ok(status?.oauth);
    assert.equal(status.oauth.loggedIn, true);
  });
});
