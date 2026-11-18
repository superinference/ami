/**
 * U001 (w116) — McpManager.logout: clear a server's stored OAuth login WITHOUT
 * forgetting the server (parity: Claude's "Clear Auth" / revokeServerTokens).
 *
 * Unlike removeServer (forgets the config), logout keeps the server listed but
 * clears its credentials: best-effort server-side revoke via a live client + purge
 * the persisted token file + drop the needs-auth cache, so the next use re-runs the
 * OAuth consent flow.
 *
 * RED on revert: before `logout` existed the call throws `logout is not a function`.
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { McpManager } from '../src/mcp/manager';
import type { McpServerConfig } from '../src/mcp/manager';
import { mcpCredentialKey, mcpTokenFileName } from '../src/mcp/oauth';
import { resolveConfigDir } from '../src/reference-journal';

const SERVER = 'https://rs.example/mcp';
const oauthHttpConfig = (): McpServerConfig => ({ command: '', transport: 'http', url: SERVER, oauth: {} });

function tokenPathFor(name: string, cfg: McpServerConfig): string {
  const key = mcpCredentialKey(name, { type: cfg.transport, url: cfg.url, headers: cfg.headers });
  return path.join(resolveConfigDir(), 'mcp-tokens', mcpTokenFileName(key));
}

describe('McpManager.logout — clear stored OAuth login, keep the server', () => {
  let tmp: string;
  let prev: string | undefined;

  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u001-w116-logout-'));
    prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
  });
  after(() => {
    if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prev;
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('purges the persisted token file and reports hadToken, keeping the server listed', async () => {
    const mgr = new McpManager();
    const cfg = oauthHttpConfig();
    mgr.addPendingServer('srv', cfg);
    const tp = tokenPathFor('srv', cfg);
    fs.mkdirSync(path.dirname(tp), { recursive: true });
    fs.writeFileSync(tp, JSON.stringify({ accessToken: 'AT', tokenType: 'Bearer' }));

    const res = await mgr.logout('srv');
    assert.deepEqual(res, { loggedOut: true, hadToken: true, revoked: false });
    assert.equal(fs.existsSync(tp), false, 'token file purged');
    // The server is still known (not forgotten) — a second logout resolves rather
    // than rejecting with "Unknown MCP server".
    await assert.doesNotReject(() => mgr.logout('srv'));
  });

  it('reports hadToken:false when there is no stored credential', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('srv2', oauthHttpConfig());
    const res = await mgr.logout('srv2');
    assert.deepEqual(res, { loggedOut: true, hadToken: false, revoked: false });
  });

  it('best-effort server-side revoke via a live client sets revoked:true', async () => {
    const mgr = new McpManager();
    const cfg = oauthHttpConfig();
    mgr.addPendingServer('srv3', cfg);
    let revokeCalled = false;
    // Inject a fake live client exposing revokeAuth.
    (mgr as unknown as { clients: Map<string, unknown> }).clients.set('srv3', {
      revokeAuth: async () => { revokeCalled = true; },
    });
    const res = await mgr.logout('srv3');
    assert.equal(res.revoked, true);
    assert.equal(revokeCalled, true);
  });

  it('a failed server-side revoke does not block the local clear', async () => {
    const mgr = new McpManager();
    const cfg = oauthHttpConfig();
    mgr.addPendingServer('srv4', cfg);
    const tp = tokenPathFor('srv4', cfg);
    fs.mkdirSync(path.dirname(tp), { recursive: true });
    fs.writeFileSync(tp, '{}');
    (mgr as unknown as { clients: Map<string, unknown> }).clients.set('srv4', {
      revokeAuth: async () => { throw new Error('revoke endpoint down'); },
    });
    const res = await mgr.logout('srv4');
    assert.equal(res.loggedOut, true);
    assert.equal(res.hadToken, true);
    assert.equal(fs.existsSync(tp), false, 'token still purged despite revoke failure');
  });

  it('rejects an unknown server', async () => {
    const mgr = new McpManager();
    await assert.rejects(() => mgr.logout('nope'), /Unknown MCP server "nope"/);
  });

  it('rejects a non-OAuth (stdio) server — nothing to clear', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('local', { command: 'node', args: ['s.js'] });
    await assert.rejects(() => mgr.logout('local'), /not an OAuth http\/sse server/);
  });
});
