import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { FileOAuthTokenStore, mcpTokenFileName } from '../src/mcp/oauth';

// ---------------------------------------------------------------------------
// H22/H45: OAuth tokens persist to disk so a server stays authenticated across
// process restarts.
// ---------------------------------------------------------------------------

let dir: string;
before(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-tok-')); });
after(() => { fs.rmSync(dir, { recursive: true, force: true }); });

const NOW = 2_000_000_000_000;

describe('FileOAuthTokenStore (H22/H45)', () => {
  it('persists tokens to disk and a fresh store reloads them', () => {
    const file = path.join(dir, 'sub', 'srv.json');
    const a = new FileOAuthTokenStore(file);
    a.set({ accessToken: 'AT', tokenType: 'Bearer', refreshToken: 'RT', expiresAt: NOW + 3600_000, scope: 'read' });
    assert.ok(fs.existsSync(file), 'token file written');

    // A new store (simulating a restart) reloads the persisted token.
    const b = new FileOAuthTokenStore(file);
    assert.equal(b.authorizationHeader(NOW), 'Bearer AT');
    assert.equal(b.hasRefreshToken(), true);
  });

  it('writes the token file with owner-only permissions', () => {
    const file = path.join(dir, 'perm.json');
    const s = new FileOAuthTokenStore(file);
    s.set({ accessToken: 'AT', tokenType: 'Bearer' });
    const mode = fs.statSync(file).mode & 0o777;
    assert.equal(mode, 0o600);
  });

  it('clear() removes the persisted file', () => {
    const file = path.join(dir, 'clear.json');
    const s = new FileOAuthTokenStore(file);
    s.set({ accessToken: 'AT', tokenType: 'Bearer' });
    assert.ok(fs.existsSync(file));
    s.clear();
    assert.equal(fs.existsSync(file), false);
    assert.equal(s.get(), null);
  });

  it('starts empty when the file is absent or unreadable', () => {
    const s = new FileOAuthTokenStore(path.join(dir, 'does-not-exist.json'));
    assert.equal(s.get(), null);
    // A garbage file is tolerated, not thrown.
    const bad = path.join(dir, 'bad.json');
    fs.writeFileSync(bad, 'not json');
    assert.equal(new FileOAuthTokenStore(bad).get(), null);
  });

  it('mcpTokenFileName is filesystem-safe and stable per server', () => {
    const n = mcpTokenFileName('github/remote:prod');
    assert.doesNotMatch(n, /[/:]/);
    assert.ok(n.endsWith('.json'));
    assert.equal(mcpTokenFileName('github/remote:prod'), n, 'stable');
    assert.notEqual(mcpTokenFileName('other'), n, 'distinct per server');
  });
});
