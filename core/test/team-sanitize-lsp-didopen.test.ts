import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { teamCreateTool, teamDeleteTool, resetTeam } from '../src/tools/team';
import { LSPClient } from '../src/lsp/client';

// ---------------------------------------------------------------------------
// team_create — sanitizeTeamName path-traversal prevention
// ---------------------------------------------------------------------------
describe('teamCreateTool — team name sanitization', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetTeam();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-team-sanitize-'));
  });

  afterEach(() => {
    resetTeam();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  const ctx = () => ({
    cwd: tmpDir,
    sessionId: 'test',
    abortSignal: new AbortController().signal,
    config: {} as any,
  });

  it('accepts a valid alphanumeric team name', async () => {
    const result = await teamCreateTool.execute({ team_name: 'alpha-team_1' }, ctx());
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes('alpha-team_1'));
    const teamDir = path.join(tmpDir, '.ami', 'teams', 'alpha-team_1');
    assert.ok(fs.existsSync(teamDir));
  });

  it('rejects team name with path traversal (../)', async () => {
    const result = await teamCreateTool.execute({ team_name: '../../../etc/evil' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('alphanumeric'));
    const evilDir = path.join(tmpDir, '..', '..', '..', 'etc', 'evil');
    assert.ok(!fs.existsSync(evilDir));
  });

  it('rejects team name with slashes', async () => {
    const result = await teamCreateTool.execute({ team_name: 'foo/bar' }, ctx());
    assert.equal(result.isError, true);
  });

  it('rejects team name with dots', async () => {
    const result = await teamCreateTool.execute({ team_name: 'a.b' }, ctx());
    assert.equal(result.isError, true);
  });

  it('rejects team name with spaces', async () => {
    const result = await teamCreateTool.execute({ team_name: 'my team' }, ctx());
    assert.equal(result.isError, true);
  });

  it('rejects empty team name', async () => {
    const result = await teamCreateTool.execute({ team_name: '' }, ctx());
    assert.equal(result.isError, true);
  });

  it('rejects team name that is only special characters', async () => {
    const result = await teamCreateTool.execute({ team_name: '...' }, ctx());
    assert.equal(result.isError, true);
  });

  it('allows hyphens and underscores', async () => {
    const result = await teamCreateTool.execute({ team_name: 'my_team-v2' }, ctx());
    assert.equal(result.isError, undefined);
  });
});

// ---------------------------------------------------------------------------
// LSPClient — ensureDidOpen before didChange/didSave
// ---------------------------------------------------------------------------
describe('LSPClient — didOpen tracking', () => {
  let client: LSPClient;
  const notifications: Array<{ method: string; params: any }> = [];

  // Build a fully-initialized ServerConn around a fake process so `ensureServer`
  // short-circuits (it returns true when `servers` already has the language) —
  // no spawn / handshake needed for these document-sync unit tests.
  const seedServer = (lang: string) => {
    const fakeProc = { stdin: { write: () => {} }, kill: () => {}, on: () => {} } as any;
    (client as any).servers.set(lang, {
      proc: fakeProc,
      buffer: Buffer.alloc(0),
      nextId: 0,
      pending: new Map(),
      capabilities: {},
      initialized: true,
    });
  };

  beforeEach(() => {
    client = new LSPClient();
    notifications.length = 0;

    // Capture outbound notifications (the new client's canonical low-level
    // notification sink is rawNotify(conn, method, params)).
    (client as any).rawNotify = (_conn: any, method: string, params: any) => {
      notifications.push({ method, params });
    };

    seedServer('typescript');
  });

  afterEach(() => {
    client.shutdown();
  });

  it('sends didOpen before first didChange for a file', async () => {
    await client.notifyDidChange('/tmp/test.ts', 'const x = 1;', '/tmp');

    assert.equal(notifications.length, 2);
    assert.equal(notifications[0].method, 'textDocument/didOpen');
    assert.equal(notifications[0].params.textDocument.uri, 'file:///tmp/test.ts');
    assert.equal(notifications[0].params.textDocument.text, 'const x = 1;');
    assert.equal(notifications[1].method, 'textDocument/didChange');
  });

  it('sends didOpen before first didSave for a file', async () => {
    await client.notifyDidSave('/tmp/test.ts', '/tmp');

    assert.equal(notifications.length, 2);
    assert.equal(notifications[0].method, 'textDocument/didOpen');
    assert.equal(notifications[1].method, 'textDocument/didSave');
  });

  it('does not send duplicate didOpen for the same file', async () => {
    await client.notifyDidChange('/tmp/test.ts', 'v1', '/tmp');
    await client.notifyDidChange('/tmp/test.ts', 'v2', '/tmp');

    const didOpens = notifications.filter(n => n.method === 'textDocument/didOpen');
    assert.equal(didOpens.length, 1);
    assert.equal(notifications.length, 3); // 1 didOpen + 2 didChange
  });

  it('sends separate didOpen for different files', async () => {
    await client.notifyDidChange('/tmp/a.ts', 'a', '/tmp');
    await client.notifyDidChange('/tmp/b.ts', 'b', '/tmp');

    const didOpens = notifications.filter(n => n.method === 'textDocument/didOpen');
    assert.equal(didOpens.length, 2);
    assert.equal(didOpens[0].params.textDocument.uri, 'file:///tmp/a.ts');
    assert.equal(didOpens[1].params.textDocument.uri, 'file:///tmp/b.ts');
  });

  it('didOpen includes correct languageId', async () => {
    await client.notifyDidChange('/tmp/test.ts', 'code', '/tmp');

    const open = notifications.find(n => n.method === 'textDocument/didOpen');
    assert.ok(open);
    assert.equal(open!.params.textDocument.languageId, 'typescript');
  });

  it('shutdown clears openedDocuments tracking', async () => {
    await client.notifyDidChange('/tmp/test.ts', 'code', '/tmp');
    assert.equal(notifications.length, 2);

    client.shutdown();
    notifications.length = 0;

    // Re-seed the server since shutdown cleared it
    seedServer('typescript');

    await client.notifyDidChange('/tmp/test.ts', 'code2', '/tmp');
    const didOpens = notifications.filter(n => n.method === 'textDocument/didOpen');
    assert.equal(didOpens.length, 1, 'should send didOpen again after shutdown');
  });

  it('skips non-supported file extensions', async () => {
    await client.notifyDidChange('/tmp/test.txt', 'hello', '/tmp');
    assert.equal(notifications.length, 0);
  });

  it('process crash clears openedDocuments for that language', async () => {
    await client.notifyDidChange('/tmp/test.ts', 'code', '/tmp');
    assert.equal(notifications.length, 2); // didOpen + didChange

    // Simulate a server crash: clearServer runs the per-language opened-document
    // cleanup and drops the connection (what the proc 'exit'/'error' handler does).
    (client as any).clearServer('typescript');
    notifications.length = 0;

    // Re-seed the server (simulates server restart)
    seedServer('typescript');

    await client.notifyDidChange('/tmp/test.ts', 'code2', '/tmp');
    const didOpens = notifications.filter(n => n.method === 'textDocument/didOpen');
    assert.equal(didOpens.length, 1, 'should re-send didOpen after process crash');
  });

  it('process crash only clears documents for the crashed language', async () => {
    // Seed a python server too
    seedServer('python');

    await client.notifyDidChange('/tmp/test.ts', 'ts code', '/tmp');
    await client.notifyDidChange('/tmp/test.py', 'py code', '/tmp');
    notifications.length = 0;

    // Crash only typescript (clears its opened docs + connection; python untouched)
    (client as any).clearServer('typescript');

    // Re-seed typescript
    seedServer('typescript');

    await client.notifyDidChange('/tmp/test.ts', 'ts2', '/tmp');
    await client.notifyDidChange('/tmp/test.py', 'py2', '/tmp');

    const didOpens = notifications.filter(n => n.method === 'textDocument/didOpen');
    assert.equal(didOpens.length, 1, 'only typescript should get new didOpen');
    assert.equal(didOpens[0].params.textDocument.uri, 'file:///tmp/test.ts');
  });
});
