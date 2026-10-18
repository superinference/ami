import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';

describe('McpManager allowlist enforcement', () => {
  let mgr: McpManager;

  beforeEach(() => {
    mgr = new McpManager();
  });

  it('allows all servers by default (null allowlist)', () => {
    assert.equal(mgr.isServerAllowed('anything'), true);
    assert.equal(mgr.isServerAllowed('random-server'), true);
  });

  it('isServerAllowed returns true for listed servers', () => {
    mgr.setAllowedServers(['alpha', 'beta']);
    assert.equal(mgr.isServerAllowed('alpha'), true);
    assert.equal(mgr.isServerAllowed('beta'), true);
  });

  it('isServerAllowed returns false for unlisted servers', () => {
    mgr.setAllowedServers(['alpha']);
    assert.equal(mgr.isServerAllowed('gamma'), false);
    assert.equal(mgr.isServerAllowed('delta'), false);
  });

  it('addServer throws for unlisted server', () => {
    mgr.setAllowedServers(['allowed-one']);
    assert.throws(
      () => mgr.addServer('blocked', { command: 'echo' }),
      { message: 'MCP server "blocked" is not in the allowed servers list' },
    );
  });

  it('addServer succeeds for listed server', () => {
    mgr.setAllowedServers(['my-server']);
    assert.doesNotThrow(() => mgr.addServer('my-server', { command: 'echo' }));
  });

  it('addPendingServer throws for unlisted server', () => {
    mgr.setAllowedServers(['allowed-one']);
    assert.throws(
      () => mgr.addPendingServer('blocked', { command: 'echo' }),
      { message: 'MCP server "blocked" is not in the allowed servers list' },
    );
  });

  it('addPendingServer succeeds for listed server', () => {
    mgr.setAllowedServers(['pending-ok']);
    assert.doesNotThrow(() => mgr.addPendingServer('pending-ok', { command: 'echo' }));
  });

  it('tracks denied servers', () => {
    mgr.setAllowedServers(['allowed']);
    try { mgr.addServer('denied-1', { command: 'a' }); } catch {}
    try { mgr.addServer('denied-2', { command: 'b' }); } catch {}
    const denied = mgr.getDeniedServers();
    assert.deepEqual(denied, ['denied-1', 'denied-2']);
  });

  it('does not duplicate denied server entries', () => {
    mgr.setAllowedServers(['allowed']);
    try { mgr.addServer('dup', { command: 'a' }); } catch {}
    try { mgr.addPendingServer('dup', { command: 'b' }); } catch {}
    assert.deepEqual(mgr.getDeniedServers(), ['dup']);
  });

  it('getDeniedServers returns a copy', () => {
    mgr.setAllowedServers(['allowed']);
    try { mgr.addServer('x', { command: 'a' }); } catch {}
    const copy = mgr.getDeniedServers();
    copy.push('injected');
    assert.deepEqual(mgr.getDeniedServers(), ['x']);
  });

  it('empty allowlist blocks all servers', () => {
    mgr.setAllowedServers([]);
    assert.equal(mgr.isServerAllowed('anything'), false);
    assert.throws(
      () => mgr.addServer('any', { command: 'echo' }),
      { message: 'MCP server "any" is not in the allowed servers list' },
    );
  });

  it('server names are case-sensitive', () => {
    mgr.setAllowedServers(['MyServer']);
    assert.equal(mgr.isServerAllowed('MyServer'), true);
    assert.equal(mgr.isServerAllowed('myserver'), false);
    assert.equal(mgr.isServerAllowed('MYSERVER'), false);
  });

  it('setting a new allowlist replaces the old one', () => {
    mgr.setAllowedServers(['alpha', 'beta']);
    assert.equal(mgr.isServerAllowed('alpha'), true);
    assert.equal(mgr.isServerAllowed('beta'), true);

    mgr.setAllowedServers(['gamma']);
    assert.equal(mgr.isServerAllowed('alpha'), false);
    assert.equal(mgr.isServerAllowed('beta'), false);
    assert.equal(mgr.isServerAllowed('gamma'), true);
  });

  it('setting new allowlist clears denied list', () => {
    mgr.setAllowedServers(['a']);
    try { mgr.addServer('bad', { command: 'x' }); } catch {}
    assert.deepEqual(mgr.getDeniedServers(), ['bad']);

    mgr.setAllowedServers(['b']);
    assert.deepEqual(mgr.getDeniedServers(), []);
  });

  it('disconnectAll clears denied list', () => {
    mgr.setAllowedServers(['a']);
    try { mgr.addServer('nope', { command: 'x' }); } catch {}
    assert.ok(mgr.getDeniedServers().length > 0);

    mgr.disconnectAll();
    assert.deepEqual(mgr.getDeniedServers(), []);
  });

  it('without allowlist, getDeniedServers returns empty', () => {
    assert.deepEqual(mgr.getDeniedServers(), []);
  });
});
