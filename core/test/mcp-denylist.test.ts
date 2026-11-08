import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpManager, normalizeMcpUrl } from '../src/mcp/manager';

// S9: deniedMcpServers — block a server by name or by normalized upstream URL,
// default-open (empty list blocks nothing).
describe('McpManager denylist (S9)', () => {
  let mgr: McpManager;
  beforeEach(() => { mgr = new McpManager(); });

  it('default-open: nothing is denied when no denylist is set', () => {
    assert.equal(mgr.isServerDenied('anything'), false);
    assert.doesNotThrow(() => mgr.addServer('anything', { command: 'echo' }));
  });

  it('denies a server by name (case-insensitive)', () => {
    mgr.setDeniedServerList(['Evil-Server']);
    assert.equal(mgr.isServerDenied('evil-server'), true);
    assert.throws(
      () => mgr.addServer('evil-server', { command: 'echo' }),
      /blocked by the denied-servers list/,
    );
  });

  it('a name entry does not block an unrelated server', () => {
    mgr.setDeniedServerList(['evil-server']);
    assert.equal(mgr.isServerDenied('good-server'), false);
    assert.doesNotThrow(() => mgr.addServer('good-server', { command: 'echo' }));
  });

  it('denies by upstream URL — two differently-named servers at the same URL are both blocked', () => {
    mgr.setDeniedServerList(['https://evil.example.com/mcp']);
    const cfgA = { transport: 'http' as const, url: 'https://evil.example.com/mcp', command: '' };
    const cfgB = { transport: 'http' as const, url: 'https://EVIL.example.com/mcp/', command: '' };
    assert.equal(mgr.isServerDenied('alias-one', cfgA), true);
    assert.equal(mgr.isServerDenied('alias-two', cfgB), true); // host case + trailing slash normalized
    assert.throws(() => mgr.addServer('alias-one', cfgA), /blocked by the denied-servers list/);
    assert.throws(() => mgr.addServer('alias-two', cfgB), /blocked by the denied-servers list/);
  });

  it('a URL entry does not block a server at a different URL', () => {
    mgr.setDeniedServerList(['https://evil.example.com/mcp']);
    const ok = { transport: 'http' as const, url: 'https://good.example.com/mcp', command: '' };
    assert.equal(mgr.isServerDenied('ok', ok), false);
    assert.doesNotThrow(() => mgr.addServer('ok', ok));
  });

  it('setDeniedServerList replaces the previous list', () => {
    mgr.setDeniedServerList(['a']);
    mgr.setDeniedServerList(['b']);
    assert.equal(mgr.isServerDenied('a'), false);
    assert.equal(mgr.isServerDenied('b'), true);
  });

  it('ignores empty/non-string entries', () => {
    mgr.setDeniedServerList(['', null as unknown as string, 'real']);
    assert.equal(mgr.isServerDenied('real'), true);
    assert.equal(mgr.isServerDenied(''), false);
  });
});

describe('normalizeMcpUrl (S9)', () => {
  it('normalizes host case and strips trailing slashes', () => {
    assert.equal(normalizeMcpUrl('https://EXAMPLE.com/mcp/'), 'https://example.com/mcp');
    assert.equal(normalizeMcpUrl('https://example.com/mcp'), 'https://example.com/mcp');
  });
  it('returns null for a bare name', () => {
    assert.equal(normalizeMcpUrl('github-mcp'), null);
    assert.equal(normalizeMcpUrl('not a url'), null);
  });
});
