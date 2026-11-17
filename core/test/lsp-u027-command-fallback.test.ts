/**
 * U027 parity: ordered-candidate binary fallback for an LSP server command
 * (Claude Code's alternate-binary fallback chains, e.g. uvx→pipx→pip→pip3).
 *
 * Covers:
 *  - firstAvailableBinary — returns the first installed candidate in preference
 *    order, honours an injected probe, and is undefined when none resolve;
 *  - validateLspServerConfig — accepts a valid `commandAlternatives`, rejects a
 *    non-array / empty-string entry;
 *  - LSPClient.ensureServer — spawns the FIRST installed candidate when the
 *    primary `command` is absent but a `commandAlternatives` entry is present, and
 *    reports every candidate when none are installed.
 */
import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { firstAvailableBinary } from '../src/utils/which';
import { LSPClient } from '../src/lsp/client';
import { registerLspServers, resetLspServers, validateLspServerConfig } from '../src/lsp/config';

// A minimal LSP server: answers `initialize`, ignores everything else. Enough to
// prove the client launched the right binary and completed the handshake.
const MINI_SERVER = String.raw`
let buf = Buffer.alloc(0);
function send(msg){ const b = JSON.stringify(msg); process.stdout.write('Content-Length: '+Buffer.byteLength(b)+'\r\n\r\n'+b); }
process.stdin.on('data', (c) => {
  buf = Buffer.concat([buf, c]);
  for(;;){
    const i = buf.indexOf('\r\n\r\n'); if(i===-1) return;
    const len = Number(/Content-Length:\s*(\d+)/i.exec(buf.subarray(0,i).toString())[1]);
    if(buf.length < i+4+len) return;
    const body = buf.subarray(i+4, i+4+len).toString(); buf = buf.subarray(i+4+len);
    const msg = JSON.parse(body);
    if(msg.method==='initialize') send({ jsonrpc:'2.0', id: msg.id, result: { capabilities: {} } });
  }
});
`;

let dir: string;
let serverScript: string;

before(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lsp-cmdfallback-'));
  serverScript = path.join(dir, 'mini-server.cjs');
  fs.writeFileSync(serverScript, MINI_SERVER);
});

after(() => {
  resetLspServers();
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
});

describe('firstAvailableBinary', () => {
  it('returns the first candidate the probe marks installed, in order', () => {
    const installed = new Set(['pip', 'pip3']);
    assert.equal(
      firstAvailableBinary(['uv', 'pipx', 'pip', 'pip3'], (c) => installed.has(c)),
      'pip',
    );
  });

  it('returns undefined when no candidate is installed', () => {
    assert.equal(firstAvailableBinary(['a', 'b', 'c'], () => false), undefined);
  });

  it('short-circuits on the very first installed candidate', () => {
    const probed: string[] = [];
    const chosen = firstAvailableBinary(['first', 'second'], (c) => { probed.push(c); return c === 'first'; });
    assert.equal(chosen, 'first');
    assert.deepEqual(probed, ['first']); // never probes 'second'
  });
});

describe('validateLspServerConfig: commandAlternatives', () => {
  it('accepts a valid string array', () => {
    const cfg = validateLspServerConfig('x', { command: 'pyright-langserver', commandAlternatives: ['pyright'] });
    assert.deepEqual(cfg.commandAlternatives, ['pyright']);
  });

  it('omits the field when absent', () => {
    const cfg = validateLspServerConfig('x', { command: 'gopls' });
    assert.equal(cfg.commandAlternatives, undefined);
  });

  it('rejects a non-array commandAlternatives', () => {
    assert.throws(
      () => validateLspServerConfig('x', { command: 'gopls', commandAlternatives: 'pyright' }),
      /commandAlternatives.*array of non-empty strings/,
    );
  });

  it('rejects an empty-string entry', () => {
    assert.throws(
      () => validateLspServerConfig('x', { command: 'gopls', commandAlternatives: [''] }),
      /commandAlternatives.*array of non-empty strings/,
    );
  });
});

describe('LSPClient.ensureServer: commandAlternatives fallback', () => {
  let client: LSPClient;
  afterEach(() => client?.shutdown());

  it('spawns the first installed alternative when the primary command is absent', async () => {
    registerLspServers({
      fblang: {
        command: 'definitely-not-installed-xyz',
        commandAlternatives: [process.execPath],
        args: [serverScript],
        extensionToLanguage: { '.fb': 'fblang' },
      },
    });
    client = new LSPClient();
    client._sleep = async () => {};
    // Primary absent, alternative (the real node binary) present.
    client._isBinaryInstalled = (c) => c === process.execPath;
    const spawned: string[] = [];
    const realSpawn = client._spawn;
    client._spawn = ((cmd: string, ...rest: unknown[]) => {
      spawned.push(cmd);
      return (realSpawn as (...a: unknown[]) => unknown)(cmd, ...rest);
    }) as typeof client._spawn;

    const ok = await client.ensureServer('fblang', dir);
    assert.equal(ok, true);
    assert.equal(client.isInitialized('fblang'), true);
    // It must have spawned the resolved ALTERNATIVE, not the absent primary.
    assert.deepEqual(spawned, [process.execPath]);
  });

  it('fails and lists every candidate when none are installed', async () => {
    registerLspServers({
      gonelang: {
        command: 'primary-gone',
        commandAlternatives: ['alt-gone'],
        args: [serverScript],
        extensionToLanguage: { '.gone': 'gonelang' },
      },
    });
    client = new LSPClient();
    client._isBinaryInstalled = () => false;
    let spawnedAny = false;
    client._spawn = (() => { spawnedAny = true; throw new Error('must not spawn'); }) as unknown as typeof client._spawn;

    const ok = await client.ensureServer('gonelang', dir);
    assert.equal(ok, false);
    assert.equal(spawnedAny, false);
    const status = client.getServerStatus('gonelang');
    assert.match(status?.lastError ?? '', /command not found: primary-gone, alt-gone/);
  });
});
