import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  DEFAULT_LSP_SERVERS,
  getAllLspServers,
  registerLspServers,
  resetLspServers,
  resolveLanguageForExtension,
  languageIdForExtension,
  assertNoAutoFetch,
  validateLspServerConfig,
  loadLspServersFromFile,
} from '../src/lsp/config';

afterEach(() => resetLspServers());

describe('lsp config registry', () => {
  it('ships built-in defaults for the four seeded languages', () => {
    assert.ok(DEFAULT_LSP_SERVERS.typescript);
    assert.ok(DEFAULT_LSP_SERVERS.python);
    assert.ok(DEFAULT_LSP_SERVERS.go);
    assert.ok(DEFAULT_LSP_SERVERS.rust);
  });

  it('merges registered servers over defaults (later wins)', () => {
    registerLspServers({ ruby: { command: 'solargraph', args: ['stdio'], extensionToLanguage: { '.rb': 'ruby' } } });
    registerLspServers({ typescript: { command: 'my-ts-server', extensionToLanguage: { '.ts': 'typescript' } } });
    const all = getAllLspServers();
    assert.equal(all.ruby.command, 'solargraph');
    assert.equal(all.typescript.command, 'my-ts-server'); // override wins
  });

  it('resets registered servers but keeps defaults', () => {
    registerLspServers({ ruby: { command: 'solargraph' } });
    resetLspServers();
    assert.equal(getAllLspServers().ruby, undefined);
    assert.ok(getAllLspServers().typescript);
  });

  it('resolves a language key by extension (case-insensitive, with dot)', () => {
    assert.equal(resolveLanguageForExtension('.ts'), 'typescript');
    assert.equal(resolveLanguageForExtension('.TSX'), 'typescript');
    assert.equal(resolveLanguageForExtension('.rs'), 'rust');
    assert.equal(resolveLanguageForExtension('.unknownext'), null);
  });

  it('maps extension to the LSP languageId', () => {
    assert.equal(languageIdForExtension('.tsx'), 'typescriptreact');
    assert.equal(languageIdForExtension('.js'), 'javascript');
    assert.equal(languageIdForExtension('.py'), 'python');
    assert.equal(languageIdForExtension('.nope'), 'plaintext');
  });
});

describe('supply-chain guard (assertNoAutoFetch)', () => {
  it('allows the default typescript server because it uses --no-install', () => {
    assert.doesNotThrow(() => assertNoAutoFetch(DEFAULT_LSP_SERVERS.typescript));
  });

  it('allows non-npx commands', () => {
    assert.doesNotThrow(() => assertNoAutoFetch({ command: 'gopls', args: ['serve'] }));
  });

  it('rejects a bare npx invocation that would auto-fetch', () => {
    assert.throws(
      () => assertNoAutoFetch({ command: 'npx', args: ['typescript-language-server', '--stdio'] }),
      /auto-fetch/,
    );
  });

  it('accepts --offline as an equivalent guard', () => {
    assert.doesNotThrow(() => assertNoAutoFetch({ command: 'npx', args: ['--offline', 'foo'] }));
  });

  it('detects npx via an absolute path', () => {
    assert.throws(() => assertNoAutoFetch({ command: '/usr/local/bin/npx', args: ['foo'] }), /auto-fetch/);
  });
});

describe('validateLspServerConfig', () => {
  it('accepts a full config', () => {
    const c = validateLspServerConfig('x', {
      command: 'srv',
      args: ['--stdio'],
      env: { A: '1' },
      workspaceFolder: '/w',
      initializationOptions: { k: 1 },
      startupTimeout: 1000,
      maxRestarts: 2,
      extensionToLanguage: { '.X': 'x' },
    });
    assert.equal(c.command, 'srv');
    assert.deepEqual(c.args, ['--stdio']);
    assert.equal(c.extensionToLanguage!['.x'], 'x'); // key lowercased
    assert.equal(c.startupTimeout, 1000);
  });

  it('rejects a non-object', () => {
    assert.throws(() => validateLspServerConfig('x', 'nope'), /must be an object/);
  });

  it('rejects a missing command', () => {
    assert.throws(() => validateLspServerConfig('x', {}), /command/);
  });

  it('rejects non-string args', () => {
    assert.throws(() => validateLspServerConfig('x', { command: 'c', args: [1, 2] }), /array of strings/);
  });

  it('accepts a command-only config (all optional fields absent)', () => {
    const c = validateLspServerConfig('x', { command: 'c' });
    assert.equal(c.command, 'c');
    assert.equal(c.args, undefined);
    assert.equal(c.env, undefined);
    assert.equal(c.workspaceFolder, undefined);
    assert.equal(c.initializationOptions, undefined);
    assert.equal(c.startupTimeout, undefined);
    assert.equal(c.maxRestarts, undefined);
    assert.equal(c.extensionToLanguage, undefined);
  });

  it('parses the full ScopedLspServerConfig knob set (transport/settings/timeouts/restartOnCrash)', () => {
    const c = validateLspServerConfig('x', {
      command: 'srv',
      transport: 'stdio',
      settings: { srv: { strict: true } },
      shutdownTimeout: 250,
      restartOnCrash: false,
    });
    assert.equal(c.transport, 'stdio');
    assert.deepEqual(c.settings, { srv: { strict: true } });
    assert.equal(c.shutdownTimeout, 250);
    assert.equal(c.restartOnCrash, false);
  });

  it('accepts the socket transport value (schema parity) even though runtime refuses it', () => {
    const c = validateLspServerConfig('x', { command: 'srv', transport: 'socket' });
    assert.equal(c.transport, 'socket');
  });

  it('rejects an unknown transport value', () => {
    assert.throws(() => validateLspServerConfig('x', { command: 'c', transport: 'ipc' }), /"transport" must be "stdio" or "socket"/);
  });

  it('ignores a non-boolean restartOnCrash and non-number shutdownTimeout', () => {
    const c = validateLspServerConfig('x', { command: 'c', restartOnCrash: 'yes', shutdownTimeout: '5s' });
    assert.equal(c.restartOnCrash, undefined);
    assert.equal(c.shutdownTimeout, undefined);
  });

  it('skips non-string entries in extensionToLanguage', () => {
    const c = validateLspServerConfig('x', {
      command: 'c',
      extensionToLanguage: { '.ok': 'lang', '.bad': 123 },
    });
    assert.equal(c.extensionToLanguage!['.ok'], 'lang');
    assert.equal('.bad' in c.extensionToLanguage!, false);
  });

  // Claude parity (LspServerConfigSchema.command refine): a command carrying its
  // arguments inline is rejected with a corrective, example-bearing message.
  it('rejects a command with spaces and names the "args" array as the fix', () => {
    assert.throws(
      () => validateLspServerConfig('ts', { command: 'typescript-language-server --stdio' }),
      /command should not contain spaces\. Use the "args" array/,
    );
  });

  it('accepts an absolute-path command that contains spaces (e.g. a macOS app bundle)', () => {
    const c = validateLspServerConfig('mac', { command: '/Applications/My App/bin/lsp' });
    assert.equal(c.command, '/Applications/My App/bin/lsp');
  });

  // Claude parity (schemas.ts fileExtension refine): an extension key must start
  // with a dot, with a corrective example in the message.
  it('rejects an extensionToLanguage key missing its leading dot', () => {
    assert.throws(
      () => validateLspServerConfig('x', { command: 'c', extensionToLanguage: { ts: 'typescript' } }),
      /file extension "ts" must start with a dot \(e\.g\., "\.ts", not "ts"\)/,
    );
  });

  it('rejects a single-character extension key (a bare dot)', () => {
    assert.throws(
      () => validateLspServerConfig('x', { command: 'c', extensionToLanguage: { '.': 'lang' } }),
      /must start with a dot/,
    );
  });
});

describe('loadLspServersFromFile', () => {
  let dir: string;
  afterEach(() => { if (dir) fs.rmSync(dir, { recursive: true, force: true }); });

  it('returns {} when the file is absent', () => {
    assert.deepEqual(loadLspServersFromFile('/no/such/file.lsp.json'), {});
  });

  it('parses and validates a .lsp.json map', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lspcfg-'));
    const f = path.join(dir, '.lsp.json');
    fs.writeFileSync(f, JSON.stringify({ ruby: { command: 'solargraph', args: ['stdio'] } }));
    const loaded = loadLspServersFromFile(f);
    assert.equal(loaded.ruby.command, 'solargraph');
  });

  it('throws on malformed JSON', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lspcfg-'));
    const f = path.join(dir, '.lsp.json');
    fs.writeFileSync(f, '{ not json');
    assert.throws(() => loadLspServersFromFile(f), /Invalid JSON/);
  });

  it('throws when the top level is not an object', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lspcfg-'));
    const f = path.join(dir, '.lsp.json');
    fs.writeFileSync(f, '[]');
    assert.throws(() => loadLspServersFromFile(f), /must be an object/);
  });
});
