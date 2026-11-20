import { test } from 'node:test';
import * as assert from 'node:assert/strict';

import { validatePluginManifest } from '../src/plugins';

// ---------------------------------------------------------------------------
// U029 — corrective, example-bearing validation messages for a plugin manifest's
// inline `lspServers` declarations (the reference parity: LspServerConfigSchema +
// fileExtension refine, utils/plugins/schemas.ts L708-L736).
//
// AMI's validatePluginManifest previously treated an inline `lspServers` record as
// structurally opaque (validateServerDeclarations only path-checked string/array
// forms), so a bad inline LSP config — a command that carries its args inline, or a
// file-extension key missing its leading dot — slipped past `/plugin validate` with
// no signal. These tests lock the inline-record validation + Claude's actionable,
// example-bearing messages, which flow through the SINGLE canonical
// validateLspServerConfig (no second LSP-validation spelling).
// ---------------------------------------------------------------------------

test('inline lspServers: a command with spaces is flagged with the "args" array fix', () => {
  const r = validatePluginManifest({
    name: 'p',
    lspServers: { ts: { command: 'typescript-language-server --stdio' } },
  });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'lspServers.ts');
  assert.ok(e, 'expected an lspServers.ts error');
  assert.match(e!.message, /command should not contain spaces/);
  assert.match(e!.message, /"args" array/);
});

test('inline lspServers: a file-extension key missing its dot is flagged with a corrective example', () => {
  const r = validatePluginManifest({
    name: 'p',
    lspServers: { ruby: { command: 'solargraph', extensionToLanguage: { rb: 'ruby' } } },
  });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'lspServers.ruby');
  assert.ok(e, 'expected an lspServers.ruby error');
  assert.match(e!.message, /file extension "rb" must start with a dot/);
  assert.match(e!.message, /\.ts/);
});

test('inline lspServers: a well-formed config passes validation', () => {
  const r = validatePluginManifest({
    name: 'p',
    lspServers: { ruby: { command: 'solargraph', args: ['stdio'], extensionToLanguage: { '.rb': 'ruby' } } },
  });
  assert.equal(r.valid, true, JSON.stringify(r.errors));
});

test('lspServers as an ARRAY mixing a file path and an inline record validates the inline record', () => {
  const r = validatePluginManifest({
    name: 'p',
    lspServers: ['servers.lsp.json', { gopls: { command: 'gopls serve' } }],
  });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'lspServers[1].gopls');
  assert.ok(e, 'expected an lspServers[1].gopls error');
  assert.match(e!.message, /command should not contain spaces/);
});

test('lspServers declared as a bare file-path string is still path-traversal checked (no inline validation)', () => {
  const r = validatePluginManifest({ name: 'p', lspServers: '../escape.lsp.json' });
  assert.equal(r.valid, false);
  const e = r.errors.find(e => e.path === 'lspServers');
  assert.ok(e, 'expected an lspServers traversal error');
  assert.match(e!.message, /escape the plugin directory|must be relative|contains ".."/);
});
