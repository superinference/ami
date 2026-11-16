/**
 * Pre-flight project-settings trust scan + home-dir trust helper
 * (core/core/src/safe-mode.ts).
 *
 * Covers scanProjectSettingsForTrust() — the wiring that runs the per-capability
 * config detectors against a project's REAL settings files and derives the
 * TrustIndicators the trust gate consumes — and isHomeDirectory().
 */
import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  scanProjectSettingsForTrust,
  isHomeDirectory,
  PROJECT_SETTINGS_SOURCES,
} from '../src/safe-mode';

function mkProject(files: Record<string, unknown>): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'safe-mode-scan-'));
  for (const [rel, body] of Object.entries(files)) {
    const abs = path.join(dir, ...rel.split('/'));
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, typeof body === 'string' ? body : JSON.stringify(body));
  }
  return dir;
}

const cleanup: string[] = [];
function track(dir: string): string { cleanup.push(dir); return dir; }
process.on('exit', () => { for (const d of cleanup) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

test('scanProjectSettingsForTrust flags credential/command helpers as config escalation', () => {
  const dir = track(mkProject({ '.ami/settings.json': { apiKeyHelper: '/bin/get-key' } }));
  const res = scanProjectSettingsForTrust(dir);
  assert.equal(res.indicators.hasConfigEscalation, true);
  assert.ok(res.escalationVectors.includes('apiKeyHelper'));
  assert.ok(res.bySource['.ami/settings.json']);
  assert.deepEqual(res.bySource['.ami/settings.json'].commandHelpers, ['apiKeyHelper']);
});

test('scanProjectSettingsForTrust flags non-allowlisted env vars via env.<name> vectors', () => {
  const dir = track(mkProject({ '.claude/settings.json': { env: { LD_PRELOAD: '/evil.so', NODE_ENV: 'production' } } }));
  const res = scanProjectSettingsForTrust(dir);
  assert.equal(res.indicators.hasConfigEscalation, true);
  assert.ok(res.escalationVectors.includes('env.LD_PRELOAD'));
  assert.ok(!res.escalationVectors.includes('env.NODE_ENV'), 'NODE_ENV is an allowlisted safe env var');
});

test('scanProjectSettingsForTrust surfaces MCP servers as untrusted but not escalation', () => {
  const dir = track(mkProject({ '.ami/settings.local.json': { mcpServers: { foo: { command: 'x' } } } }));
  const res = scanProjectSettingsForTrust(dir);
  assert.equal(res.indicators.hasUntrustedMcp, true);
  assert.equal(res.indicators.hasConfigEscalation, false);
});

test('scanProjectSettingsForTrust surfaces hooks per-source without forcing escalation', () => {
  const dir = track(mkProject({ '.ami/settings.json': { hooks: { preToolUse: [{}] } } }));
  const res = scanProjectSettingsForTrust(dir);
  assert.equal(res.bySource['.ami/settings.json'].hooks, true);
  assert.equal(res.indicators.hasConfigEscalation, false);
  assert.equal(res.indicators.hasUntrustedMcp, false);
});

test('scanProjectSettingsForTrust merges escalation vectors across all four sources (dedup)', () => {
  const dir = track(mkProject({
    '.ami/settings.json': { apiKeyHelper: 'a' },
    '.ami/settings.local.json': { apiKeyHelper: 'b', otelHeadersHelper: 'c' },
  }));
  const res = scanProjectSettingsForTrust(dir);
  assert.deepEqual([...res.escalationVectors].sort(), ['apiKeyHelper', 'otelHeadersHelper']);
});

test('scanProjectSettingsForTrust ignores missing / malformed / non-object settings', () => {
  const empty = track(fs.mkdtempSync(path.join(os.tmpdir(), 'safe-mode-scan-')));
  const resEmpty = scanProjectSettingsForTrust(empty);
  assert.equal(resEmpty.indicators.hasConfigEscalation, false);
  assert.equal(resEmpty.indicators.hasUntrustedMcp, false);
  assert.deepEqual(resEmpty.bySource, {});

  const bad = track(mkProject({ '.ami/settings.json': '{ not json', '.claude/settings.json': '["array"]' }));
  const resBad = scanProjectSettingsForTrust(bad);
  assert.equal(resBad.indicators.hasConfigEscalation, false);
  assert.deepEqual(resBad.bySource, {});
});

test('scanProjectSettingsForTrust with empty cwd returns empty result', () => {
  const res = scanProjectSettingsForTrust('');
  assert.deepEqual(res.bySource, {});
  assert.deepEqual(res.escalationVectors, []);
  assert.equal(res.indicators.hasConfigEscalation, false);
});

test('PROJECT_SETTINGS_SOURCES covers both .ami and .claude project + local settings', () => {
  assert.deepEqual([...PROJECT_SETTINGS_SOURCES], [
    '.ami/settings.json',
    '.ami/settings.local.json',
    '.claude/settings.json',
    '.claude/settings.local.json',
  ]);
});

test('isHomeDirectory is true only for the resolved home directory', () => {
  assert.equal(isHomeDirectory(os.homedir()), true);
  assert.equal(isHomeDirectory(path.join(os.homedir(), 'sub')), false);
  assert.equal(isHomeDirectory(''), false);
  // A relative path that does not resolve to home.
  assert.equal(isHomeDirectory('.'), path.resolve('.') === os.homedir());
});
