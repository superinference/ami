// U010 (w91) — raw-key intent detection on invalid config files.
//
// Parity: Claude Code's `rawSettingsContainsKey` (utils/settings/settings.ts) +
// the InvalidSettingsDialog fail-safe surfacing. When a config file fails to parse
// (or is a non-object), AMI previously returned a diagnostic that said the file was
// skipped but NOT which security/behaviour-bearing keys the user had tried to set —
// so a broken `~/.ami/config.json` carrying e.g. `cleanupPeriodDays` or `safeMode`
// would silently lose those settings and "fail open" unannounced. These tests lock
// in `rawConfigContainsKey` / `detectIntendedConfigKeys` and that a parse-error /
// not-object diagnostic reports the intended (recognized) keys it was trying to set.

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  rawConfigContainsKey,
  detectIntendedConfigKeys,
  loadConfigFileChecked,
  collectConfigDiagnostics,
} from '../src/config';

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-intent-'));
  tmpDirs.push(d);
  return d;
}
afterEach(() => {
  while (tmpDirs.length) {
    const d = tmpDirs.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

describe('rawConfigContainsKey', () => {
  it('detects a top-level key in a VALID config object', () => {
    assert.equal(rawConfigContainsKey('{"model":"opus","safeMode":true}', 'model'), true);
    assert.equal(rawConfigContainsKey('{"model":"opus"}', 'safeMode'), false);
  });

  it('detects a key even when the file FAILS to parse (user intent survives)', () => {
    // Trailing garbage makes this unparseable, but intent is still visible.
    const broken = '{ "cleanupPeriodDays": 30, "safeMode": true,,, oops';
    assert.equal(rawConfigContainsKey(broken, 'cleanupPeriodDays'), true);
    assert.equal(rawConfigContainsKey(broken, 'safeMode'), true);
    assert.equal(rawConfigContainsKey(broken, 'model'), false);
  });

  it('does NOT match a key that only appears inside a string VALUE', () => {
    const raw = '{ "note": "remember to set cleanupPeriodDays later" }';
    assert.equal(rawConfigContainsKey(raw, 'cleanupPeriodDays'), false);
    assert.equal(rawConfigContainsKey(raw, 'note'), true);
  });

  it('is false for empty / whitespace input', () => {
    assert.equal(rawConfigContainsKey('', 'model'), false);
    assert.equal(rawConfigContainsKey('   \n', 'model'), false);
  });
});

describe('detectIntendedConfigKeys', () => {
  it('returns only RECOGNIZED keys present in a broken file, sorted', () => {
    const broken = '{ "safeMode": true, "cleanupPeriodDays": 10, "totallyMadeUp": 1,,,';
    const keys = detectIntendedConfigKeys(broken);
    assert.deepEqual(keys, ['cleanupPeriodDays', 'safeMode']);
    assert.ok(!keys.includes('totallyMadeUp'));
  });

  it('is empty for a file that sets no recognized keys', () => {
    assert.deepEqual(detectIntendedConfigKeys('{ "foo": 1, "bar": 2 ,,,'), []);
  });
});

describe('loadConfigFileChecked attaches intendedKeys', () => {
  it('reports intended recognized keys on a PARSE-ERROR diagnostic', () => {
    const dir = mkTmp();
    const file = path.join(dir, 'config.json');
    fs.writeFileSync(file, '{ "safeMode": true, "maxTurns": 5,,, broken', 'utf-8');
    const { config, diagnostic } = loadConfigFileChecked(file);
    assert.equal(config, null);
    assert.equal(diagnostic?.kind, 'parse-error');
    assert.deepEqual(diagnostic?.intendedKeys, ['maxTurns', 'safeMode']);
  });

  it('reports intended recognized keys on a NOT-OBJECT diagnostic (array root)', () => {
    const dir = mkTmp();
    const file = path.join(dir, 'config.json');
    fs.writeFileSync(file, '[ { "model": "opus" } ]', 'utf-8');
    const { config, diagnostic } = loadConfigFileChecked(file);
    assert.equal(config, null);
    assert.equal(diagnostic?.kind, 'not-object');
    assert.deepEqual(diagnostic?.intendedKeys, ['model']);
  });

  it('omits intendedKeys when a broken file sets no recognized key', () => {
    const dir = mkTmp();
    const file = path.join(dir, 'config.json');
    fs.writeFileSync(file, '{ "foo": 1,,, broken', 'utf-8');
    const { diagnostic } = loadConfigFileChecked(file);
    assert.equal(diagnostic?.kind, 'parse-error');
    assert.equal(diagnostic?.intendedKeys, undefined);
  });
});

describe('collectConfigDiagnostics propagates intendedKeys', () => {
  it('surfaces the intended keys of a broken PROJECT config', () => {
    const dir = mkTmp();
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), '{ "safeMode": true,,, broken', 'utf-8');
    const diags = collectConfigDiagnostics(dir);
    const projectDiag = diags.find(d => d.source === 'project');
    assert.ok(projectDiag, 'expected a project diagnostic');
    assert.deepEqual(projectDiag?.intendedKeys, ['safeMode']);
  });
});
