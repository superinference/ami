// U010 — resetConfigToDefault self-heal (parity: Claude Code's InvalidConfigDialog
// "Reset with default configuration" branch). Verifies that a corrupt/present config
// file is QUARANTINED (bytes preserved) and replaced with the default config, and that
// the absent-file case writes a default without a spurious backup.

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  resetConfigToDefault,
  DEFAULT_PROJECT_CONFIG,
  loadConfigFileChecked,
} from '../src/config';

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-reset-'));
  tmpDirs.push(d);
  return d;
}

afterEach(() => {
  while (tmpDirs.length) {
    const d = tmpDirs.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

describe('resetConfigToDefault', () => {
  it('DEFAULT_PROJECT_CONFIG is an empty (frozen) object', () => {
    assert.deepEqual(DEFAULT_PROJECT_CONFIG, {});
    assert.ok(Object.isFrozen(DEFAULT_PROJECT_CONFIG));
  });

  it('quarantines a CORRUPT config and writes a parseable default in its place', () => {
    const dir = mkTmp();
    const file = path.join(dir, 'config.json');
    const corrupt = '{ "model": "gpt-4o",,, oops not json';
    fs.writeFileSync(file, corrupt, 'utf-8');

    const result = resetConfigToDefault(file);

    assert.equal(result.path, file);
    assert.equal(result.hadExisting, true);
    assert.ok(result.backupPath, 'the corrupt bytes must be quarantined');
    // The quarantine holds the ORIGINAL (broken) bytes verbatim — nothing is lost.
    assert.equal(fs.readFileSync(result.backupPath!, 'utf-8'), corrupt);
    // The file now parses as the default (empty) config.
    const checked = loadConfigFileChecked(file);
    assert.equal(checked.diagnostic, undefined);
    assert.deepEqual(checked.config, {});
  });

  it('backs up a VALID prior config before overwriting it with the default', () => {
    const dir = mkTmp();
    const file = path.join(dir, 'config.json');
    fs.writeFileSync(file, JSON.stringify({ model: 'gpt-4o', maxTurns: 5 }), 'utf-8');

    const result = resetConfigToDefault(file);

    assert.equal(result.hadExisting, true);
    assert.ok(result.backupPath);
    assert.match(fs.readFileSync(result.backupPath!, 'utf-8'), /gpt-4o/);
    assert.deepEqual(loadConfigFileChecked(file).config, {});
  });

  it('writes a default with NO backup when the file is absent', () => {
    const dir = mkTmp();
    const file = path.join(dir, '.ami', 'config.json'); // nested dir does not exist yet

    const result = resetConfigToDefault(file);

    assert.equal(result.hadExisting, false);
    assert.equal(result.backupPath, null);
    assert.ok(fs.existsSync(file), 'the default config file is created (dirs made)');
    assert.deepEqual(loadConfigFileChecked(file).config, {});
  });

  it('is idempotent — resetting an already-default file leaves a parseable default', () => {
    const dir = mkTmp();
    const file = path.join(dir, 'config.json');
    resetConfigToDefault(file);
    const second = resetConfigToDefault(file);
    assert.equal(second.hadExisting, true);
    assert.deepEqual(loadConfigFileChecked(file).config, {});
  });
});
