import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  safeWriteConfig,
  updateConfigFile,
  resolveConfigWriteTarget,
  backupConfigBeforeWrite,
  MAX_CONFIG_BACKUPS,
  CONFIG_BACKUP_MIN_INTERVAL_MS,
  stripBom,
  loadConfigFileChecked,
  loadManagedConfig,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// UNIT U010 (write safety) — core/core/src/config.ts
//   Gap D — symlink-target write-through: when the config path is a symlink
//           (dotfile-managed via stow/chezmoi), safeWriteConfig updates the
//           LINK TARGET in place instead of replacing the symlink with a plain
//           file. Parity: the reference's "if settings.json is a symlink, update the
//           target file instead".
//   Gap E — rolling timestamped backup-before-write with throttle + rotation:
//           the prior on-disk content is snapshotted to `<file>.backup-<ms>`
//           before being overwritten, throttled to one per
//           CONFIG_BACKUP_MIN_INTERVAL_MS and pruned to MAX_CONFIG_BACKUPS.
//           Parity: the reference's saveConfigWithLock timestamped-backup rotation.
// Each assertion discriminates the NEW behavior from the pre-fix behavior.
// ---------------------------------------------------------------------------

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-ws-'));
}

function listBackups(fp: string): string[] {
  const dir = path.dirname(fp);
  const base = path.basename(fp);
  return fs.readdirSync(dir).filter(e => e.startsWith(`${base}.backup-`));
}

// ===========================================================================
// Gap D — symlink-target write-through
// ===========================================================================
describe('U010 Gap D — symlink-target write-through', () => {
  let dir: string;
  beforeEach(() => { dir = mkTemp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('resolveConfigWriteTarget returns the path itself for a non-existent file', () => {
    const fp = path.join(dir, 'config.json');
    assert.equal(resolveConfigWriteTarget(fp), fp);
  });

  it('resolveConfigWriteTarget returns the path itself for a regular file', () => {
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(fp, '{}');
    assert.equal(resolveConfigWriteTarget(fp), fp);
  });

  it('resolveConfigWriteTarget resolves a symlink to its (absolute) target', () => {
    const target = path.join(dir, 'real-config.json');
    const link = path.join(dir, 'config.json');
    fs.writeFileSync(target, '{"version":1}');
    fs.symlinkSync('real-config.json', link); // relative link
    assert.equal(resolveConfigWriteTarget(link), target);
  });

  it('safeWriteConfig writes THROUGH a symlink, preserving the link', () => {
    const target = path.join(dir, 'real-config.json');
    const link = path.join(dir, 'config.json');
    fs.writeFileSync(target, JSON.stringify({ version: 1 }));
    fs.symlinkSync(target, link); // absolute link

    // RED (pre-fix): tmp+rename over `link` replaces the symlink with a regular
    // file, so lstat(link).isSymbolicLink() becomes false and `target` is stale.
    safeWriteConfig(link, { version: 2 });

    assert.ok(fs.lstatSync(link).isSymbolicLink(), 'symlink must be preserved');
    assert.deepEqual(JSON.parse(fs.readFileSync(target, 'utf-8')), { version: 2 });
    // Reading through the link sees the updated target.
    assert.deepEqual(JSON.parse(fs.readFileSync(link, 'utf-8')), { version: 2 });
    assert.ok(!fs.existsSync(target + '.tmp'));
  });
});

// ===========================================================================
// Gap E — rolling timestamped backup-before-write (throttle + rotation)
// ===========================================================================
describe('U010 Gap E — backup-before-write', () => {
  let dir: string;
  beforeEach(() => { dir = mkTemp(); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('exports the reference-parity rotation constants', () => {
    assert.equal(MAX_CONFIG_BACKUPS, 5);
    assert.equal(CONFIG_BACKUP_MIN_INTERVAL_MS, 60_000);
  });

  it('backupConfigBeforeWrite returns null when the file is absent (nothing to back up)', () => {
    const fp = path.join(dir, 'config.json');
    assert.equal(backupConfigBeforeWrite(fp), null);
    assert.equal(listBackups(fp).length, 0);
  });

  it('snapshots the prior good content before an overwrite', () => {
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { version: 1 }); // no prior file → no backup
    assert.equal(listBackups(fp).length, 0);

    safeWriteConfig(fp, { version: 2 }); // prior v1 exists → backed up
    const backups = listBackups(fp);
    assert.equal(backups.length, 1, 'one pre-write backup expected');
    const backed = JSON.parse(fs.readFileSync(path.join(dir, backups[0]!), 'utf-8'));
    assert.deepEqual(backed, { version: 1 }, 'backup holds the PRIOR content');
    // The live file is the new content.
    assert.deepEqual(JSON.parse(fs.readFileSync(fp, 'utf-8')), { version: 2 });
  });

  it('throttles: no second backup within CONFIG_BACKUP_MIN_INTERVAL_MS', () => {
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(fp, JSON.stringify({ v: 1 }));
    const t0 = 1_000_000;
    assert.ok(backupConfigBeforeWrite(fp, t0), 'first backup created');
    fs.writeFileSync(fp, JSON.stringify({ v: 2 }));
    // Within the throttle window → skipped.
    assert.equal(backupConfigBeforeWrite(fp, t0 + 100), null);
    assert.equal(listBackups(fp).length, 1);
    // Past the window → a new backup is made.
    assert.ok(backupConfigBeforeWrite(fp, t0 + CONFIG_BACKUP_MIN_INTERVAL_MS), 'backup after window');
    assert.equal(listBackups(fp).length, 2);
  });

  it('prunes to the MAX_CONFIG_BACKUPS most recent', () => {
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(fp, JSON.stringify({ v: 0 }));
    let t = 2_000_000;
    // Make 8 backups spaced beyond the throttle window.
    for (let i = 0; i < 8; i++) {
      fs.writeFileSync(fp, JSON.stringify({ v: i }));
      const made = backupConfigBeforeWrite(fp, t);
      assert.ok(made, `backup ${i} created`);
      t += CONFIG_BACKUP_MIN_INTERVAL_MS;
    }
    const backups = listBackups(fp);
    assert.equal(backups.length, MAX_CONFIG_BACKUPS, 'rotation keeps only the newest 5');
    // The newest (largest timestamp) must survive the prune.
    const timestamps = backups.map(b => Number(b.split('.backup-').pop())).sort((a, b) => a - b);
    assert.equal(timestamps[timestamps.length - 1], t - CONFIG_BACKUP_MIN_INTERVAL_MS);
  });

  it('wires through updateConfigFile (the real config-tool writer): a change backs up the prior state', () => {
    const fp = path.join(dir, 'config.json');
    safeWriteConfig(fp, { model: 'old' });
    // A functional update through the guarded writer the config-tool uses.
    const res = updateConfigFile(fp, (cur: ProjectConfig) => ({ ...cur, model: 'new' }));
    assert.equal(res.written, true);
    assert.equal(res.config.model, 'new');
    const backups = listBackups(fp);
    assert.equal(backups.length, 1);
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(dir, backups[0]!), 'utf-8')), { model: 'old' });
  });
});

// ===========================================================================
// Gap F — shared BOM strip across EVERY config-parse path (incl. managed)
// ===========================================================================
describe('U010 Gap F — UTF-8 BOM strip (user + managed parse paths)', () => {
  let dir: string;
  const savedManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  beforeEach(() => { dir = mkTemp(); });
  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
    if (savedManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = savedManaged;
  });

  it('stripBom removes a leading U+FEFF and is a no-op otherwise', () => {
    assert.equal(stripBom('﻿{"a":1}'), '{"a":1}');
    assert.equal(stripBom('{"a":1}'), '{"a":1}');
    assert.equal(stripBom(''), '');
  });

  it('user config.json with a BOM parses instead of misreporting as corrupt', () => {
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(fp, '﻿{"model":"opus"}', 'utf-8');
    const { config, diagnostic } = loadConfigFileChecked(fp);
    assert.equal(diagnostic, undefined, 'a BOM must not be flagged as a parse error');
    assert.equal(config?.model, 'opus');
  });

  it('managed config with a BOM parses (previously would fail-closed on the BOM)', () => {
    const managed = path.join(dir, 'managed.json');
    fs.writeFileSync(managed, '﻿{"enforceAvailableModels":["opus"]}', 'utf-8');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;
    // RED (pre-fix): parseManagedFile did not strip the BOM → ManagedConfigError.
    const cfg = loadManagedConfig();
    assert.deepEqual(cfg?.enforceAvailableModels, ['opus']);
  });
});
