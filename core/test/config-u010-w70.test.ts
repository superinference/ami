import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  backupCorruptConfig,
  findMostRecentConfigBackup,
  backupConfigBeforeWrite,
  loadConfigFileChecked,
  safeWriteConfig,
  getGlobalConfigWriteCount,
  getGlobalConfigPath,
  CONFIG_WRITE_DISPLAY_THRESHOLD,
} from '../src/config';

const tmpDirs: string[] = [];
function makeTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cfg-w70-'));
  tmpDirs.push(d);
  return d;
}
after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ---------------------------------------------------------------------------
// Corrupted-config quarantine content dedup (backupCorruptConfig)
// ---------------------------------------------------------------------------
describe('backupCorruptConfig content dedup', () => {
  it('returns an existing byte-identical quarantine instead of writing a new one', () => {
    const dir = makeTmp();
    const cfg = path.join(dir, 'config.json');
    const broken = '{ broken json';
    // A prior quarantine of the identical broken bytes already exists.
    const existing = `${cfg}.corrupt-2020-01-01T00-00-00-000Z`;
    fs.writeFileSync(existing, broken, 'utf-8');

    const result = backupCorruptConfig(cfg, broken);
    assert.equal(result, existing, 'should return the existing identical quarantine');

    const corrupts = fs.readdirSync(dir).filter(e => e.includes('.corrupt-'));
    assert.equal(corrupts.length, 1, 'must NOT write a second identical quarantine');
  });

  it('writes a fresh quarantine when the broken content differs', () => {
    const dir = makeTmp();
    const cfg = path.join(dir, 'config.json');
    fs.writeFileSync(`${cfg}.corrupt-2020-01-01T00-00-00-000Z`, '{ broken A', 'utf-8');

    const result = backupCorruptConfig(cfg, '{ broken B');
    assert.ok(result && result !== `${cfg}.corrupt-2020-01-01T00-00-00-000Z`);
    const corrupts = fs.readdirSync(dir).filter(e => e.includes('.corrupt-'));
    assert.equal(corrupts.length, 2, 'different content gets its own quarantine');
  });
});

// ---------------------------------------------------------------------------
// Restore hint on a corrupt-config diagnostic
// ---------------------------------------------------------------------------
describe('loadConfigFileChecked restore hint', () => {
  it('points at a cp of the most recent good backup when one exists', () => {
    const dir = makeTmp();
    const cfg = path.join(dir, 'config.json');
    // A known-good config exists; snapshot it to a `.backup-*` pre-write backup.
    fs.writeFileSync(cfg, JSON.stringify({ model: 'opus' }), 'utf-8');
    const good = backupConfigBeforeWrite(cfg);
    assert.ok(good, 'a good pre-write backup should be created');
    assert.equal(findMostRecentConfigBackup(cfg), good);

    // Now corrupt the live file.
    fs.writeFileSync(cfg, '{ not: json,,,', 'utf-8');
    const { config, diagnostic } = loadConfigFileChecked(cfg);
    assert.equal(config, null);
    assert.ok(diagnostic);
    assert.equal(diagnostic!.kind, 'parse-error');
    assert.equal(diagnostic!.restoreHint, `cp "${good}" "${cfg}"`);
  });

  it('omits the restore hint when there is no good backup', () => {
    const dir = makeTmp();
    const cfg = path.join(dir, 'config.json');
    fs.writeFileSync(cfg, '{ not: json,,,', 'utf-8');
    const { diagnostic } = loadConfigFileChecked(cfg);
    assert.ok(diagnostic);
    assert.equal(diagnostic!.restoreHint, undefined);
  });
});

// ---------------------------------------------------------------------------
// Global-config write-count introspection
// ---------------------------------------------------------------------------
describe('global-config write-count introspection', () => {
  it('CONFIG_WRITE_DISPLAY_THRESHOLD matches the reference (20)', () => {
    assert.equal(CONFIG_WRITE_DISPLAY_THRESHOLD, 20);
  });

  it('increments only on writes to the GLOBAL config path', () => {
    const home = makeTmp();
    const savedHome = process.env.HOME;
    const savedUserProfile = process.env.USERPROFILE;
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    try {
      const globalPath = getGlobalConfigPath();
      assert.ok(globalPath, 'global path should resolve under the temp HOME');

      const before = getGlobalConfigWriteCount();
      safeWriteConfig(globalPath!, { model: 'opus' });
      assert.equal(getGlobalConfigWriteCount(), before + 1, 'global write counts');

      // A write to a NON-global path must not move the counter.
      const other = path.join(makeTmp(), '.ami', 'config.json');
      safeWriteConfig(other, { model: 'sonnet' });
      assert.equal(getGlobalConfigWriteCount(), before + 1, 'non-global write does not count');
    } finally {
      if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;
      if (savedUserProfile !== undefined) process.env.USERPROFILE = savedUserProfile;
    }
  });
});
