/**
 * U010 / w109 — managed `config.d/` drop-in enumeration sanitization.
 *
 * Parity: Claude Code's managed-settings.d enumeration guard defensively skips any
 * entry that is NOT a regular file or symlink, does not end in `.json`, or is a
 * dotfile. AMI previously filtered the drop-in listing ONLY on the `.json` suffix,
 * so a hidden `.secret.json` dotfile was read as policy, and a `*.json`-NAMED FIFO
 * would make `readFileSync` BLOCK forever (a trivial DoS if an entry can be planted).
 *
 * This locks in {@link isEligibleManagedDropIn} + its use inside
 * {@link loadManagedConfig}: dotfiles are skipped, non-file/non-symlink types are
 * refused, and well-formed `*.json` fragments still layer normally.
 *
 * RED (pre-fix, `.filter(e => e.endsWith('.json'))`): the `.secret.json` dotfile is
 * read and its `maxTurns` shows up → the dotfile-skip assertion FAILS.
 */
import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  loadManagedConfig,
  isEligibleManagedDropIn,
  fsHooks,
  type ProjectConfig,
} from '../src/config';

function mkTemp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-w109-'));
}

/** A minimal fs.Stats-like object for lstatSync mocking. */
function fakeStats(opts: { file?: boolean; symlink?: boolean; special?: boolean }): fs.Stats {
  return {
    isFile: () => opts.file === true,
    isSymbolicLink: () => opts.symlink === true,
    isFIFO: () => opts.special === true,
    isSocket: () => false,
    isCharacterDevice: () => false,
    isBlockDevice: () => false,
    isDirectory: () => false,
  } as unknown as fs.Stats;
}

describe('U010/w109 — isEligibleManagedDropIn', () => {
  afterEach(() => mock.restoreAll());

  it('rejects a dotfile even when it ends in .json', () => {
    assert.equal(isEligibleManagedDropIn('.secret.json', '/etc/x/config.d'), false);
  });

  it('rejects a non-.json entry', () => {
    mock.method(fsHooks, 'lstatSync', (() => fakeStats({ file: true })) as typeof fs.lstatSync);
    assert.equal(isEligibleManagedDropIn('notes.txt', '/etc/x/config.d'), false);
  });

  it('accepts a regular .json file', () => {
    mock.method(fsHooks, 'lstatSync', (() => fakeStats({ file: true })) as typeof fs.lstatSync);
    assert.equal(isEligibleManagedDropIn('10-team.json', '/etc/x/config.d'), true);
  });

  it('accepts a symlinked .json entry (trust-checked downstream)', () => {
    mock.method(fsHooks, 'lstatSync', (() => fakeStats({ symlink: true })) as typeof fs.lstatSync);
    assert.equal(isEligibleManagedDropIn('20-link.json', '/etc/x/config.d'), true);
  });

  it('refuses a FIFO/device named *.json (would block readFileSync)', () => {
    mock.method(fsHooks, 'lstatSync', (() => fakeStats({ special: true })) as typeof fs.lstatSync);
    assert.equal(isEligibleManagedDropIn('pipe.json', '/etc/x/config.d'), false);
  });

  it('refuses a directory named *.json', () => {
    mock.method(fsHooks, 'lstatSync', (() => fakeStats({})) as typeof fs.lstatSync);
    assert.equal(isEligibleManagedDropIn('subdir.json', '/etc/x/config.d'), false);
  });

  it('treats an unstattable entry as absent', () => {
    mock.method(fsHooks, 'lstatSync', (() => {
      throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
    }) as typeof fs.lstatSync);
    assert.equal(isEligibleManagedDropIn('gone.json', '/etc/x/config.d'), false);
  });

  it('falls back to eligible when the stat seam cannot report a type', () => {
    mock.method(fsHooks, 'lstatSync', (() => ({ isSymbolicLink: () => false }) as unknown as fs.Stats) as typeof fs.lstatSync);
    assert.equal(isEligibleManagedDropIn('10-team.json', '/etc/x/config.d'), true);
  });
});

describe('U010/w109 — loadManagedConfig skips hidden drop-ins (real fs)', () => {
  const savedOverride = process.env.SUPERINFERENCE_MANAGED_CONFIG;
  let dir: string | null = null;

  afterEach(() => {
    if (savedOverride === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = savedOverride;
    if (dir) {
      try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* best effort */ }
      dir = null;
    }
    mock.restoreAll();
  });

  it('applies a well-formed .json fragment but NOT a .secret.json dotfile', () => {
    dir = mkTemp();
    const base = path.join(dir, 'config.json');
    fs.writeFileSync(base, JSON.stringify({ model: 'base-model' }), { mode: 0o600 });
    const dropDir = path.join(dir, 'config.d');
    fs.mkdirSync(dropDir, { mode: 0o700 });
    // A well-formed fragment that SHOULD layer on top.
    fs.writeFileSync(path.join(dropDir, '10-ok.json'), JSON.stringify({ permissionMode: 'auto-allow' }), { mode: 0o600 });
    // A hidden dotfile whose ONLY-source key (maxTurns) must NOT be picked up.
    fs.writeFileSync(path.join(dropDir, '.secret.json'), JSON.stringify({ maxTurns: 99 }), { mode: 0o600 });
    process.env.SUPERINFERENCE_MANAGED_CONFIG = base;

    const cfg = loadManagedConfig() as ProjectConfig;
    assert.equal(cfg.model, 'base-model');
    assert.equal(cfg.permissionMode, 'auto-allow', 'the well-formed drop-in must apply');
    assert.notEqual(cfg.maxTurns, 99, 'the hidden .secret.json dotfile must be skipped');
  });

  it('ignores a subdirectory named like a fragment without failing the load', () => {
    dir = mkTemp();
    const base = path.join(dir, 'config.json');
    fs.writeFileSync(base, JSON.stringify({ model: 'base-model' }), { mode: 0o600 });
    const dropDir = path.join(dir, 'config.d');
    fs.mkdirSync(dropDir, { mode: 0o700 });
    fs.mkdirSync(path.join(dropDir, 'nested.json'), { mode: 0o700 });
    fs.writeFileSync(path.join(dropDir, '10-ok.json'), JSON.stringify({ permissionMode: 'auto-allow' }), { mode: 0o600 });
    process.env.SUPERINFERENCE_MANAGED_CONFIG = base;

    const cfg = loadManagedConfig() as ProjectConfig;
    assert.equal(cfg.model, 'base-model');
    assert.equal(cfg.permissionMode, 'auto-allow');
  });
});
