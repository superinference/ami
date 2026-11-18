import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  updateConfigFile,
  loadConfigFileChecked,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// UNIT U010 — cross-process advisory lock on config writes.
//   updateConfigFile wraps its re-read → merge → write in withFileLockSync so
//   concurrent AMI processes serialize instead of losing each other's keys
//   (parity: Claude Code's saveConfigWithLock lockSync + onCompromised). Each
//   assertion discriminates the LOCKED behavior from the pre-fix unlocked one.
// ---------------------------------------------------------------------------

describe('U010 — cross-process advisory lock on updateConfigFile', () => {
  let dir: string;
  let cfg: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-u010-lock-'));
    cfg = path.join(dir, 'config.json');
  });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('holds the advisory lock DURING the read-modify-write and releases it after', () => {
    let lockHeldDuringUpdater = false;
    const res = updateConfigFile(cfg, (cur) => {
      // The lock file must exist for the whole critical section — proving the
      // read+write run under the lock, not just the atomic rename.
      lockHeldDuringUpdater = fs.existsSync(`${cfg}.lock`);
      return { ...cur, model: 'm1' } as ProjectConfig;
    });
    assert.equal(res.written, true);
    assert.equal(lockHeldDuringUpdater, true, 'lock must be held while the updater runs');
    assert.equal(fs.existsSync(`${cfg}.lock`), false, 'lock released after the write');
    assert.deepEqual(loadConfigFileChecked(cfg).config, { model: 'm1' });
  });

  it('steals a stale/dead-holder lock so a crashed writer cannot wedge config forever', () => {
    // A leftover lock recording a provably-dead PID must be reclaimed immediately.
    // The lock is an mkdir'd DIRECTORY whose `pid` entry records the holder.
    fs.mkdirSync(`${cfg}.lock`, { recursive: true });
    fs.writeFileSync(path.join(`${cfg}.lock`, 'pid'), String(2 ** 31 - 1));
    const res = updateConfigFile(cfg, (cur) => ({ ...cur, model: 'after-steal' } as ProjectConfig), { retries: 0 });
    assert.equal(res.written, true, 'dead-holder lock reclaimed with retries:0, so the write proceeded');
    assert.equal(loadConfigFileChecked(cfg).config?.model, 'after-steal');
  });

  it('routes a stolen-mid-section lock to onCompromised (downgrade to a log, not a throw)', () => {
    const compromised: string[] = [];
    const res = updateConfigFile(
      cfg,
      (cur) => {
        // Simulate a waiter stealing the lock mid-write by rewriting its pid entry.
        fs.writeFileSync(path.join(`${cfg}.lock`, 'pid'), String(process.pid + 1));
        return { ...cur, model: 'm2' } as ProjectConfig;
      },
      { onCompromised: (e) => compromised.push(e.message) },
    );
    assert.equal(res.written, true, 'the write still completes — a stolen lock is recoverable');
    assert.equal(compromised.length, 1, 'onCompromised fired instead of throwing');
    // Clean up the synthetic successor lock.
    fs.rmSync(`${cfg}.lock`, { recursive: true, force: true });
  });

  it('still re-reads FRESH on-disk state inside the lock (lost-update guard preserved)', () => {
    updateConfigFile(cfg, (cur) => ({ ...cur, a: 1 } as ProjectConfig));
    // Another process writes `c` after a stale read.
    fs.writeFileSync(cfg, JSON.stringify({ a: 1, c: 3 }), 'utf-8');
    let sawC: unknown;
    const res = updateConfigFile(cfg, (cur) => {
      sawC = (cur as Record<string, unknown>).c;
      return { ...cur, d: 4 } as ProjectConfig;
    });
    assert.equal(sawC, 3, 'updater sees the concurrently-written key (fresh re-read inside the lock)');
    assert.deepEqual(loadConfigFileChecked(cfg).config, { a: 1, c: 3, d: 4 } as ProjectConfig);
    assert.equal(res.written, true);
  });

  it('still refuses to clobber a present-but-corrupt file (corruption guard preserved)', () => {
    fs.writeFileSync(cfg, '{ this is : not json', 'utf-8');
    const res = updateConfigFile(cfg, (cur) => ({ ...cur, model: 'x' } as ProjectConfig));
    assert.equal(res.written, false);
    assert.equal(res.reason, 'corrupt');
    assert.equal(fs.existsSync(`${cfg}.lock`), false, 'lock released even on the corrupt short-circuit');
  });
});
