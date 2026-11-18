import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { mock } from 'node:test';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn, type ChildProcess } from 'child_process';
import { safeWriteConfig, cleanupStaleConfigTmp, fsHooks } from '../src/config';

/**
 * U010 — PID-scoped runtime namespace for config atomic writes.
 *
 * Parity target: Claude's getSwarmSocketName() embeds process.pid so concurrent
 * instances never clobber one shared ephemeral runtime path. AMI's config writer
 * now does the same for its atomic-write scratch file, and the crash-recovery
 * sweep reclaims a dead instance's orphan while leaving a LIVE concurrent
 * instance's in-flight temp untouched.
 *
 * RED on revert: before the fix, safeWriteConfig used a fixed `<target>.tmp`
 * (collides between instances) and cleanupStaleConfigTmp only ever looked at that
 * single fixed name — so it could neither recover a PID-scoped orphan nor skip a
 * live concurrent writer.
 */
describe('config PID-scoped atomic-write temp', () => {
  const dirs: string[] = [];
  function mkTmp(): string {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-pid-'));
    dirs.push(d);
    return d;
  }
  afterEach(() => mock.restoreAll());
  after(() => {
    for (const d of dirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* */ } }
  });

  it('writes through a PID-scoped scratch path, not a fixed <target>.tmp', () => {
    const dir = mkTmp();
    const fp = path.join(dir, 'config.json');
    const renamed: string[] = [];
    const real = fs.renameSync;
    mock.method(fsHooks, 'renameSync', (from: string, to: string) => {
      renamed.push(from);
      return real(from, to);
    });
    safeWriteConfig(fp, { model: 'opus' });
    // The scratch file that was renamed over the target must carry THIS pid, not
    // the fixed collision-prone name.
    assert.equal(renamed.length, 1);
    assert.equal(renamed[0], `${fp}.${process.pid}.tmp`);
    assert.notEqual(renamed[0], `${fp}.tmp`);
    assert.deepEqual(JSON.parse(fs.readFileSync(fp, 'utf-8')), { model: 'opus' });
  });

  it('recovers a dead-instance PID-scoped orphan on next startup', () => {
    const dir = mkTmp();
    const fp = path.join(dir, 'config.json');
    const deadPid = 2147483646; // effectively never a live process
    fs.writeFileSync(`${fp}.${deadPid}.tmp`, '{"recovered": true}', 'utf-8');
    assert.equal(cleanupStaleConfigTmp(fp), true);
    assert.equal(fs.existsSync(`${fp}.${deadPid}.tmp`), false);
    assert.deepEqual(JSON.parse(fs.readFileSync(fp, 'utf-8')), { recovered: true });
  });

  it('still recovers a legacy un-scoped <target>.tmp orphan', () => {
    const dir = mkTmp();
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(`${fp}.tmp`, '{"legacy": 1}', 'utf-8');
    assert.equal(cleanupStaleConfigTmp(fp), true);
    assert.equal(fs.existsSync(`${fp}.tmp`), false);
    assert.deepEqual(JSON.parse(fs.readFileSync(fp, 'utf-8')), { legacy: 1 });
  });

  it('discards a corrupt dead-instance orphan without creating the target', () => {
    const dir = mkTmp();
    const fp = path.join(dir, 'config.json');
    fs.writeFileSync(`${fp}.999999999.tmp`, '{bad json', 'utf-8');
    assert.equal(cleanupStaleConfigTmp(fp), true);
    assert.equal(fs.existsSync(`${fp}.999999999.tmp`), false);
    assert.equal(fs.existsSync(fp), false);
  });

  it('returns false when there is no temp orphan at all', () => {
    const dir = mkTmp();
    assert.equal(cleanupStaleConfigTmp(path.join(dir, 'config.json')), false);
  });
});

/**
 * Concurrent-instance contention: a temp owned by a DIFFERENT, still-live process
 * is an in-flight write and must NOT be reclaimed by another instance's sweep.
 * Uses a real spawned child as the "other live instance" so the liveness probe is
 * genuine.
 */
describe('config recovery leaves a live concurrent instance alone', () => {
  let dir: string;
  let child: ChildProcess;
  let childPid: number;

  before(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-live-'));
    // A real, long-lived child process to stand in for a second AMI instance.
    child = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 60000)'], { stdio: 'ignore' });
    childPid = child.pid!;
    // Wait until the child is actually scheduled so the liveness probe is reliable.
    await new Promise<void>((resolve) => setTimeout(resolve, 100));
  });

  after(() => {
    try { child.kill('SIGKILL'); } catch { /* */ }
    try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* */ }
  });

  it('skips (does not promote or delete) a live other-instance temp', () => {
    const fp = path.join(dir, 'config.json');
    assert.notEqual(childPid, process.pid);
    const liveTmp = `${fp}.${childPid}.tmp`;
    fs.writeFileSync(liveTmp, '{"inflight": true}', 'utf-8');
    // A temp WAS found (true), but it belongs to a live instance → untouched.
    assert.equal(cleanupStaleConfigTmp(fp), true);
    assert.equal(fs.existsSync(liveTmp), true, 'live instance temp must be preserved');
    assert.equal(fs.existsSync(fp), false, 'must not promote a live instance temp');
  });

  it("reclaims this instance's OWN orphan even though its PID is alive", () => {
    const fp = path.join(dir, 'own.json');
    const ownTmp = `${fp}.${process.pid}.tmp`;
    fs.writeFileSync(ownTmp, '{"own": 1}', 'utf-8');
    assert.equal(cleanupStaleConfigTmp(fp), true);
    assert.equal(fs.existsSync(ownTmp), false);
    assert.deepEqual(JSON.parse(fs.readFileSync(fp, 'utf-8')), { own: 1 });
  });
});
