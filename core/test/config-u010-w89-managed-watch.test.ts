// U010 / w89 — ConfigService managed/policy-tier hot-reload + background poll.
//
// Covers the additive `watchManaged` capability added to ConfigService: watch +
// low-frequency poll the managed/policy config tier (getManagedConfigPath + its
// config.d drop-ins) and fire onConfigChange('managed', path) ONLY when the merged
// managed policy actually changed, failing OPEN on a transient/tampered read so a
// mid-session refresh never crashes the running session. Parity: Claude Code's
// background managed/policy settings refresh + notifyChange('policySettings').
//
// Gaps: managed-tier mid-session refresh (remoteManagedSettings), runtime
// refresh/poll of managed settings without restart (mdm/settings.ts), filesystem
// poll for managed/policy settings (changeDetector).

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ConfigService,
  getManagedConfigPath,
  CONFIG_MANAGED_POLL_MS,
} from '../src/config';

const delay = (ms: number) => new Promise(r => setTimeout(r, ms));

/** A ConfigService rooted at a throwaway project dir with no project poll noise. */
function newService(managedPath: string, opts: Record<string, unknown> = {}): { svc: ConfigService; cwd: string } {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mgd-proj-'));
  const svc = new ConfigService(cwd, { fallbackPollMs: 0, debounceMs: 5, ...opts });
  return { svc, cwd };
}

describe('ConfigService — managed-tier watch/poll (watchManaged)', () => {
  let managedDir: string;
  let managedPath: string;
  let prevEnv: string | undefined;
  const services: ConfigService[] = [];

  function setupManaged(initial: unknown): void {
    managedDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mgd-'));
    managedPath = path.join(managedDir, 'config.json');
    fs.writeFileSync(managedPath, JSON.stringify(initial));
    prevEnv = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managedPath;
  }

  afterEach(async () => {
    for (const s of services.splice(0)) { try { await s.stop(); } catch { /* ignore */ } }
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevEnv;
    prevEnv = undefined;
  });

  it('exports a managed-poll default interval', () => {
    assert.equal(typeof CONFIG_MANAGED_POLL_MS, 'number');
    assert.ok(CONFIG_MANAGED_POLL_MS > 0);
  });

  it('resolves the active managed path from the override and targets it', () => {
    setupManaged({ model: 'a' });
    assert.equal(getManagedConfigPath(), managedPath);
  });

  it('fires onConfigChange("managed", path) when the managed policy changes (poll)', async () => {
    setupManaged({ model: 'a' });
    const { svc } = newService(managedPath, { watchManaged: true, managedPollMs: 15 });
    services.push(svc);
    const events: Array<[string, string]> = [];
    svc.onConfigChange((source, p) => events.push([source, p]));
    svc.watch();

    // No change yet → no managed event.
    await delay(40);
    assert.equal(events.filter(e => e[0] === 'managed').length, 0, 'unchanged policy must not fire');

    // Mutate the managed policy → exactly one managed event with the managed path.
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'b' }));
    await delay(60);
    const managedEvents = events.filter(e => e[0] === 'managed');
    assert.equal(managedEvents.length, 1, 'a real policy change fires exactly once');
    assert.equal(managedEvents[0]![1], managedPath);

    // A second poll with no further change must NOT re-fire (snapshot-gated).
    await delay(50);
    assert.equal(events.filter(e => e[0] === 'managed').length, 1, 'no spurious re-fire');
  });

  it('does NOT watch/poll the managed tier when watchManaged is off (default)', async () => {
    setupManaged({ model: 'a' });
    const { svc } = newService(managedPath, { managedPollMs: 15 });
    services.push(svc);
    const events: Array<[string, string]> = [];
    svc.onConfigChange((source, p) => events.push([source, p]));
    svc.watch();

    fs.writeFileSync(managedPath, JSON.stringify({ model: 'b' }));
    await delay(60);
    assert.equal(events.filter(e => e[0] === 'managed').length, 0, 'managed tier is not watched by default');
  });

  it('fails OPEN: a tampered/broken managed file fires a change instead of throwing', async () => {
    setupManaged({ model: 'a' });
    const { svc } = newService(managedPath, { watchManaged: true, managedPollMs: 15 });
    services.push(svc);
    const events: Array<[string, string]> = [];
    svc.onConfigChange((source, p) => events.push([source, p]));
    svc.watch();
    await delay(30);

    // Corrupt the policy to invalid JSON — the live refresh must NOT crash the session;
    // it collapses to an empty policy (a change) rather than fail-closing mid-session.
    fs.writeFileSync(managedPath, '{ this is not json');
    await delay(60);
    const managedEvents = events.filter(e => e[0] === 'managed');
    assert.equal(managedEvents.length, 1, 'tampered policy treated as changed-to-empty, no throw');
  });

  it('honors a non-positive managedPollMs by disabling the poll', async () => {
    setupManaged({ model: 'a' });
    const { svc } = newService(managedPath, { watchManaged: true, managedPollMs: 0 });
    services.push(svc);
    const events: Array<[string, string]> = [];
    svc.onConfigChange((source, p) => events.push([source, p]));
    svc.watch();
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'b' }));
    await delay(50);
    // With the poll disabled, only the base-dir fs.watch could fire; at most one event
    // and never a crash. (fs.watch delivery is platform-timing dependent.)
    assert.ok(events.filter(e => e[0] === 'managed').length <= 1);
  });

  it('stop() tears the managed watcher/timers down and resolves', async () => {
    setupManaged({ model: 'a' });
    const { svc } = newService(managedPath, { watchManaged: true, managedPollMs: 15 });
    svc.watch();
    await assert.doesNotReject(svc.stop());
    // After stop, a further policy change must not fire (idempotent, no live timers).
    const events: string[] = [];
    svc.onConfigChange((source) => events.push(source));
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'c' }));
    await delay(50);
    assert.equal(events.length, 0);
    // A second stop is safe (nothing left to close).
    await assert.doesNotReject(svc.stop());
  });
});
