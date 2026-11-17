import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { HookManager } from '../src/hooks';

// U007 / w95 parity:
//  - SessionContext.signal + CompactContext.signal: cooperative cancellation of the
//    in-process session/compact lifecycle hook chains (Claude Code threads
//    AbortSignal.timeout(sessionEndTimeoutMs) / abortController.signal into
//    executeSessionEndHooks / executePreCompactHooks).
//  - Per-item admin-trust gate at frontmatter/bundle hook registration
//    (isSourceAdminTrusted): when strictPluginOnlyCustomization locks the `hooks`
//    surface, an untrusted (user/project-authored, no plugin provenance) frontmatter
//    hook is fail-closed blocked, while a plugin-provided one survives.
const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

describe('HookManager — SessionContext.signal cooperative cancellation', () => {
  let tmp: string;
  let hm: HookManager;
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-sig-'));
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });
  afterEach(() => { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('SessionStart: a pre-aborted signal skips the whole in-process chain', async () => {
    const ac = new AbortController();
    ac.abort();
    let calls = 0;
    hm.onSessionStart(async () => { calls++; });
    await hm.executeSessionStart({ sessionId: 's', cwd: tmp, signal: ac.signal });
    assert.equal(calls, 0, 'no SessionStart hook runs once the signal is aborted');
  });

  it('SessionStart: aborting mid-chain stops the next hook (RED without raceAbort)', async () => {
    const ac = new AbortController();
    let first = false;
    let second = false;
    hm.onSessionStart(async () => { first = true; ac.abort(); await sleep(50); });
    hm.onSessionStart(async () => { second = true; });
    await hm.executeSessionStart({ sessionId: 's', cwd: tmp, signal: ac.signal });
    assert.equal(first, true, 'the first hook started');
    assert.equal(second, false, 'the second hook must not run after the chain is aborted');
  });

  it('SessionStart: no signal passes every hook through unchanged', async () => {
    let calls = 0;
    hm.onSessionStart(async () => { calls++; });
    hm.onSessionStart(async () => { calls++; });
    await hm.executeSessionStart({ sessionId: 's', cwd: tmp });
    assert.equal(calls, 2, 'both hooks run when no signal is supplied');
  });

  it('SessionEnd: a pre-aborted signal skips the in-process teardown chain', async () => {
    const ac = new AbortController();
    ac.abort();
    let calls = 0;
    hm.onSessionEnd(async () => { calls++; });
    await hm.executeSessionEnd({ sessionId: 's', cwd: tmp, reason: 'clear', signal: ac.signal });
    assert.equal(calls, 0, 'no SessionEnd hook runs once the signal is aborted');
  });

  it('SessionEnd: aborting mid-chain stops the next teardown hook', async () => {
    const ac = new AbortController();
    let first = false;
    let second = false;
    hm.onSessionEnd(async () => { first = true; ac.abort(); await sleep(50); });
    hm.onSessionEnd(async () => { second = true; });
    await hm.executeSessionEnd({ sessionId: 's', cwd: tmp, reason: 'clear', signal: ac.signal });
    assert.equal(first, true, 'the first teardown hook started');
    assert.equal(second, false, 'the second teardown hook must not run after abort');
  });
});

describe('HookManager — CompactContext.signal cooperative cancellation', () => {
  let tmp: string;
  let hm: HookManager;
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-csig-'));
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });
  afterEach(() => { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('PreCompact: a pre-aborted signal skips the whole in-process chain', async () => {
    const ac = new AbortController();
    ac.abort();
    let calls = 0;
    hm.onPreCompact(async () => { calls++; });
    await hm.executePreCompact({ messageCount: 1, tokenEstimate: 1, signal: ac.signal });
    assert.equal(calls, 0, 'no PreCompact hook runs once the signal is aborted');
  });

  it('PreCompact: aborting mid-chain stops the next hook (RED without raceAbort)', async () => {
    const ac = new AbortController();
    let first = false;
    let second = false;
    hm.onPreCompact(async () => { first = true; ac.abort(); await sleep(50); return; });
    hm.onPreCompact(async () => { second = true; return { newCustomInstructions: 'x' }; });
    const r = await hm.executePreCompact({ messageCount: 1, tokenEstimate: 1, signal: ac.signal });
    assert.equal(first, true, 'the first PreCompact hook started');
    assert.equal(second, false, 'the second PreCompact hook must not run after abort');
    assert.equal(r?.newCustomInstructions, undefined, 'a hook aborted before contributing adds nothing');
  });

  it('PreCompact: no signal aggregates every hook', async () => {
    hm.onPreCompact(async () => ({ newCustomInstructions: 'a' }));
    hm.onPreCompact(async () => ({ newCustomInstructions: 'b' }));
    const r = await hm.executePreCompact({ messageCount: 1, tokenEstimate: 1 });
    assert.match(r?.newCustomInstructions ?? '', /a/);
    assert.match(r?.newCustomInstructions ?? '', /b/);
  });
});

describe('HookManager — per-item admin-trust gate for frontmatter/bundle hooks', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-trust-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    process.env.HOME = home;
    delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
  });
  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH; else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  function lockHooksSurface(): void {
    const file = path.join(home, 'managed.json');
    fs.writeFileSync(file, JSON.stringify({ strictPluginOnlyCustomization: ['hooks'] }));
    process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = file;
  }

  function mk(): HookManager {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    return hm;
  }

  it('blocks an untrusted (no-provenance) frontmatter hook when the hooks surface is locked (RED without the gate)', () => {
    lockHooksSurface();
    const hm = mk();
    hm.registerConfigEntries([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo user' } },
    ], tmp);
    assert.equal(hm.getSummary().preToolUse, 0, 'user/project frontmatter hook is fail-closed blocked under the lock');
  });

  it('admits a plugin-provenance frontmatter hook even under the lock', () => {
    lockHooksSurface();
    const hm = mk();
    hm.registerConfigEntries([
      { event: 'postToolUse', hook: { type: 'command', command: 'echo plug', pluginRoot: '/plugins/p' }, pluginRoot: '/plugins/p', pluginName: 'p' },
    ], tmp);
    assert.equal(hm.getSummary().postToolUse, 1, 'a plugin-provided (admin-trusted) frontmatter hook survives the lock');
  });

  it('registers an untrusted frontmatter hook normally when nothing locks the hooks surface', () => {
    // No managed policy at all → unlocked default.
    const hm = mk();
    hm.registerConfigEntries([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo user' } },
    ], tmp);
    assert.equal(hm.getSummary().preToolUse, 1, 'without the lock, frontmatter hooks register as before');
  });

  it('a lock on a different surface leaves frontmatter hooks registerable', () => {
    const file = path.join(home, 'managed.json');
    fs.writeFileSync(file, JSON.stringify({ strictPluginOnlyCustomization: ['skills'] }));
    process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = file;
    const hm = mk();
    hm.registerConfigEntries([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo user' } },
    ], tmp);
    assert.equal(hm.getSummary().preToolUse, 1, 'locking skills does not gate the hooks surface');
  });
});
