import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  HookManager,
  ALWAYS_EMITTED_HOOK_EVENTS,
  shouldEmitHookLifecycle,
  formatStopHookSummaryMessage,
  hookSourceDescription,
  hookSourceHeader,
  hookSourceInline,
  HOOK_TIMING_DISPLAY_THRESHOLD_MS,
  type HookLifecycleEvent,
  type StopHookSummary,
} from '../src/hooks';

// U007 (w86) — three confirmed gaps on core/core/src/hooks.ts:
//   F1. includeHookEvents / setAllHookEventsEnabled config knob + the
//       ALWAYS_EMITTED_HOOK_EVENTS allowlist (hookEvents.ts:L18-19,59,83-91,179-186).
//   F2. Stop-hook run summary message (count, per-hook durations, errors, output
//       flag) — stopHooks.ts:L242-323 createStopHookSummaryMessage.
//   F3. Human-readable hook-source labels for UI: full description WITH file path,
//       section header, and inline short form (hooksSettings.ts:L170-228).

// ---------------------------------------------------------------------------
// F1 — hook-event emission gating
// ---------------------------------------------------------------------------
describe('U007-w86 F1 — shouldEmitHookLifecycle gate', () => {
  it('always-emitted allowlist passes regardless of the all-events flag', () => {
    for (const ev of ALWAYS_EMITTED_HOOK_EVENTS) {
      assert.equal(shouldEmitHookLifecycle(ev, false), true, `${ev} passes when gated`);
      assert.equal(shouldEmitHookLifecycle(ev, true), true, `${ev} passes when open`);
    }
  });

  it('allowlist is exactly sessionStart + setup (the reference parity)', () => {
    assert.deepEqual([...ALWAYS_EMITTED_HOOK_EVENTS], ['sessionStart', 'setup']);
  });

  it('a non-allowlist known event only passes when all-events is enabled', () => {
    assert.equal(shouldEmitHookLifecycle('preToolUse', false), false);
    assert.equal(shouldEmitHookLifecycle('stop', false), false);
    assert.equal(shouldEmitHookLifecycle('preToolUse', true), true);
    assert.equal(shouldEmitHookLifecycle('stop', true), true);
  });

  it('an unknown event name is never emitted, even when all-events is enabled', () => {
    assert.equal(shouldEmitHookLifecycle('not-a-real-event', true), false);
    assert.equal(shouldEmitHookLifecycle('not-a-real-event', false), false);
  });
});

describe('U007-w86 F1 — HookManager all-events toggle', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('defaults ON (AMI superset) so existing all-events observability is preserved', () => {
    assert.equal(hm.isAllHookEventsEnabled(), true);
    const seen: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => seen.push(e));
    hm.emit('hookProgress', { event: 'preToolUse', hookType: 'command', label: 'A' });
    assert.equal(seen.length, 1, 'non-allowlist event still delivered by default');
  });

  it('setAllHookEventsEnabled(false) quiets the channel to the allowlist only', () => {
    hm.setAllHookEventsEnabled(false);
    assert.equal(hm.isAllHookEventsEnabled(), false);
    const seen: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => seen.push(e));
    // Non-allowlist events are suppressed...
    hm.emit('hookProgress', { event: 'preToolUse', hookType: 'command', label: 'A' });
    hm.emit('hookResolved', { event: 'stop', hookType: 'command', label: 'B', success: true });
    assert.equal(seen.length, 0, 'preToolUse/stop suppressed when gated');
    // ...but the allowlist still flows.
    hm.emit('hookProgress', { event: 'sessionStart', hookType: 'command', label: 'S' });
    hm.emit('hookProgress', { event: 'setup', hookType: 'command', label: 'U' });
    assert.equal(seen.length, 2, 'sessionStart + setup still delivered when gated');
    assert.deepEqual(seen.map(e => e.event), ['sessionStart', 'setup']);
  });

  it('gating also suppresses buffering (no subscriber) for non-allowlist events', () => {
    hm.setAllHookEventsEnabled(false);
    hm.emit('hookProgress', { event: 'preToolUse', hookType: 'command', label: 'A' });
    assert.equal(hm.getPendingHookEventCount(), 0, 'suppressed event is not buffered');
    hm.emit('hookProgress', { event: 'setup', hookType: 'command', label: 'U' });
    assert.equal(hm.getPendingHookEventCount(), 1, 'allowlist event is buffered for replay');
  });

  it('re-enabling restores all-events delivery', () => {
    hm.setAllHookEventsEnabled(false);
    hm.setAllHookEventsEnabled(true);
    const seen: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => seen.push(e));
    hm.emit('hookProgress', { event: 'stop', hookType: 'command', label: 'B' });
    assert.equal(seen.length, 1);
  });

  it('isAllHookEventsEnabled reflects the toggle (introspection surface)', () => {
    assert.equal(hm.isAllHookEventsEnabled(), true);
    hm.setAllHookEventsEnabled(false);
    assert.equal(hm.isAllHookEventsEnabled(), false);
    hm.setAllHookEventsEnabled(true);
    assert.equal(hm.isAllHookEventsEnabled(), true);
  });
});

// ---------------------------------------------------------------------------
// F2 — stop-hook run summary message
// ---------------------------------------------------------------------------
describe('U007-w86 F2 — formatStopHookSummaryMessage', () => {
  const base = (over: Partial<StopHookSummary>): StopHookSummary => ({
    count: 1,
    hookInfos: [{ durationMs: 10 }],
    totalDurationMs: 10,
    hookErrors: [],
    preventContinuation: false,
    hasOutput: false,
    ...over,
  });

  it('returns empty when the chain is fast, clean, and has no output', () => {
    assert.equal(formatStopHookSummaryMessage(base({})), '');
  });

  it('renders count + wall-clock, pluralizing correctly', () => {
    const one = formatStopHookSummaryMessage(base({ hasOutput: true, count: 1 }));
    assert.match(one, /^Ran 1 stop hook in 10ms/);
    const many = formatStopHookSummaryMessage(base({
      hasOutput: true, count: 2, hookInfos: [{ durationMs: 1 }, { durationMs: 2 }], totalDurationMs: 3,
    }));
    assert.match(many, /^Ran 2 stop hooks in 3ms/);
  });

  it('surfaces per-hook error lines with their duration', () => {
    const msg = formatStopHookSummaryMessage(base({
      hasOutput: true,
      count: 2,
      hookInfos: [{ durationMs: 5 }, { durationMs: 7, error: 'boom' }],
      totalDurationMs: 12,
      hookErrors: ['boom'],
    }));
    assert.match(msg, /hook 2 \(7ms\): boom/);
    // The clean fast hook 1 is NOT listed (no noise).
    assert.doesNotMatch(msg, /hook 1/);
  });

  it('lists an individually-slow (non-error) hook but omits fast clean ones', () => {
    const slow = HOOK_TIMING_DISPLAY_THRESHOLD_MS + 1;
    const msg = formatStopHookSummaryMessage(base({
      hasOutput: true,
      count: 2,
      hookInfos: [{ durationMs: 1 }, { durationMs: slow }],
      totalDurationMs: slow + 1,
    }));
    assert.match(msg, new RegExp(`hook 2: ${slow}ms`));
    assert.doesNotMatch(msg, /hook 1/);
  });

  it('renders even a clean chain when it was slow overall', () => {
    const slow = HOOK_TIMING_DISPLAY_THRESHOLD_MS + 50;
    const msg = formatStopHookSummaryMessage(base({ hasOutput: false, totalDurationMs: slow, hookInfos: [{ durationMs: slow }] }));
    assert.match(msg, new RegExp(`Ran 1 stop hook in ${slow}ms`));
  });

  it('notes a prevented continuation with the stop reason (default when terse)', () => {
    const withReason = formatStopHookSummaryMessage(base({
      hasOutput: true, preventContinuation: true, stopReason: 'needs review',
    }));
    assert.match(withReason, /Continuation prevented: needs review/);
    const terse = formatStopHookSummaryMessage(base({ hasOutput: true, preventContinuation: true }));
    assert.match(terse, /Continuation prevented: Stop hook prevented continuation/);
  });
});

describe('U007-w86 F2 — executeStop wires hasOutput + getLastStopHookSummaryMessage', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('a clean fast stop chain sets hasOutput=false and yields no summary message', async () => {
    hm.onStop(async () => {});
    await hm.executeStop({ messages: [], turnCount: 0 });
    const summary = hm.getLastStopHookSummary();
    assert.ok(summary);
    assert.equal(summary!.hasOutput, false);
    assert.equal(hm.getLastStopHookSummaryMessage(), null, 'nothing worth surfacing');
  });

  it('an erroring stop hook sets hasOutput=true and renders a diagnostic message', async () => {
    hm.onStop(async () => { throw new Error('scan failed'); });
    await hm.executeStop({ messages: [], turnCount: 0 });
    const summary = hm.getLastStopHookSummary();
    assert.ok(summary);
    assert.equal(summary!.hasOutput, true);
    assert.deepEqual(summary!.hookErrors, ['scan failed']);
    const msg = hm.getLastStopHookSummaryMessage();
    assert.ok(msg);
    assert.match(msg!, /Ran 1 stop hook/);
    assert.match(msg!, /scan failed/);
  });

  it('a prevented continuation sets hasOutput=true and notes it in the message', async () => {
    hm.onStop(async () => ({ preventContinuation: true, stopReason: 'keep going' }));
    await hm.executeStop({ messages: [], turnCount: 0 });
    assert.equal(hm.getLastStopHookSummary()!.hasOutput, true);
    assert.match(hm.getLastStopHookSummaryMessage()!, /Continuation prevented: keep going/);
  });

  it('getLastStopHookSummaryMessage is null before any stop chain runs', () => {
    assert.equal(hm.getLastStopHookSummaryMessage(), null);
  });
});

// ---------------------------------------------------------------------------
// F3 — hook-source label forms
// ---------------------------------------------------------------------------
describe('U007-w86 F3 — hook-source label forms (description/header/inline)', () => {
  it('full description includes the backing file path per tier', () => {
    assert.equal(hookSourceDescription('project'), 'Project settings (.ami/settings.json)');
    assert.equal(hookSourceDescription('user'), 'User settings (~/.ami/settings.json)');
    assert.equal(hookSourceDescription('config'), 'Project config (.ami/config.json)');
    assert.match(hookSourceDescription('managed'), /^Managed policy /);
    assert.match(hookSourceDescription('plugin'), /^Plugin hooks /);
  });

  it('section header is Title-Case per tier', () => {
    assert.equal(hookSourceHeader('project'), 'Project Settings');
    assert.equal(hookSourceHeader('user'), 'User Settings');
    assert.equal(hookSourceHeader('config'), 'Project Config');
    assert.equal(hookSourceHeader('managed'), 'Managed Policy');
    assert.equal(hookSourceHeader('plugin'), 'Plugin Hooks');
  });

  it('inline badge is a single terse word per tier', () => {
    assert.equal(hookSourceInline('project'), 'Project');
    assert.equal(hookSourceInline('user'), 'User');
    assert.equal(hookSourceInline('config'), 'Config');
    assert.equal(hookSourceInline('managed'), 'Managed');
    assert.equal(hookSourceInline('plugin'), 'Plugin');
  });

  it('description + header append the plugin name; inline stays short', () => {
    assert.equal(hookSourceDescription('project', 'acme'), 'Project settings (.ami/settings.json) (acme)');
    assert.equal(hookSourceHeader('plugin', 'acme'), 'Plugin Hooks (acme)');
    assert.equal(hookSourceInline('plugin'), 'Plugin', 'inline never carries the plugin name');
  });

  it('unknown source passes through verbatim in every form', () => {
    assert.equal(hookSourceDescription('weird'), 'weird');
    assert.equal(hookSourceHeader('weird'), 'weird');
    assert.equal(hookSourceInline('weird'), 'weird');
  });
});

describe('U007-w86 F3 — getHookAuditView surfaces the three label forms end-to-end', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w86-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w86-home-'));
    origHome = process.env.HOME;
    process.env.HOME = home;
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });
  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  it('a project .ami/hooks.json hook carries description/header/inline in its audit row', () => {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo hi' } }] }),
    );
    hm.loadFromFile(tmp);
    const row = hm.getHookAuditView().find(r => r.event === 'preToolUse');
    assert.ok(row, 'audit row present');
    assert.equal(row!.sourceDescription, 'Project settings (.ami/settings.json)');
    assert.equal(row!.sourceHeader, 'Project Settings');
    assert.equal(row!.sourceInline, 'Project');
  });
});
