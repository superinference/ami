/**
 * Unit U007 (w103) — four additive hook-lifecycle parity gaps on
 * core/core/src/hooks.ts, all extensions of the existing hook-lifecycle /
 * progress infrastructure (no parallel module, no duplication):
 *
 *  1. Per-hook named lifecycle events — `hookId` + `hookName` on the
 *     progress/resolved lifecycle events (Claude Code hookEvents.ts
 *     HookStartedEvent/HookResponseEvent carry a stable hookId + hookName).
 *     AMI previously emitted only {phase,event,hookType,label,success}.
 *  2. Explicit 3-way `outcome` on the SYNC resolved lifecycle event
 *     ('success' | 'error' | 'cancelled'), consolidated through the shared
 *     `classifyHookOutcome` helper (Claude Code's HookResponseEvent.outcome).
 *     Previously only the async `hookResponse` path had the 3-way classification
 *     (inline); the sync `hookResolved` had only a boolean.
 *  3. `startHookProgressInterval` — the progress-polling helper with an
 *     intervalMs default, gated by the all-events allowlist, dedup on output,
 *     unref'd, returning a stop closure (Claude Code hookEvents.ts).
 *  4. Live hook progress streaming — incremental stdout/stderr published on the
 *     lifecycle channel WHILE a command hook runs (Claude Code emitHookProgress
 *     driven by startHookProgressInterval), instead of only start + end.
 *
 * RED on revert: each assertion below references a symbol/field that does not
 * exist before the implementation (classifyHookOutcome / computeHookId /
 * startHookProgressInterval / the hookId/hookName/outcome/stdout fields), and
 * the streaming test sees NO incremental progress event without the wiring.
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  classifyHookOutcome,
  computeHookId,
  startHookProgressInterval,
  HOOK_LIFECYCLE_OUTCOMES,
  HOOK_PROGRESS_INTERVAL_MS,
  type SessionContext,
  type HookProgressEvent,
  type HookLifecycleEvent,
  type HookLifecycleOutcome,
} from '../src/hooks';

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

function sessCtx(overrides: Partial<SessionContext> = {}): SessionContext {
  return { sessionId: 's1', cwd: process.cwd(), source: 'startup', ...overrides };
}

function writeTmpHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w103-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

// ---------------------------------------------------------------------------
// 2. classifyHookOutcome — pure 3-way classifier
// ---------------------------------------------------------------------------
describe('U007/w103 — classifyHookOutcome', () => {
  it('maps (success,notAborted) → success', () => {
    assert.equal(classifyHookOutcome(true, false), 'success');
  });
  it('maps (failure,notAborted) → error', () => {
    assert.equal(classifyHookOutcome(false, false), 'error');
  });
  it('maps aborted → cancelled regardless of success', () => {
    assert.equal(classifyHookOutcome(true, true), 'cancelled');
    assert.equal(classifyHookOutcome(false, true), 'cancelled');
  });
  it('only ever returns a value from HOOK_LIFECYCLE_OUTCOMES', () => {
    for (const s of [true, false]) {
      for (const a of [true, false]) {
        assert.ok((HOOK_LIFECYCLE_OUTCOMES as readonly string[]).includes(classifyHookOutcome(s, a)));
      }
    }
    assert.deepEqual([...HOOK_LIFECYCLE_OUTCOMES], ['success', 'error', 'cancelled']);
  });
});

// ---------------------------------------------------------------------------
// 1. computeHookId — stable deterministic id
// ---------------------------------------------------------------------------
describe('U007/w103 — computeHookId', () => {
  it('is deterministic for identical inputs', () => {
    assert.equal(
      computeHookId('preToolUse', 'command', 'echo hi', 0),
      computeHookId('preToolUse', 'command', 'echo hi', 0),
    );
  });
  it('distinguishes index, event, type and content', () => {
    const base = computeHookId('preToolUse', 'command', 'echo hi', 0);
    assert.notEqual(base, computeHookId('preToolUse', 'command', 'echo hi', 1));
    assert.notEqual(base, computeHookId('postToolUse', 'command', 'echo hi', 0));
    assert.notEqual(base, computeHookId('preToolUse', 'prompt', 'echo hi', 0));
    assert.notEqual(base, computeHookId('preToolUse', 'command', 'echo bye', 0));
  });
  it('is a short non-empty token', () => {
    const id = computeHookId('stop', 'command', 'true', 0);
    assert.ok(id.length > 0 && id.length <= 32);
  });
});

// ---------------------------------------------------------------------------
// 3. startHookProgressInterval — gated, deduped, stoppable poll
// ---------------------------------------------------------------------------
describe('U007/w103 — startHookProgressInterval', () => {
  it('returns a no-op (and never polls) when the event is gated off', async () => {
    let polled = false;
    const stop = startHookProgressInterval({
      event: 'preToolUse',         // not on the always-emit allowlist
      allEnabled: false,           // and all-events disabled → gated off
      getOutput: async () => { polled = true; return { stdout: '', stderr: '', output: '' }; },
      onProgress: () => {},
      intervalMs: 5,
    });
    await new Promise(r => setTimeout(r, 30));
    stop();
    assert.equal(polled, false, 'gated-off interval must never poll');
  });

  it('emits on output change and dedups identical output', async () => {
    const seen: Array<{ stdout: string; output: string }> = [];
    let out = 'one';
    const stop = startHookProgressInterval({
      event: 'sessionStart',       // always-emit allowlist
      allEnabled: false,
      getOutput: async () => ({ stdout: out, stderr: '', output: out }),
      onProgress: (o) => seen.push({ stdout: o.stdout, output: o.output }),
      intervalMs: 5,
    });
    await new Promise(r => setTimeout(r, 25));
    out = 'two';
    await new Promise(r => setTimeout(r, 25));
    stop();
    // 'one' once, then 'two' once — no repeated identical emissions.
    const outputs = seen.map(s => s.output);
    assert.ok(outputs.includes('one'), 'first output surfaced');
    assert.ok(outputs.includes('two'), 'changed output surfaced');
    assert.equal(outputs.filter(o => o === 'one').length, 1, 'first output not re-emitted');
  });

  it('stop() halts further polling', async () => {
    let calls = 0;
    const stop = startHookProgressInterval({
      event: 'sessionStart',
      allEnabled: true,
      getOutput: async () => { calls++; return { stdout: String(calls), stderr: '', output: String(calls) }; },
      onProgress: () => {},
      intervalMs: 5,
    });
    await new Promise(r => setTimeout(r, 25));
    stop();
    const after = calls;
    await new Promise(r => setTimeout(r, 25));
    assert.equal(calls, after, 'no polling after stop()');
  });

  it('exposes a default cadence constant', () => {
    assert.equal(typeof HOOK_PROGRESS_INTERVAL_MS, 'number');
    assert.ok(HOOK_PROGRESS_INTERVAL_MS > 0);
  });
});

// ---------------------------------------------------------------------------
// 1 + 2. hookId/hookName + outcome on the live EventEmitter channel (sync)
// ---------------------------------------------------------------------------
describe('U007/w103 — per-hook id/name + outcome on sync command hook', () => {
  it('progress + resolved carry a stable hookId and hookName; resolved has outcome=success', async () => {
    const progress: HookProgressEvent[] = [];
    const resolved: HookProgressEvent[] = [];
    hm.on('hookProgress', (p: HookProgressEvent) => progress.push(p));
    hm.on('hookResolved', (p: HookProgressEvent) => resolved.push(p));

    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'true', statusMessage: 'Guard' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executeSessionStart(sessCtx());
    fs.rmSync(tmpDir, { recursive: true, force: true });

    assert.equal(progress.length, 1);
    assert.equal(resolved.length, 1);
    assert.ok(progress[0].hookId && progress[0].hookId.length > 0, 'progress carries hookId');
    assert.equal(progress[0].hookName, 'Guard', 'hookName = display label (statusMessage)');
    assert.equal(progress[0].hookId, resolved[0].hookId, 'same hook → same id across progress/resolved');
    const out: HookLifecycleOutcome | undefined = resolved[0].outcome;
    assert.equal(out, 'success', 'clean exit → success outcome');
  });

  it('a failing command hook resolves with outcome=error', async () => {
    const resolved: HookProgressEvent[] = [];
    hm.on('hookResolved', (p: HookProgressEvent) => resolved.push(p));

    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'exit 3' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executeSessionStart(sessCtx());
    fs.rmSync(tmpDir, { recursive: true, force: true });

    assert.equal(resolved.length, 1);
    assert.equal(resolved[0].outcome, 'error', 'non-zero exit → error outcome');
  });
});

// ---------------------------------------------------------------------------
// 4. Live streaming — incremental stdout on the lifecycle channel (e2e)
// ---------------------------------------------------------------------------
describe('U007/w103 — live hook progress streaming', () => {
  it('publishes an incremental progress lifecycle event with stdout while the hook runs', async () => {
    const events: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => events.push(e));

    // Print a line, pause long enough for the progress interval to fire, then
    // finish — so the mid-run poll observes partial stdout before resolve.
    const pause = (HOOK_PROGRESS_INTERVAL_MS * 3) / 1000; // seconds
    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: `printf 'streamed-line\\n'; sleep ${pause}; printf 'done\\n'` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executeSessionStart(sessCtx());
    fs.rmSync(tmpDir, { recursive: true, force: true });

    const streaming = events.filter(e => e.phase === 'progress' && typeof e.stdout === 'string' && e.stdout.includes('streamed-line'));
    assert.ok(streaming.length >= 1, `expected >=1 incremental progress event with partial stdout, got events: ${JSON.stringify(events)}`);
    // Resolved comes after, with a terminal outcome.
    const resolved = events.filter(e => e.phase === 'resolved');
    assert.equal(resolved.length, 1);
    assert.equal(resolved[0].outcome, 'success');
  });
});
