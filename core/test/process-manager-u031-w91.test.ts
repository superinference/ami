import { describe, it, afterEach, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  ProcessManager,
  pmTraceHooks,
  type PmLifecyclePhase,
} from '../src/process-manager';

// ---------------------------------------------------------------------------
// U031 / w91: per-foreground-subagent abort registry.
//
// Parity with Claude Code's InProcessTeammateDetailDialog 'x' → onKill, which can
// abort ANY running teammate — foreground OR background. A foreground subagent
// blocks the parent turn and is NOT a tracked background row, so kill() (which only
// targets `processes` entries) can never reach it; the only recourse used to be
// Ctrl+C aborting the WHOLE turn. registerForegroundAbort / hasForegroundAbort /
// abortForeground / unregisterForegroundAbort let a frontend abort ONE such subagent.
//
// Red-green: reverting the registry (any of the four methods) fails the matching
// cases below — e.g. abortForeground would not exist / never invoke the callback.
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-w91-'));
  tmpDirs.push(d);
  return d;
}
function makePm(): ProcessManager {
  return new ProcessManager(makeTmpDir());
}

after(() => {
  for (const d of tmpDirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('ProcessManager – foreground-subagent abort registry', () => {
  let pm: ProcessManager | null = null;
  afterEach(() => { pm?.cleanup(); pm = null; });

  it('registers a foreground subagent as individually abortable', () => {
    pm = makePm();
    assert.equal(pm.hasForegroundAbort('agent-1'), false);
    pm.registerForegroundAbort('agent-1', () => { /* noop */ });
    assert.equal(pm.hasForegroundAbort('agent-1'), true);
  });

  it('abortForeground invokes the stored callback and returns true', () => {
    pm = makePm();
    let aborted = 0;
    pm.registerForegroundAbort('agent-1', () => { aborted++; });
    assert.equal(pm.abortForeground('agent-1'), true);
    assert.equal(aborted, 1);
  });

  it('abortForeground is one-shot: a second press is a no-op (returns false)', () => {
    pm = makePm();
    let aborted = 0;
    pm.registerForegroundAbort('agent-1', () => { aborted++; });
    assert.equal(pm.abortForeground('agent-1'), true);
    assert.equal(pm.hasForegroundAbort('agent-1'), false);
    assert.equal(pm.abortForeground('agent-1'), false);
    assert.equal(aborted, 1, 'the callback must fire exactly once');
  });

  it('abortForeground on an unknown id returns false and fires nothing', () => {
    pm = makePm();
    assert.equal(pm.abortForeground('does-not-exist'), false);
  });

  it('re-registering the same id replaces the prior callback (resumed subagent reuses its taskId)', () => {
    pm = makePm();
    let first = 0; let second = 0;
    pm.registerForegroundAbort('agent-1', () => { first++; });
    pm.registerForegroundAbort('agent-1', () => { second++; });
    assert.equal(pm.abortForeground('agent-1'), true);
    assert.equal(first, 0, 'the stale callback must not fire');
    assert.equal(second, 1);
  });

  it('swallows a throwing callback (controller already aborted) and still returns true', () => {
    pm = makePm();
    pm.registerForegroundAbort('agent-1', () => { throw new Error('already aborted'); });
    assert.doesNotThrow(() => {
      assert.equal(pm!.abortForeground('agent-1'), true);
    });
    // One-shot even on the throwing path: the entry was removed before the throw.
    assert.equal(pm.hasForegroundAbort('agent-1'), false);
  });

  it('unregisterForegroundAbort removes an entry so it can no longer be aborted', () => {
    pm = makePm();
    let aborted = 0;
    pm.registerForegroundAbort('agent-1', () => { aborted++; });
    pm.unregisterForegroundAbort('agent-1');
    assert.equal(pm.hasForegroundAbort('agent-1'), false);
    assert.equal(pm.abortForeground('agent-1'), false);
    assert.equal(aborted, 0);
  });

  it('unregisterForegroundAbort on an unknown id is a safe no-op', () => {
    pm = makePm();
    assert.doesNotThrow(() => pm!.unregisterForegroundAbort('nope'));
  });

  it('abortForeground records a foreground kill lifecycle trace (introspection parity)', () => {
    pm = makePm();
    const seen: Array<{ phase: PmLifecyclePhase; taskId: string; info?: Record<string, unknown> }> = [];
    const prev = pmTraceHooks.onLifecycle;
    pmTraceHooks.onLifecycle = (phase, taskId, info) => { seen.push({ phase, taskId, info }); };
    try {
      pm.registerForegroundAbort('agent-1', () => { /* noop */ });
      pm.abortForeground('agent-1');
    } finally {
      pmTraceHooks.onLifecycle = prev;
    }
    const trace = seen.find(s => s.taskId === 'agent-1' && s.phase === 'kill');
    assert.ok(trace, 'expected a kill lifecycle trace for the foreground abort');
    assert.equal(trace!.info?.foreground, true);
  });

  it('cleanup() drops all foreground-abort registrations without firing them', () => {
    pm = makePm();
    let aborted = 0;
    pm.registerForegroundAbort('agent-1', () => { aborted++; });
    pm.registerForegroundAbort('agent-2', () => { aborted++; });
    pm.cleanup();
    assert.equal(pm.hasForegroundAbort('agent-1'), false);
    assert.equal(pm.hasForegroundAbort('agent-2'), false);
    assert.equal(aborted, 0, 'cleanup must not invoke the callbacks — the turn abort owns teardown');
    pm = null; // already cleaned
  });
});
