/**
 * U007 / w76 — Stop-hook run summary + malformed-config surfacing (parity).
 *
 * Two confirmed the reference parity gaps, both OPERATOR-facing (never fed to the
 * model):
 *
 *  A. Stop-hook summary (stopHooks.ts createStopHookSummaryMessage / hookInfos):
 *     after a Stop-hook chain, Claude builds a per-hook timing + error summary
 *     ("Ran N stop hook(s)", wall-clock total, per-hook failures, prevented
 *     continuation) and yields a user-visible "Stop hook failed: ..." warning on
 *     an error. AMI's executeStop previously only swallowed per-hook errors into
 *     the debug log with NO per-hook duration, NO aggregate, and NO summary.
 *     Now executeStop captures per-hook durationMs + errors + the wall-clock total
 *     and emits a `stopHookSummary` event (retained via getLastStopHookSummary).
 *     RED without the change: the `stopHookSummary` event never fires and
 *     getLastStopHookSummary() stays null.
 *
 *  C. Malformed settings/hooks/config JSON (updateConfig.ts "silently disables
 *     ALL settings from that file"): a broken .ami/hooks.json / .ami/config.json
 *     silently dropped every hook it declared and only hit coreLog (a temp file
 *     the user never sees). Now loadFromFile records each parse failure, exposes
 *     it via getConfigErrors(), and emits a `configError` event. RED without the
 *     change: getConfigErrors() does not exist / returns empty.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  HOOK_TIMING_DISPLAY_THRESHOLD_MS,
  type HookContext,
  type StopHookSummary,
  type HookConfigError,
} from '../src/hooks';

function makeContext(overrides: Partial<HookContext> = {}): HookContext {
  return { messages: [{ role: 'user', content: 'hi' }], turnCount: 1, ...overrides };
}

function tmpDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-w76-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  return dir;
}

describe('U007/w76 — Stop-hook run summary', () => {
  it('captures per-hook durations + errors and emits stopHookSummary', async () => {
    const hm = new HookManager();
    hm.onStop(async () => ({})); // success
    hm.onStop(async () => { throw new Error('boom'); }); // failure

    let emitted: StopHookSummary | undefined;
    hm.on('stopHookSummary', (s: StopHookSummary) => { emitted = s; });

    const result = await hm.executeStop(makeContext());

    // The aggregate return shape is unchanged (additive-only; no existing consumer
    // breaks): a thrown Stop hook is not a blocking error.
    assert.deepEqual(result.blockingErrors, []);

    assert.ok(emitted, 'stopHookSummary event must fire after a Stop-hook chain');
    assert.equal(emitted!.count, 2);
    assert.equal(emitted!.hookInfos.length, 2);
    // First hook succeeded (no error), second failed (error recorded on its info).
    assert.equal(emitted!.hookInfos[0].error, undefined);
    assert.equal(emitted!.hookInfos[1].error, 'boom');
    assert.deepEqual(emitted!.hookErrors, ['boom']);
    assert.equal(emitted!.preventContinuation, false);
    assert.ok(emitted!.totalDurationMs >= 0);
    for (const info of emitted!.hookInfos) assert.ok(info.durationMs >= 0);

    // Retained for introspection; returns a deep copy (mutating it must not alter
    // the manager's retained summary).
    const retained = hm.getLastStopHookSummary();
    assert.ok(retained);
    assert.equal(retained!.count, 2);
    retained!.hookErrors.push('tamper');
    assert.deepEqual(hm.getLastStopHookSummary()!.hookErrors, ['boom']);
  });

  it('records preventContinuation + stopReason in the summary', async () => {
    const hm = new HookManager();
    hm.onStop(async () => ({ preventContinuation: true, stopReason: 'keep going' }));
    let emitted: StopHookSummary | undefined;
    hm.on('stopHookSummary', (s: StopHookSummary) => { emitted = s; });

    await hm.executeStop(makeContext());
    assert.ok(emitted);
    assert.equal(emitted!.preventContinuation, true);
    assert.equal(emitted!.stopReason, 'keep going');
    assert.deepEqual(emitted!.hookErrors, []);
  });

  it('does NOT emit a summary (and leaves getLastStopHookSummary null) with no hooks', async () => {
    const hm = new HookManager();
    let fired = false;
    hm.on('stopHookSummary', () => { fired = true; });
    const result = await hm.executeStop(makeContext());
    assert.equal(fired, false);
    assert.equal(hm.getLastStopHookSummary(), null);
    assert.deepEqual(result, { blockingErrors: [], preventContinuation: false, stopReason: undefined });
  });

  it('exports the display-threshold parity constant', () => {
    assert.equal(typeof HOOK_TIMING_DISPLAY_THRESHOLD_MS, 'number');
    assert.ok(HOOK_TIMING_DISPLAY_THRESHOLD_MS > 0);
  });
});

describe('U007/w76 — malformed hook/config file surfacing', () => {
  it('records a malformed .ami/hooks.json via getConfigErrors + emits configError', async () => {
    const dir = tmpDir();
    fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), '{ this is not json ]');

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    const events: HookConfigError[] = [];
    hm.on('configError', (e: HookConfigError) => { events.push(e); });

    hm.loadFromFile(dir);

    const errs = hm.getConfigErrors();
    assert.equal(errs.length, 1);
    assert.equal(errs[0].path, path.join(dir, '.ami', 'hooks.json'));
    assert.ok(errs[0].error.length > 0);
    // Live event fired too (so a mid-session reload consumer can surface it).
    assert.equal(events.length, 1);
    assert.equal(events[0].path, errs[0].path);

    // Copy semantics: mutating the returned list must not corrupt the retained set.
    errs.push({ path: 'x', error: 'y' });
    assert.equal(hm.getConfigErrors().length, 1);

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('records a malformed .ami/config.json', async () => {
    const dir = tmpDir();
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), '{ broken ');

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    const errs = hm.getConfigErrors();
    assert.ok(errs.some(e => e.path === path.join(dir, '.ami', 'config.json')));

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('clears prior parse errors on a subsequent clean reload', async () => {
    const dir = tmpDir();
    const hooksPath = path.join(dir, '.ami', 'hooks.json');
    fs.writeFileSync(hooksPath, '{ nope');

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    assert.equal(hm.getConfigErrors().length, 1);

    // Fix the file and reload — the stale error must be gone.
    fs.writeFileSync(hooksPath, JSON.stringify({ hooks: [] }));
    hm.loadFromFile(dir);
    assert.equal(hm.getConfigErrors().length, 0);

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('reports no config errors for a well-formed hooks.json', async () => {
    const dir = tmpDir();
    fs.writeFileSync(
      path.join(dir, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'stop', hook: { type: 'command', command: 'true' } }] }),
    );
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    assert.deepEqual(hm.getConfigErrors(), []);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
