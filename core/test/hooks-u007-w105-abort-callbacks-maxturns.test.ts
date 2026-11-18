import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, MAX_AGENT_TURNS } from '../src/hooks';

// ---------------------------------------------------------------------------
// U007 / w105
//
// Gap 13 — AbortSignal passed into in-process ('callback') hooks for cooperative
//          cancellation (Claude Code's HookCallback `abort` arg).
// Gap 24 — Max-turns introspection/diagnostic distinct from a timeout for agent
//          hooks (Claude Code's "Agent hook did not complete within N turns").
// ---------------------------------------------------------------------------

const tmpDirs: string[] = [];
function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-hooks-w105-'));
  tmpDirs.push(dir);
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} }
});

function preCtx(toolName: string) {
  return { messages: [], turnCount: 0, toolName, toolInput: {} } as any;
}

describe('gap13: AbortSignal threaded into executeCallbacks callbacks', () => {
  it('passes the abort signal as the callback third argument', async () => {
    const hm = new HookManager();
    const ac = new AbortController();
    let seen: AbortSignal | undefined | 'unset' = 'unset';
    hm.registerCallback('custom', async (_event, _data, abort) => { seen = abort; });
    await hm.executeCallbacks('custom', { x: 1 }, undefined, ac.signal);
    assert.strictEqual(seen, ac.signal, 'callback must receive the exact AbortSignal passed in');
  });

  it('runs no callbacks when the signal is already aborted', async () => {
    const hm = new HookManager();
    const ac = new AbortController();
    ac.abort();
    let ran = 0;
    hm.registerCallback('custom', async () => { ran++; });
    const result = await hm.executeCallbacks('custom', {}, undefined, ac.signal);
    assert.equal(ran, 0, 'a pre-aborted turn must not run any in-process callback');
    assert.equal(result, undefined);
  });

  it('stops awaiting a hung callback the instant the signal aborts (raceAbort)', async () => {
    const hm = new HookManager();
    const ac = new AbortController();
    let settled = false;
    // A callback that never resolves on its own — only the abort unblocks the await.
    hm.registerCallback('custom', () => new Promise<void>(() => {}), 60_000);
    const p = hm.executeCallbacks('custom', {}, undefined, ac.signal).then(() => { settled = true; });
    setTimeout(() => ac.abort(), 20);
    await p;
    assert.ok(settled, 'executeCallbacks must resolve once the abort fires, not hang on the callback');
  });

  it('skips remaining callbacks once a running callback aborts the turn', async () => {
    const hm = new HookManager();
    const ac = new AbortController();
    let secondRan = false;
    // First callback observes the abort and bails; it also aborts the turn so the
    // second callback is skipped by the pre-iteration guard.
    hm.registerCallback('custom', async (_e, _d, abort) => {
      ac.abort();
      assert.ok(abort?.aborted, 'the aborted signal is visible to the cooperative callback');
    });
    hm.registerCallback('custom', async () => { secondRan = true; });
    await hm.executeCallbacks('custom', {}, undefined, ac.signal);
    assert.equal(secondRan, false, 'callbacks after an abort are not consulted');
  });

  it('passes undefined abort when no signal is supplied (backward behavior preserved)', async () => {
    const hm = new HookManager();
    let observed: { event: string; data: unknown; abort: AbortSignal | undefined } | undefined;
    hm.registerCallback('custom', async (event, data, abort) => { observed = { event, data, abort }; });
    await hm.executeCallbacks('custom', { a: 1 });
    assert.deepEqual(observed, { event: 'custom', data: { a: 1 }, abort: undefined });
  });
});

describe('gap24: agent-hook max-turns diagnostic distinct from timeout', () => {
  function agentHookManager(events: Array<Record<string, unknown>>): { hm: HookManager; errors: Array<{ hookType: string; error: string }> } {
    const hm = new HookManager();
    const errors: Array<{ hookType: string; error: string }> = [];
    hm.on('hookError', (d: { hookType: string; hook: string; error: string }) => errors.push({ hookType: d.hookType, error: d.error }));
    hm.setEngineFactory(() => ({
      // eslint-disable-next-line require-yield
      async *submit() { for (const e of events) yield e as any; },
      shutdown() {},
    }));
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'agent', prompt: 'Evaluate this tool call' } }]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    return { hm, errors };
  }

  it('emits a distinct "did not complete within N turns" diagnostic when the sub-engine exhausts max_turns', async () => {
    const { hm, errors } = agentHookManager([
      { type: 'text_delta', text: 'still thinking' },
      { type: 'done', exitReason: 'max_turns' },
    ]);
    const decision = await hm.executePreToolUse(preCtx('bash'));
    // Non-blocking: the run stays allow (no verdict → no block), behavior preserved.
    assert.equal(decision.action, 'allow');
    const turnErr = errors.find(e => e.error.includes('did not complete within'));
    assert.ok(turnErr, 'a turn-budget exhaustion must surface on the hookError channel');
    assert.ok(turnErr!.error.includes(String(MAX_AGENT_TURNS)), 'the diagnostic names the turn cap');
    assert.ok(/exitReason: max_turns/.test(turnErr!.error), 'the diagnostic names the exit reason');
    assert.equal(turnErr!.hookType, 'agent');
  });

  it('does NOT emit the turn diagnostic when the agent hook completes normally', async () => {
    const { hm, errors } = agentHookManager([
      { type: 'text_delta', text: 'SYSTEM:looks fine' },
      { type: 'done', exitReason: 'completed' },
    ]);
    await hm.executePreToolUse(preCtx('bash'));
    assert.equal(errors.find(e => e.error.includes('did not complete within')), undefined,
      'a completed agent hook must not be reported as a turn-budget give-up');
  });

  it('does NOT emit the turn diagnostic when a verdict was produced before max_turns', async () => {
    const { hm, errors } = agentHookManager([
      { type: 'tool_use_start', toolName: 'verification_output', input: { ok: true } },
      { type: 'done', exitReason: 'max_turns' },
    ]);
    await hm.executePreToolUse(preCtx('bash'));
    assert.equal(errors.find(e => e.error.includes('did not complete within')), undefined,
      'a structured verdict means the hook completed regardless of the engine exit reason');
  });
});
