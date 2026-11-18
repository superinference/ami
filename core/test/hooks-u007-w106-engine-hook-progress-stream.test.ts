/**
 * Unit U007 (w106) — live progress streaming for ENGINE-BACKED hooks.
 *
 * Gap (confirmed): AMI's prompt/agent (engine-backed) hooks previously emitted
 * only a start (`hookProgress`) and an end (`hookResolved`) lifecycle event —
 * the live incremental-output streaming wired in w103 was gated to `command`
 * hooks only (`config.type === 'command'`). Claude Code's execPromptHook /
 * execAgentHook surface a LIVE progress affordance while the model streams
 * (`toolUseContext.setResponseLength(len => len + content.length)`), so an
 * operator sees the hook producing output instead of a silent multi-second hang.
 *
 * This extends the EXISTING lifecycle/progress infrastructure (no parallel
 * module, no duplication): `runArmHook` now allocates a `live` sink for
 * prompt/agent hooks too, `runEngineHook` mirrors the accumulating model text
 * into it on each `text_delta`, and the already-present `streamHookProgress`
 * poll publishes incremental `progress` lifecycle events for them — identical to
 * the command-hook path.
 *
 * RED on revert: with the wiring removed (`live` undefined for engine hooks /
 * the sink never updated), NO `progress`-phase lifecycle event with partial
 * stdout is published while a prompt/agent hook runs — the streaming assertions
 * below fail. The start/end events remain, so this discriminates the fix from
 * the old start+end-only behavior.
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  HOOK_PROGRESS_INTERVAL_MS,
  type PreToolUseContext,
  type HookLifecycleEvent,
} from '../src/hooks';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

function tmpCwdWithHook(type: 'prompt' | 'agent'): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-w106-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({
    hooks: [{ event: 'preToolUse', hook: { type, prompt: 'verify this', timeout: 30 } }],
  }));
  return dir;
}

/**
 * Engine stub that yields the chunks in order, pausing `pauseMs` BETWEEN each so
 * the mid-run progress poll (every {@link HOOK_PROGRESS_INTERVAL_MS}) observes
 * the first chunk mirrored into the live sink before the run completes.
 */
function mkStreamingEngine(chunks: string[], pauseMs: number) {
  return () => ({
    async *submit(_p: string) {
      for (let i = 0; i < chunks.length; i++) {
        if (i > 0) await new Promise(r => setTimeout(r, pauseMs));
        yield { type: 'text_delta', text: chunks[i] } as const;
      }
    },
    shutdown() {},
  });
}

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

describe('U007/w106 — live progress streaming for prompt hooks', () => {
  it('publishes an incremental progress lifecycle event with partial model text while the prompt hook runs', async () => {
    const events: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => events.push(e));

    const dir = tmpCwdWithHook('prompt');
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    // First chunk is mirrored into the live sink immediately; the pause lets the
    // 250ms poll fire and read it before the verdict chunk completes the run.
    hm.setEngineFactory(mkStreamingEngine(['evaluating the command…', ' {"ok":true}'], HOOK_PROGRESS_INTERVAL_MS * 3));

    await hm.executePreToolUse(preCtx);
    fs.rmSync(dir, { recursive: true, force: true });

    const streaming = events.filter(e =>
      e.phase === 'progress' && typeof e.stdout === 'string' && e.stdout.includes('evaluating the command'));
    assert.ok(
      streaming.length >= 1,
      `expected >=1 incremental progress event with partial model text, got: ${JSON.stringify(events.map(e => ({ phase: e.phase, stdout: e.stdout })))}`,
    );
    // The hookType on the streamed progress event is the engine-hook type.
    assert.equal(streaming[0].hookType, 'prompt');
    // A terminal resolved event still follows.
    const resolved = events.filter(e => e.phase === 'resolved');
    assert.equal(resolved.length, 1);
  });
});

describe('U007/w106 — live progress streaming for agent hooks', () => {
  it('publishes an incremental progress lifecycle event with partial model text while the agent hook runs', async () => {
    const events: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => events.push(e));

    const dir = tmpCwdWithHook('agent');
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory(mkStreamingEngine(['inspecting transcript…', ' {"ok":true}'], HOOK_PROGRESS_INTERVAL_MS * 3));

    await hm.executePreToolUse(preCtx);
    fs.rmSync(dir, { recursive: true, force: true });

    const streaming = events.filter(e =>
      e.phase === 'progress' && typeof e.stdout === 'string' && e.stdout.includes('inspecting transcript'));
    assert.ok(
      streaming.length >= 1,
      `expected >=1 incremental progress event for an agent hook, got: ${JSON.stringify(events.map(e => ({ phase: e.phase, stdout: e.stdout })))}`,
    );
    assert.equal(streaming[0].hookType, 'agent');
  });
});

describe('U007/w106 — dedup + no spurious streaming for non-streaming hooks', () => {
  it('does not re-emit identical partial output (dedup on output change)', async () => {
    const events: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => events.push(e));

    const dir = tmpCwdWithHook('prompt');
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    // One chunk, then a long pause → the poll sees the SAME output twice but
    // publishes it once (startHookProgressInterval dedup).
    hm.setEngineFactory(mkStreamingEngine(['stable-partial', ' {"ok":true}'], HOOK_PROGRESS_INTERVAL_MS * 4));

    await hm.executePreToolUse(preCtx);
    fs.rmSync(dir, { recursive: true, force: true });

    const withPartial = events.filter(e =>
      e.phase === 'progress' && typeof e.stdout === 'string' && e.stdout === 'stable-partial');
    assert.equal(withPartial.length, 1, 'identical partial output is published exactly once');
  });
});
