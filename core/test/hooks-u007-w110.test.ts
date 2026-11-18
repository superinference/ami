/**
 * U007 / w110 — hooks.ts parity additions:
 *
 *  - Parallel OBSERVATIONAL hook execution (gap: "parallel hook execution with
 *    per-hook timeouts"). Claude Code fans an event's hooks out in parallel
 *    (matchingHooks.map(async …), utils/hooks.ts L2142); AMI had awaited each
 *    fire-and-forget hook serially in a for-loop so N independent notification/
 *    idle/file-change hooks stacked their latency. {@link
 *    HookManager.runObservationalHooksConcurrently} now overlaps them while keeping
 *    per-hook error isolation (one throwing never cancels its siblings).
 *
 *  - FileChanged / CwdChanged notify channel (gaps: "hook result notify channel
 *    systemMessages surfaced" + "surfacing hook systemMessages and failed hook
 *    output error vs non-error"). A watcher hook's USER-facing `systemMessage` is
 *    forwarded on a `hookNotify` event (isError=false) and a failed watcher hook's
 *    error on the same channel (isError=true) — Claude Code's fileChangedWatcher
 *    `setEnvHookNotifier((text, isError) => …)` (fileChangedWatcher.ts L90-L105,
 *    L162-L169). Previously the stdout/systemMessage and failures were dropped.
 *
 *  - Structured-output enforcement nudge (gap: "stop-hook enforcement nudge when
 *    the required structured-output tool was not called"). Claude Code's
 *    registerStructuredOutputEnforcement (hookHelpers.ts L66-L83) re-prompts a run
 *    that stopped without calling the output tool. AMI adds an opt-in
 *    `enforceStructuredOutput` on agent hooks: an agent hook that stops without a
 *    `verification_output` verdict is re-prompted ONCE with the exact nudge.
 *
 * HOME is isolated so real ~/.ami / ~/.claude hooks never leak in.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  buildStructuredOutputEnforcementNudge,
  type NotificationContext,
  type FileChangeContext,
} from '../src/hooks';

describe('HookManager — parallel observational hook execution (w110)', () => {
  it('runs fan-out notification hooks CONCURRENTLY, not serially', async () => {
    const hm = new HookManager();
    const order: string[] = [];
    const mk = (name: string, ms: number) => async (_ctx: NotificationContext) => {
      order.push(`${name}:start`);
      await new Promise((r) => setTimeout(r, ms));
      order.push(`${name}:end`);
    };
    hm.onNotification(mk('a', 60));
    hm.onNotification(mk('b', 60));
    hm.onNotification(mk('c', 60));

    const started = Date.now();
    await hm.executeNotification({ message: 'hi' });
    const elapsed = Date.now() - started;

    // Serial would be ~180ms (3×60); parallel overlaps to ~60ms. A generous ceiling
    // (< 150ms) discriminates parallel from serial without flaking on slow CI.
    assert.ok(elapsed < 150, `expected concurrent (~60ms), got ${elapsed}ms (serial would be ~180ms)`);
    // All three started before any finished — proves true overlap, not fast serial.
    assert.deepEqual(order.slice(0, 3).sort(), ['a:start', 'b:start', 'c:start']);
    assert.equal(order.filter((o) => o.endsWith(':end')).length, 3, 'all three hooks completed');
  });

  it('isolates a throwing observational hook — siblings still run', async () => {
    const hm = new HookManager();
    let ok1 = false;
    let ok2 = false;
    hm.onNotification(async () => { ok1 = true; });
    hm.onNotification(async () => { throw new Error('boom'); });
    hm.onNotification(async () => { ok2 = true; });
    // Must not reject even though one hook throws (per-hook isolation).
    await assert.doesNotReject(hm.executeNotification({ message: 'x' }));
    assert.ok(ok1 && ok2, 'both non-throwing hooks ran despite the sibling throwing');
  });
});

describe('HookManager — FileChanged/CwdChanged notify channel (w110)', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w110-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-home-'));
    origHome = process.env.HOME;
    process.env.HOME = home;
  });
  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  function writeFileChangedHook(command: string): void {
    const dir = path.join(tmp, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'fileChanged', matcher: 'watched.txt', hook: { type: 'command', command, timeout: 5000 } }] }),
    );
  }

  it('surfaces a FileChanged hook systemMessage on hookNotify with isError=false', async () => {
    writeFileChangedHook(`printf '{"systemMessage":"watched file changed"}\\n'`);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    const notes: Array<{ event: string; message: string; isError: boolean }> = [];
    hm.on('hookNotify', (d) => notes.push(d));

    await hm.executeFileChanged({ filePath: path.join(tmp, 'watched.txt'), event: 'change' });

    const sys = notes.find((n) => n.message.includes('watched file changed'));
    assert.ok(sys, `expected a systemMessage notify, got ${JSON.stringify(notes)}`);
    assert.equal(sys.isError, false, 'a systemMessage is a non-error notice');
    assert.equal(sys.event, 'fileChanged');
  });

  it('surfaces a THROWING watcher hook error on hookNotify with isError=true', async () => {
    const hm = new HookManager();
    // A raw watcher hook that throws routes through emitWatcherHookError.
    hm.onFileChanged(async (_ctx: FileChangeContext) => { throw new Error('watcher blew up'); });
    const notes: Array<{ event: string; message: string; isError: boolean }> = [];
    const errs: Array<{ error: string }> = [];
    hm.on('hookNotify', (d) => notes.push(d));
    hm.on('hookError', (d) => errs.push(d));

    await hm.executeFileChanged({ filePath: '/tmp/x', event: 'change' });

    const errNote = notes.find((n) => n.isError);
    assert.ok(errNote, 'a failed watcher hook surfaces an error notice');
    assert.match(errNote.message, /watcher blew up/);
    // Still ALSO reported on the operator hookError channel (not a regression).
    assert.equal(errs.length, 1);
  });

  it('emits no notify for an observe-only (silent) watcher hook', async () => {
    const hm = new HookManager();
    hm.onFileChanged(async () => { /* observe only, no output, no throw */ });
    const notes: unknown[] = [];
    hm.on('hookNotify', (d) => notes.push(d));
    await hm.executeFileChanged({ filePath: '/tmp/x', event: 'change' });
    assert.equal(notes.length, 0, 'a silent watcher hook emits no spurious notice');
  });
});

describe('buildStructuredOutputEnforcementNudge (w110)', () => {
  it('names the tool verbatim (Claude Code hookHelpers.ts wording)', () => {
    assert.equal(
      buildStructuredOutputEnforcementNudge('verification_output'),
      'You MUST call the verification_output tool to complete this request. Call this tool now.',
    );
  });
  it('defaults to the verification tool AMI advertises to hook sub-engines', () => {
    assert.match(buildStructuredOutputEnforcementNudge(), /verification_output/);
  });
});

describe('HookManager — agent hook structured-output enforcement re-prompt (w110)', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w110a-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-home-'));
    origHome = process.env.HOME;
    process.env.HOME = home;
  });
  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  function writeAgentHook(enforce: boolean): void {
    const dir = path.join(tmp, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, 'hooks.json'),
      JSON.stringify({
        hooks: [{ event: 'preToolUse', hook: { type: 'agent', prompt: 'verify', timeout: 5, enforceStructuredOutput: enforce } }],
      }),
    );
  }

  /**
   * Mock engine whose submits are scripted: `scripts[i]` drives the i-th submit().
   * Each script is an array of events to yield. Records every prompt submitted.
   */
  function scriptedEngine(scripts: Array<Array<Record<string, unknown>>>, prompts: string[]) {
    let call = 0;
    return () => ({
      async *submit(p: string) {
        prompts.push(p);
        const script = scripts[Math.min(call, scripts.length - 1)];
        call++;
        for (const ev of script) yield ev as { type: string } & Record<string, unknown>;
      },
      shutdown() {},
    });
  }

  const preCtx = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

  it('re-prompts ONCE with the nudge when an agent hook stops without a verdict', async () => {
    writeAgentHook(true);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    const prompts: string[] = [];
    // 1st submit: only free text (no verification tool). 2nd submit: the verdict.
    hm.setEngineFactory(scriptedEngine(
      [
        [{ type: 'text_delta', text: 'thinking out loud, no verdict' }, { type: 'done', exitReason: 'end_turn' }],
        [{ type: 'tool_use_start', toolName: 'verification_output', input: { ok: false, reason: 'condition failed' } }, { type: 'done', exitReason: 'end_turn' }],
      ],
      prompts,
    ));

    const decision = await hm.executePreToolUse(preCtx);

    assert.equal(prompts.length, 2, 'the enforcement nudge drove exactly one re-prompt');
    assert.equal(prompts[1], buildStructuredOutputEnforcementNudge(), 'the 2nd submit is the verbatim nudge');
    assert.equal(decision.action, 'deny', 'the re-prompt verdict (ok:false) now blocks the tool');
    assert.match(decision.action === 'deny' ? decision.reason : '', /condition failed/);
  });

  it('does NOT re-prompt when the agent hook already returned a verdict', async () => {
    writeAgentHook(true);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    const prompts: string[] = [];
    hm.setEngineFactory(scriptedEngine(
      [[{ type: 'tool_use_start', toolName: 'verification_output', input: { ok: true } }, { type: 'done', exitReason: 'end_turn' }]],
      prompts,
    ));

    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(prompts.length, 1, 'a verdict on the first run needs no nudge');
    assert.equal(decision.action, 'allow');
  });

  it('does NOT re-prompt when enforceStructuredOutput is off (default)', async () => {
    writeAgentHook(false);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    const prompts: string[] = [];
    hm.setEngineFactory(scriptedEngine(
      [[{ type: 'text_delta', text: 'no verdict, free text only' }, { type: 'done', exitReason: 'end_turn' }]],
      prompts,
    ));

    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(prompts.length, 1, 'without the opt-in flag there is no enforcement re-prompt');
    assert.equal(decision.action, 'allow', 'free-text output stays non-blocking');
  });
});
