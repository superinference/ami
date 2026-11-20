/**
 * U007 / w97 — Deliver a COMPLETED background ('async') hook's result back into
 * the turn (the reference parity: `checkForAsyncHookResponses`).
 *
 * AMI backgrounds `async:true` hooks fire-and-forget: the turn returns `allow`
 * immediately and the hook's child runs to completion unobserved. Before this
 * change, a backgrounded hook that exited 0 with a meaningful result
 * (`additionalContext`, `systemMessage`, or plain stdout) had that result
 * SILENTLY DISCARDED — only a DENY (exit 2) / engine-error re-woke the turn.
 *
 * Now, when a backgrounded hook opts into the re-wake channel (`asyncRewake`),
 * its completed success/non-blocking output is decoded once
 * ({@link asyncRewakeDeliveryMessage}) and re-injected via the existing
 * `hookRewake` event the engine turns into a `[hookRewake:<event>]` system
 * message — delivery parity with checkForAsyncHookResponses, through AMI's
 * push model instead of a poller.
 *
 * RED discriminators (fail WITHOUT the delivery wiring in fireAsyncHook):
 *  - a background hook emitting `{"systemMessage":...}` produces NO rewake.
 *  - a background hook emitting `{"additionalContext":...}` produces NO rewake.
 *  - a background hook emitting plain stdout produces NO rewake.
 * GREEN also guards against regressions:
 *  - a DENY (exit 2) hook still produces EXACTLY ONE rewake (no double-fire).
 *  - a background hook WITHOUT asyncRewake stays fire-and-forget (no rewake).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, asyncRewakeDeliveryMessage } from '../src/hooks';

/** Write a <cwd>/.ami/hooks.json with one preToolUse command hook. */
function writeHook(cwd: string, hook: Record<string, unknown>): void {
  fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
  fs.writeFileSync(
    path.join(cwd, '.ami', 'hooks.json'),
    JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', ...hook } }] }),
  );
}

type Rewake = { event: string; message: string; pluginName?: string; pluginId?: string };

async function fireAndCollect(cwd: string): Promise<Rewake[]> {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(cwd);
  const rewakes: Rewake[] = [];
  hm.on('hookRewake', (e: Rewake) => rewakes.push(e));
  const decision = await hm.executePreToolUse({
    messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' },
  });
  // A backgrounded hook never blocks the turn — it allows immediately.
  assert.equal(decision.action, 'allow');
  // Await the background hook to settle, which is when delivery fires.
  await hm.finalizePendingAsyncHooks();
  return rewakes;
}

describe('asyncRewakeDeliveryMessage (pure decoder)', () => {
  it('returns undefined for empty / whitespace / DENY outputs (already re-woke)', () => {
    assert.equal(asyncRewakeDeliveryMessage(''), undefined);
    assert.equal(asyncRewakeDeliveryMessage('   '), undefined);
    assert.equal(asyncRewakeDeliveryMessage('DENY:blocked'), undefined);
  });

  it('decodes SYSTEM: into the message, undefined when empty', () => {
    assert.equal(asyncRewakeDeliveryMessage('SYSTEM:heads up'), 'heads up');
    assert.equal(asyncRewakeDeliveryMessage('SYSTEM:   '), undefined);
  });

  it('decodes ASK: into an approval notice (with and without a reason)', () => {
    assert.equal(asyncRewakeDeliveryMessage('ASK:confirm this'), 'Requires approval: confirm this');
    assert.equal(asyncRewakeDeliveryMessage('ASK:'), 'Requires approval');
  });

  it('decodes MODIFY additionalContext, updatedInput, and malformed payloads', () => {
    assert.equal(asyncRewakeDeliveryMessage('MODIFY:{"additionalContext":"ctx here"}'), 'ctx here');
    assert.equal(
      asyncRewakeDeliveryMessage('MODIFY:{"command":"ls -la"}'),
      'Updated tool input: {"command":"ls -la"}',
    );
    // additionalContext present but blank → treated as an updated-input notice.
    assert.equal(
      asyncRewakeDeliveryMessage('MODIFY:{"additionalContext":"   "}'),
      'Updated tool input: {"additionalContext":"   "}',
    );
    assert.equal(asyncRewakeDeliveryMessage('MODIFY:{bad json'), undefined);
  });

  it('passes plain-text output through verbatim', () => {
    assert.equal(asyncRewakeDeliveryMessage('all clear'), 'all clear');
  });
});

describe('async background hook delivers its completed result (U007/w97)', () => {
  it('delivers a background hook systemMessage via hookRewake', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w97-sys-'));
    writeHook(cwd, {
      command: `printf '%s' '{"systemMessage":"touching a protected file"}'`,
      async: true,
      asyncRewake: true,
    });
    const rewakes = await fireAndCollect(cwd);
    assert.equal(rewakes.length, 1, 'completed background hook must re-wake once');
    assert.equal(rewakes[0].event, 'command');
    assert.equal(rewakes[0].message, 'touching a protected file');
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('delivers a background hook additionalContext via hookRewake', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w97-ctx-'));
    writeHook(cwd, {
      command: `printf '%s' '{"additionalContext":"scan: clean"}'`,
      async: true,
      asyncRewake: true,
    });
    const rewakes = await fireAndCollect(cwd);
    assert.equal(rewakes.length, 1);
    assert.equal(rewakes[0].message, 'scan: clean');
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('delivers a background hook plain-text stdout via hookRewake', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w97-plain-'));
    writeHook(cwd, {
      command: `printf '%s' 'background scan finished'`,
      async: true,
      asyncRewake: true,
    });
    const rewakes = await fireAndCollect(cwd);
    assert.equal(rewakes.length, 1);
    assert.equal(rewakes[0].message, 'background scan finished');
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('does NOT double-fire for a DENY (exit 2) background hook', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w97-deny-'));
    writeHook(cwd, {
      command: 'echo blocked-reason >&2; exit 2',
      async: true,
      asyncRewake: true,
    });
    const rewakes = await fireAndCollect(cwd);
    // Exit 2 already re-woke at its source; delivery must not add a second.
    assert.equal(rewakes.length, 1, 'DENY must re-wake exactly once, not twice');
    assert.match(rewakes[0].message, /blocked-reason/);
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('a suppressOutput background hook delivers nothing', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w97-suppress-'));
    writeHook(cwd, {
      command: `printf '%s' '{"suppressOutput":true}'`,
      async: true,
      asyncRewake: true,
    });
    const rewakes = await fireAndCollect(cwd);
    assert.equal(rewakes.length, 0, 'a decision-less suppressed result carries nothing to deliver');
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('a background hook WITHOUT asyncRewake stays fire-and-forget (no delivery)', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w97-noopt-'));
    writeHook(cwd, {
      command: `printf '%s' '{"systemMessage":"would warn"}'`,
      async: true,
      // no asyncRewake — the user explicitly backgrounded it without opting in.
    });
    const rewakes = await fireAndCollect(cwd);
    assert.equal(rewakes.length, 0, 'without asyncRewake a completed background hook does not re-wake');
    fs.rmSync(cwd, { recursive: true, force: true });
  });
});
