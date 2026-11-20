/**
 * U007 / w91 — two confirmed the reference parity gaps on core/core/src/hooks.ts,
 * both purely ADDITIVE (no existing behavior changes; no new cross-layer wiring):
 *
 *  A. StopHookInfo per-hook reporting fields `command` + `promptText`
 *     (query/stopHooks.ts:L198-253 — createStopHookSummaryMessage's hookInfos).
 *     Claude Code's per-hook stop summary identifies WHICH hook ran/failed by its
 *     command / prompt text, not just by an ordinal index. AMI's StopHookInfo
 *     previously carried only `durationMs` + `error`, so a failing/slow Stop hook
 *     in a multi-hook chain could not be attributed to its config. Now a Stop hook
 *     registered from config (or programmatically with a display descriptor) records
 *     its command/promptText, executeStop populates them on each StopHookInfo, and
 *     formatStopHookSummaryMessage surfaces the identifier in the per-hook row.
 *     RED without the change: `onStop` rejects the display arg / the summary's
 *     hookInfos carry no `command`.
 *
 *  B. Machine-readable field descriptions for hook output — the introspectable
 *     HOOK_JSON_SCHEMA_HINT (types/hooks.ts:L50-166 field descriptions; async OUTPUT
 *     response { async, asyncTimeout } — types/hooks.ts:L169-176). validateHookDecisionOutput
 *     already ACCEPTS `async`, `asyncTimeout`, `interrupt`, and `updatedPermissions`,
 *     but the schema hint shown to a user on a malformed-output error did not
 *     advertise them, so the introspection surface drifted from what the parser
 *     accepts. Now every field the validator type-checks appears in the hint.
 *     RED without the change: the hint omits `async` / `asyncTimeout` / `interrupt`.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  HOOK_JSON_SCHEMA_HINT,
  validateHookDecisionOutput,
  formatStopHookSummaryMessage,
  type HookContext,
  type StopHookSummary,
} from '../src/hooks';

function makeContext(overrides: Partial<HookContext> = {}): HookContext {
  return { messages: [{ role: 'user', content: 'hi' }], turnCount: 1, ...overrides };
}

describe('U007/w91 — StopHookInfo command + promptText attribution', () => {
  it('records a programmatic Stop hook display descriptor onto its StopHookInfo', async () => {
    const hm = new HookManager();
    // A command-style stop hook and a prompt-style stop hook, each with a display
    // descriptor identifying its config.
    hm.onStop(async () => ({}), { command: 'make verify' });
    hm.onStop(async () => { throw new Error('boom'); }, { promptText: 'Confirm the build is green' });

    let emitted: StopHookSummary | undefined;
    hm.on('stopHookSummary', (s: StopHookSummary) => { emitted = s; });

    await hm.executeStop(makeContext());

    assert.ok(emitted, 'stopHookSummary must fire');
    assert.equal(emitted!.hookInfos.length, 2);
    assert.equal(emitted!.hookInfos[0].command, 'make verify');
    assert.equal(emitted!.hookInfos[0].promptText, undefined);
    assert.equal(emitted!.hookInfos[1].promptText, 'Confirm the build is green');
    assert.equal(emitted!.hookInfos[1].command, undefined);
    assert.equal(emitted!.hookInfos[1].error, 'boom');
  });

  it('leaves command/promptText undefined for a plain Stop hook (no descriptor)', async () => {
    const hm = new HookManager();
    hm.onStop(async () => ({}));
    let emitted: StopHookSummary | undefined;
    hm.on('stopHookSummary', (s: StopHookSummary) => { emitted = s; });
    await hm.executeStop(makeContext());
    assert.ok(emitted);
    assert.equal(emitted!.hookInfos[0].command, undefined);
    assert.equal(emitted!.hookInfos[0].promptText, undefined);
  });

  it('a config-registered command Stop hook carries its command in the summary', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-w91-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(dir, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'stop', hook: { type: 'command', command: 'true' } }] }),
    );
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    let emitted: StopHookSummary | undefined;
    hm.on('stopHookSummary', (s: StopHookSummary) => { emitted = s; });
    await hm.executeStop(makeContext());

    assert.ok(emitted, 'a config stop hook must run and produce a summary');
    assert.equal(emitted!.count, 1);
    assert.equal(emitted!.hookInfos[0].command, 'true');

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('formatStopHookSummaryMessage shows the hook identifier on an errored/slow row', () => {
    const summary: StopHookSummary = {
      count: 2,
      hookInfos: [
        { durationMs: 3, command: 'make a' },
        { durationMs: 7, error: 'boom', command: 'make b' },
      ],
      totalDurationMs: 12,
      hookErrors: ['boom'],
      preventContinuation: false,
      hasOutput: true,
    };
    const msg = formatStopHookSummaryMessage(summary);
    // The errored hook's row names its command; the clean fast hook stays silent.
    assert.match(msg, /hook 2 \[make b\] \(7ms\): boom/);
    assert.doesNotMatch(msg, /make a/);
  });

  it('keeps the legacy per-hook row format when no identifier is present', () => {
    const summary: StopHookSummary = {
      count: 1,
      hookInfos: [{ durationMs: 7, error: 'boom' }],
      totalDurationMs: 7,
      hookErrors: ['boom'],
      preventContinuation: false,
      hasOutput: true,
    };
    assert.match(formatStopHookSummaryMessage(summary), /hook 1 \(7ms\): boom/);
  });
});

describe('U007/w91 — hook-output schema hint advertises every accepted field', () => {
  it('HOOK_JSON_SCHEMA_HINT lists async / asyncTimeout / interrupt / updatedPermissions', () => {
    const hint = JSON.parse(HOOK_JSON_SCHEMA_HINT) as Record<string, string>;
    for (const k of ['async', 'asyncTimeout', 'interrupt', 'updatedPermissions']) {
      assert.ok(k in hint, `schema hint must advertise "${k}"`);
      assert.equal(typeof hint[k], 'string');
    }
    // async OUTPUT response contract wording (types/hooks.ts async:true + asyncTimeout).
    assert.match(hint.async, /boolean/);
    assert.match(hint.asyncTimeout, /number/);
  });

  it('every field the validator type-checks is documented in the hint (no drift)', () => {
    const hint = JSON.parse(HOOK_JSON_SCHEMA_HINT) as Record<string, string>;
    // These are the output fields validateHookDecisionOutput accepts/type-checks.
    const accepted = [
      'reason', 'stopReason', 'systemMessage', 'additionalContext',
      'continue', 'suppressOutput', 'async', 'interrupt',
      'updatedPermissions', 'asyncTimeout', 'updatedInput',
    ];
    for (const k of accepted) assert.ok(k in hint, `hint missing accepted field "${k}"`);
    // And the hint's documented shapes remain valid (no accepted field is rejected).
    assert.equal(validateHookDecisionOutput({ async: true, asyncTimeout: 15000, interrupt: true }), undefined);
  });
});
