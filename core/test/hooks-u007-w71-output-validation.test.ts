/**
 * Unit U007 (wave w71) — core/core/src/hooks.ts parity gaps:
 *
 *  - validateHookDecisionOutput: strict shape/type validation of a parsed hook
 *    JSON output before any field is acted on (Claude Code parseHookOutput /
 *    hookJSONOutputSchema safeParse). A KNOWN field with a wrong-typed value is a
 *    malformed-output validation error, not a silent mis-interpretation.
 *  - HookDecisionOutput.async / asyncTimeout: runtime async-declaration output knob
 *    (Claude Code AsyncHookJSONOutput), type-validated.
 *  - formatHookDenyReason: event-specific descriptive deny/feedback wording
 *    (phase-specific default instead of a bare 'Hook blocked').
 *  - translateHookMatcher: ExitPlanMode / ExitPlanModeV2 → plan_mode so a
 *    Claude-Code-format hook matcher gates AMI's plan-exit tool.
 *  - parseWatchPaths: FileChanged watchPaths must be ABSOLUTE (relative dropped).
 *  - isHookKillSwitchEnv: CLAUDE_CODE_SIMPLE (AMI_SIMPLE / AMI_DISABLE_HOOKS) env
 *    kill-switch disabling every hook at load time.
 *
 * Framework: node:test + assert/strict. Integration cases drive the real
 * HookManager (runHookByType / loadFromFile) end to end, not mocks.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  HookManager,
  validateHookDecisionOutput,
  formatHookDenyReason,
  translateHookMatcher,
  parseWatchPaths,
  isHookKillSwitchEnv,
  type PreToolUseContext,
} from '../src/hooks';

function writeTmpHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w71-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

function makePreToolUseCtx(overrides: Partial<PreToolUseContext> = {}): PreToolUseContext {
  return { messages: [], turnCount: 1, toolName: 'bash', toolInput: { command: 'ls' }, ...overrides };
}

// ---------------------------------------------------------------------------
// validateHookDecisionOutput — strict type/shape gating
// ---------------------------------------------------------------------------
describe('validateHookDecisionOutput', () => {
  it('accepts undefined / a well-typed decision object', () => {
    assert.equal(validateHookDecisionOutput(undefined), undefined);
    assert.equal(validateHookDecisionOutput({}), undefined);
    assert.equal(validateHookDecisionOutput({
      decision: 'deny', reason: 'nope', continue: false, stopReason: 'stop',
      systemMessage: 'msg', additionalContext: 'ctx', suppressOutput: true,
      updatedInput: { command: 'ls' }, async: true, asyncTimeout: 15000,
    }), undefined);
  });

  it('delegates decision/permissionDecision enum checks (no duplicate)', () => {
    assert.match(String(validateHookDecisionOutput({ decision: 'nuke' })), /Unknown hook decision type/);
    assert.match(String(validateHookDecisionOutput({ permissionDecision: 'maybe' })), /permissionDecision/);
  });

  it('rejects a wrong-typed string field', () => {
    assert.match(String(validateHookDecisionOutput({ reason: 5 })), /"reason" must be a string/);
    assert.match(String(validateHookDecisionOutput({ additionalContext: 5 })), /"additionalContext" must be a string/);
  });

  it('rejects a wrong-typed boolean field (continue:"yes" would be truthy)', () => {
    assert.match(String(validateHookDecisionOutput({ continue: 'yes' })), /"continue" must be a boolean/);
    assert.match(String(validateHookDecisionOutput({ suppressOutput: 1 })), /"suppressOutput" must be a boolean/);
    assert.match(String(validateHookDecisionOutput({ async: 'true' })), /"async" must be a boolean/);
  });

  it('rejects a non-number asyncTimeout and a non-finite one', () => {
    assert.match(String(validateHookDecisionOutput({ asyncTimeout: '15' })), /"asyncTimeout" must be a number/);
    assert.match(String(validateHookDecisionOutput({ asyncTimeout: Infinity })), /"asyncTimeout" must be a number/);
  });

  it('rejects an updatedInput that is not a plain object (array / null)', () => {
    assert.match(String(validateHookDecisionOutput({ updatedInput: [] })), /"updatedInput" must be an object/);
    assert.match(String(validateHookDecisionOutput({ updatedInput: null })), /"updatedInput" must be an object/);
  });

  it('does NOT reject unknown/extra properties (AMI strips, never errors)', () => {
    assert.equal(validateHookDecisionOutput({ decision: 'approve', rule: 'forged', nonsense: 1 }), undefined);
  });
});

// ---------------------------------------------------------------------------
// formatHookDenyReason — event-specific default wording
// ---------------------------------------------------------------------------
describe('formatHookDenyReason', () => {
  it('a hook-supplied reason always wins', () => {
    assert.equal(formatHookDenyReason('preToolUse', 'custom reason'), 'custom reason');
    assert.equal(formatHookDenyReason('PreToolUse', '  spaced  '), 'spaced');
  });

  it('phase-specific default when no reason is given', () => {
    assert.equal(formatHookDenyReason('PreToolUse'), 'PreToolUse hook blocked this tool call');
    assert.equal(formatHookDenyReason('postToolUse'), 'PostToolUse hook blocked the result');
    assert.equal(formatHookDenyReason('UserPromptSubmit'), 'UserPromptSubmit hook blocked this prompt');
    assert.equal(formatHookDenyReason('stop'), 'Stop hook prevented the turn from ending');
  });

  it('names an unknown event, and falls back generically when undefined', () => {
    assert.equal(formatHookDenyReason('SessionStart'), 'SessionStart hook blocked this action');
    assert.equal(formatHookDenyReason(undefined), 'Hook blocked this action');
    assert.equal(formatHookDenyReason('', ''), 'Hook blocked this action');
  });
});

// ---------------------------------------------------------------------------
// translateHookMatcher — ExitPlanMode → plan_mode
// ---------------------------------------------------------------------------
describe('translateHookMatcher — plan-exit tool compat', () => {
  it('maps ExitPlanMode and ExitPlanModeV2 to plan_mode', () => {
    assert.equal(translateHookMatcher('ExitPlanMode'), 'plan_mode');
    assert.equal(translateHookMatcher('ExitPlanModeV2'), 'plan_mode');
  });
  it('translates mixed tokens and passes unknown ones through', () => {
    assert.equal(translateHookMatcher('Edit|ExitPlanMode|customRegex'), 'file_edit|plan_mode|customRegex');
  });
});

// ---------------------------------------------------------------------------
// parseWatchPaths — absolute-only path scoping
// ---------------------------------------------------------------------------
describe('parseWatchPaths — absolute path scoping', () => {
  it('keeps absolute paths and drops relative / empty / non-string ones', () => {
    assert.deepEqual(
      parseWatchPaths('{"watchPaths":["/abs/one","rel/two","","/abs/three",3]}'),
      ['/abs/one', '/abs/three'],
    );
  });
  it('drops a lone relative path entirely', () => {
    assert.deepEqual(parseWatchPaths('{"watchPaths":["relative/only"]}'), []);
  });
});

// ---------------------------------------------------------------------------
// isHookKillSwitchEnv — CLAUDE_CODE_SIMPLE env kill-switch
// ---------------------------------------------------------------------------
describe('isHookKillSwitchEnv', () => {
  it('true for CLAUDE_CODE_SIMPLE / AMI_SIMPLE / AMI_DISABLE_HOOKS truthy', () => {
    assert.equal(isHookKillSwitchEnv({ CLAUDE_CODE_SIMPLE: '1' }), true);
    assert.equal(isHookKillSwitchEnv({ AMI_SIMPLE: 'true' }), true);
    assert.equal(isHookKillSwitchEnv({ AMI_DISABLE_HOOKS: 'yes' }), true);
  });
  it('false when unset / explicitly falsy', () => {
    assert.equal(isHookKillSwitchEnv({}), false);
    assert.equal(isHookKillSwitchEnv({ CLAUDE_CODE_SIMPLE: '0' }), false);
    assert.equal(isHookKillSwitchEnv({ CLAUDE_CODE_SIMPLE: '' }), false);
  });
});

// ---------------------------------------------------------------------------
// Integration — runHookByType command parse validation + deny wording
// ---------------------------------------------------------------------------
describe('runHookByType — strict validation + deny wording (real spawn)', () => {
  let hm: HookManager;
  beforeEach(() => { hm = new HookManager(); });

  it('surfaces a malformed field as a validation error + failed run', async () => {
    const outcome = { success: true };
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"continue":"yes"}'` },
      process.cwd(), '{"hook_event_name":"PreToolUse"}', undefined, outcome,
    );
    assert.match(out, /^SYSTEM:/);
    assert.match(out, /"continue" must be a boolean/);
    assert.equal(outcome.success, false);
  });

  it('a decision:deny with no reason gets the PreToolUse phase-specific wording', async () => {
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"decision":"deny"}'` },
      process.cwd(), '{"hook_event_name":"PreToolUse"}', undefined, { success: true },
    );
    assert.equal(out, 'DENY:PreToolUse hook blocked this tool call');
  });

  it('a continue:false with no stopReason gets the Stop phase-specific wording', async () => {
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"continue":false}'` },
      process.cwd(), '{"hook_event_name":"Stop"}', undefined, { success: true },
    );
    assert.equal(out, 'DENY:Stop hook prevented the turn from ending');
  });

  it('a well-formed async-declaration output passes validation (no error)', async () => {
    const out = await (hm as any).runHookByType(
      { type: 'command', command: `printf '{"async":true,"asyncTimeout":15000,"suppressOutput":true}'` },
      process.cwd(), '{"hook_event_name":"PreToolUse"}', undefined, { success: true },
    );
    assert.equal(out, '');
  });
});

// ---------------------------------------------------------------------------
// Integration — CLAUDE_CODE_SIMPLE env disables ALL hooks at load time
// ---------------------------------------------------------------------------
describe('HookManager — CLAUDE_CODE_SIMPLE env kill-switch', () => {
  let saved: string | undefined;
  beforeEach(() => { saved = process.env.CLAUDE_CODE_SIMPLE; });
  afterEach(() => {
    if (saved === undefined) delete process.env.CLAUDE_CODE_SIMPLE;
    else process.env.CLAUDE_CODE_SIMPLE = saved;
  });

  it('skips loading every hook so a deny hook never fires', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf '{"decision":"deny","reason":"real"}'` } },
    ]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    process.env.CLAUDE_CODE_SIMPLE = '1';
    assert.equal(hm.isAllHooksDisabled(), true);
    hm.loadFromFile(tmpDir);
    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('WITHOUT the env switch the same deny hook DOES fire (control)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `printf '{"decision":"deny","reason":"real"}'` } },
    ]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    delete process.env.CLAUDE_CODE_SIMPLE;
    assert.equal(hm.isAllHooksDisabled(), false);
    hm.loadFromFile(tmpDir);
    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    assert.equal(decision.reason, 'real');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
