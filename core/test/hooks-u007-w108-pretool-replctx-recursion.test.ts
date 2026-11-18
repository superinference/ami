/**
 * U007 / w108 — three hook-payload / lifecycle parity gaps.
 *
 * Gap A (PreToolUse tool-use summary + requestPrompt): Claude Code passes
 *   `tool.getToolUseSummary(input)` and `toolUseContext.requestPrompt` into the
 *   PreToolUse hook. AMI's preToolUse payload was only {event,tool_name,tool_input}.
 *   Now it carries `tool_use_summary` (a generic one-line summary) and, when known,
 *   `request_prompt`. RED: those keys are absent without the serialize change.
 *
 * Gap B (REPL hook context fields): Claude Code's REPLHookContext exposes
 *   systemPrompt + user/system context bags to post-sampling + stop hooks. AMI now
 *   serializes `system_prompt`/`user_context`/`system_context` when the engine
 *   populated them. RED: those keys are absent without the serialize change.
 *
 * Gap C (prompt/agent-hook recursion guard): a prompt/agent hook drives a sub-engine;
 *   if that sub-engine re-fires UserPromptSubmit hooks it recurses infinitely. AMI
 *   now short-circuits executeUserPromptSubmit while an engine-backed hook runs
 *   (Claude Code's execPromptHook bypasses processUserInput for the same reason).
 *   RED: without the guard the nested executeUserPromptSubmit runs its hooks.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, buildToolUseSummary, type HookContext, type PreToolUseContext } from '../src/hooks';

const tmpDirs: string[] = [];
function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-w108-'));
  tmpDirs.push(dir);
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}
function makeManager(dir: string): HookManager {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  return hm;
}
after(() => { for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

describe('U007/w108 Gap A — buildToolUseSummary (generic tool-use summary)', () => {
  it('summarizes with primary-argument precedence (command wins)', () => {
    assert.equal(buildToolUseSummary('bash', { command: 'ls -la', file_path: '/x' }), 'bash(ls -la)');
    assert.equal(buildToolUseSummary('file_read', { file_path: '/etc/hosts' }), 'file_read(/etc/hosts)');
    assert.equal(buildToolUseSummary('web_fetch', { url: 'https://example.com' }), 'web_fetch(https://example.com)');
  });
  it('collapses internal whitespace onto one line', () => {
    assert.equal(buildToolUseSummary('bash', { command: 'echo a\n  echo   b' }), 'bash(echo a echo b)');
  });
  it('elides an over-long argument to the cap with an ellipsis', () => {
    const long = 'x'.repeat(500);
    const s = buildToolUseSummary('bash', { command: long });
    assert.ok(s.length < 140, 'summary is bounded');
    assert.ok(s.endsWith('...)'), 'elided with an ellipsis');
  });
  it('falls back to the bare tool name when no recognizable primary argument', () => {
    assert.equal(buildToolUseSummary('todo_write', { todos: [] }), 'todo_write');
    assert.equal(buildToolUseSummary('noop', { command: '   ' }), 'noop');
  });
});

describe('U007/w108 Gap A — preToolUse payload carries tool_use_summary + request_prompt', () => {
  it('serializes tool_use_summary and request_prompt into the hook stdin', async () => {
    const payload = 'pre.json';
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'command', command: `cat > ${payload}` } }]);
    const hm = makeManager(dir);
    const ctx: PreToolUseContext = {
      messages: [], turnCount: 1, toolName: 'bash',
      toolInput: { command: 'rm -rf /tmp/x' }, requestPrompt: 'clean the temp dir',
    };
    await hm.executePreToolUse(ctx);
    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.equal(parsed.hook_event_name, 'PreToolUse');
    // RED without the fix: these keys were never serialized.
    assert.equal(parsed.tool_use_summary, 'bash(rm -rf /tmp/x)');
    assert.equal(parsed.request_prompt, 'clean the temp dir');
  });
  it('omits request_prompt when the caller did not provide it, still emits a summary', async () => {
    const payload = 'pre2.json';
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'command', command: `cat > ${payload}` } }]);
    const hm = makeManager(dir);
    await hm.executePreToolUse({ messages: [], turnCount: 0, toolName: 'grep', toolInput: { pattern: 'TODO' } });
    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.equal(parsed.tool_use_summary, 'grep(TODO)');
    assert.equal('request_prompt' in parsed, false);
  });
});

describe('U007/w108 Gap B — stop + postSampling payloads carry REPL context fields', () => {
  it('serializes system_prompt / user_context / system_context for a Stop hook', async () => {
    const payload = 'stop.json';
    const dir = tmpCwdWithHooks([{ event: 'stop', hook: { type: 'command', command: `cat > ${payload}` } }]);
    const hm = makeManager(dir);
    const ctx: HookContext = {
      messages: [], turnCount: 2,
      systemPrompt: 'You are a careful engineer.',
      userContext: { ticket: 'ABC-1' },
      systemContext: { session: 's1' },
    };
    await hm.executeStop(ctx);
    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    // RED without the fix: none of these keys are serialized.
    assert.equal(parsed.system_prompt, 'You are a careful engineer.');
    assert.deepEqual(parsed.user_context, { ticket: 'ABC-1' });
    assert.deepEqual(parsed.system_context, { session: 's1' });
  });
  it('serializes the context bags for a postSampling hook', async () => {
    const payload = 'ps.json';
    const dir = tmpCwdWithHooks([{ event: 'postSampling', hook: { type: 'command', command: `cat > ${payload}` } }]);
    const hm = makeManager(dir);
    await hm.executePostSampling({ messages: [], turnCount: 1, systemPrompt: 'be brief', userContext: { k: 'v' } });
    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.equal(parsed.system_prompt, 'be brief');
    assert.deepEqual(parsed.user_context, { k: 'v' });
    assert.equal('system_context' in parsed, false, 'an absent bag is omitted');
  });
  it('omits all REPL context keys when the engine did not populate them (empty bags too)', async () => {
    const payload = 'stop2.json';
    const dir = tmpCwdWithHooks([{ event: 'stop', hook: { type: 'command', command: `cat > ${payload}` } }]);
    const hm = makeManager(dir);
    await hm.executeStop({ messages: [], turnCount: 0, userContext: {}, systemContext: {} });
    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.equal('system_prompt' in parsed, false);
    assert.equal('user_context' in parsed, false);
    assert.equal('system_context' in parsed, false);
  });
});

describe('U007/w108 Gap C — engine-backed hook cannot re-fire UserPromptSubmit (recursion guard)', () => {
  it('short-circuits executeUserPromptSubmit while a prompt hook sub-engine is running', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'prompt', prompt: 'evaluate this call' } }]);
    const hm = makeManager(dir);
    let promptSubmitRuns = 0;
    // A real UserPromptSubmit hook that WOULD contribute context if it ran.
    hm.onUserPromptSubmit(async () => { promptSubmitRuns++; return { additionalContext: 'from-ups-hook' }; });

    let insideResult: unknown = 'unset';
    hm.setEngineFactory(() => ({
      // The sub-engine re-enters the prompt pipeline mid-run (the recursion vector).
      async *submit() {
        insideResult = await hm.executeUserPromptSubmit({ prompt: 'nested', turnCount: 0 });
        yield { type: 'done', exitReason: 'completed' } as never;
      },
      shutdown() { /* noop */ },
    }));

    const decision = await hm.executePreToolUse({ messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } });
    assert.equal(decision.action, 'allow', 'the prompt hook stays non-blocking');
    // RED without the guard: the nested call runs the hook (runs=1) and returns its result.
    assert.equal(promptSubmitRuns, 0, 'nested UserPromptSubmit hooks must NOT fire inside an engine-backed hook');
    assert.equal(insideResult, undefined, 'the guarded call returns void');

    // After the hook run completes the guard is lifted — normal dispatch resumes.
    const after = await hm.executeUserPromptSubmit({ prompt: 'real', turnCount: 1 });
    assert.equal(promptSubmitRuns, 1, 'UserPromptSubmit works normally once no engine hook is running');
    assert.deepEqual(after, {
      additionalContext: 'from-ups-hook', systemMessage: undefined,
      blockingError: undefined, preventContinuation: undefined, stopReason: undefined,
    });
  });
});
