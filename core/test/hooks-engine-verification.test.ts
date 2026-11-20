/**
 * Parity tests for AI-driven (prompt/agent) hook verification contract + the
 * session permission context for hook sub-engines (the reference->AMI parity):
 *  - prompt/agent hooks run under the configured permission mode (not blanket
 *    auto-allow) and are told to return {ok, reason};
 *  - an {ok:false} verification result blocks (DENY with reason);
 *  - an {ok:true} result allows; free-form output falls back to raw handling.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  parseHookVerification,
  HOOK_VERIFICATION_CONTRACT,
  type PreToolUseContext,
} from '../src/hooks';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

function tmpCwdWithPromptHook(prompt = 'verify this'): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-verif-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({
    hooks: [{ event: 'preToolUse', hook: { type: 'prompt', prompt, timeout: 5 } }],
  }));
  return dir;
}

function mkEngine(text: string, capture?: (cfg: any) => void) {
  return (cfg: any) => {
    capture?.(cfg);
    return { async *submit(_p: string) { yield { type: 'text_delta', text }; }, shutdown() {} };
  };
}

/**
 * Engine stub that emits a `verification_output` tool call carrying `{ok, reason}`
 * plus a separate (possibly non-conforming) text reply, so a test can prove the
 * structured tool verdict is what drives the decision.
 */
function mkVerdictToolEngine(verdict: Record<string, unknown>, text = '', capture?: (cfg: any) => void) {
  return (cfg: any) => {
    capture?.(cfg);
    return {
      async *submit(_p: string) {
        yield { type: 'tool_use_start', toolName: 'verification_output', toolCallId: 't1', input: verdict };
        if (text) yield { type: 'text_delta', text };
      },
      shutdown() {},
    };
  };
}

describe('parseHookVerification', () => {
  it('parses {ok:false, reason}', () => {
    assert.deepEqual(parseHookVerification('{"ok":false,"reason":"nope"}'), { ok: false, reason: 'nope' });
  });
  it('parses {ok:true}', () => {
    assert.deepEqual(parseHookVerification('{"ok":true}'), { ok: true, reason: undefined });
  });
  it('returns undefined for non-verification output', () => {
    assert.equal(parseHookVerification('just text'), undefined);
    assert.equal(parseHookVerification('{"reason":"x"}'), undefined);
  });
});

describe('AI-driven hook verification contract', () => {
  it('blocks a tool when the prompt hook returns {ok:false}', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory(mkEngine('{"ok":false,"reason":"unsafe command"}'));
    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(decision.action, 'deny');
    assert.match(decision.action === 'deny' ? decision.reason : '', /unsafe command/);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('allows a tool when the prompt hook returns {ok:true}', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory(mkEngine('{"ok":true}'));
    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(decision.action, 'allow');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('prepends the verification contract and uses the configured permission mode', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setHookEnginePermissionMode('ask');
    let cfg: any = null;
    let prompt = '';
    hm.setEngineFactory((c: any) => {
      cfg = c;
      return { async *submit(p: string) { prompt = p; yield { type: 'text_delta', text: '{"ok":true}' }; }, shutdown() {} };
    });
    await hm.executePreToolUse(preCtx);
    assert.equal(cfg.permissionMode, 'ask', 'hook sub-engine uses the session permission mode');
    assert.ok(prompt.startsWith(HOOK_VERIFICATION_CONTRACT), 'prompt is prefixed with the {ok,reason} contract');
    assert.equal(hm.getHookEnginePermissionMode(), 'ask');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('falls back to raw handling for free-form output (SYSTEM: context)', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory(mkEngine('SYSTEM:extra context'));
    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(decision.action, 'allow');
    assert.equal(decision.additionalContext, 'extra context');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe('reusable verification-result tool (createStructuredOutputTool parity)', () => {
  it('advertises the verification_output tool to the hook sub-engine', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    let cfg: any = null;
    hm.setEngineFactory(mkEngine('{"ok":true}', (c) => { cfg = c; }));
    await hm.executePreToolUse(preCtx);
    const toolNames = (cfg.tools ?? []).map((t: any) => t.name);
    assert.ok(toolNames.includes('verification_output'), 'sub-engine is given the verification tool');
    const vtool = (cfg.tools ?? []).find((t: any) => t.name === 'verification_output');
    assert.match(vtool.description, /exactly once at the end of your response/);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // RED-GREEN: with the tool-verdict capture the {ok:false} TOOL CALL blocks even
  // though the text reply is NOT a parseable {ok,reason} verdict. Reverting the
  // capture (toolVerdict) makes it fall back to parseHookVerification(''), yield no
  // verdict, and ALLOW — so this discriminates the fix from the old behavior.
  it('blocks when the verification TOOL returns {ok:false} with a non-JSON text reply', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory(mkVerdictToolEngine({ ok: false, reason: 'tool said unsafe' }, 'all done'));
    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(decision.action, 'deny');
    assert.match(decision.action === 'deny' ? decision.reason : '', /tool said unsafe/);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('allows when the verification TOOL returns {ok:true} with a non-JSON text reply', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory(mkVerdictToolEngine({ ok: true }, 'looks good'));
    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(decision.action, 'allow');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // The structured tool verdict is authoritative over a CONFLICTING text reply.
  it('prefers the TOOL verdict over a conflicting free-text {ok,reason}', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    // Tool says BLOCK, text says OK — the tool wins.
    hm.setEngineFactory(mkVerdictToolEngine({ ok: false, reason: 'tool overrides' }, '{"ok":true}'));
    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(decision.action, 'deny');
    assert.match(decision.action === 'deny' ? decision.reason : '', /tool overrides/);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  // A malformed tool input (ok not boolean) is ignored → the text {ok,reason}
  // contract still applies (graceful superset, never a crash).
  it('ignores a malformed tool verdict and falls back to the text contract', async () => {
    const dir = tmpCwdWithPromptHook();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory(mkVerdictToolEngine({ ok: 'nope' }, '{"ok":false,"reason":"text wins"}'));
    const decision = await hm.executePreToolUse(preCtx);
    assert.equal(decision.action, 'deny');
    assert.match(decision.action === 'deny' ? decision.reason : '', /text wins/);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
