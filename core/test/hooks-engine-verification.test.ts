/**
 * Parity tests for AI-driven (prompt/agent) hook verification contract + the
 * session permission context for hook sub-engines (Claude->AMI parity):
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
