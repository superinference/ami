/**
 * U021 — Config knob CLAUDE_CODE_SAVE_HOOK_ADDITIONAL_CONTEXT parity
 * (AMI_SAVE_HOOK_ADDITIONAL_CONTEXT). Covers:
 *  - isSaveHookAdditionalContextEnabled: dual-env gate (AMI value first, Claude alias),
 *    off by default, respects truthy/falsy;
 *  - isHookAdditionalContextMessage: marker discriminator;
 *  - isLoggableMessage: canonical save-persistence predicate (drops isMeta; drops
 *    hook-additional-context unless the knob is on; keeps everything else);
 *  - SessionManager.save/load round-trip: a hook-additional-context message is FILTERED
 *    OUT of the saved transcript by default and PERSISTED (survives resume) when the
 *    knob is enabled. Reverting the save() filter makes the "off" case fail (RED).
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  isSaveHookAdditionalContextEnabled,
  isHookAdditionalContextMessage,
  isLoggableMessage,
  SAVE_HOOK_ADDITIONAL_CONTEXT_ENV,
  type Session,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u021-hookctx-'));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function session(id: string, messages: Message[]): Session {
  return {
    id: asSessionId(id),
    messages,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'u' },
  } as Session;
}
const hookCtx = (content: string): Message => ({ role: 'user', content, hookAdditionalContext: true } as Message);
const user = (content: string): Message => ({ role: 'user', content } as Message);

describe('isSaveHookAdditionalContextEnabled', () => {
  it('is off by default and honors the dual-env gate (AMI value first, Claude alias)', () => {
    assert.equal(isSaveHookAdditionalContextEnabled({}), false);
    assert.equal(SAVE_HOOK_ADDITIONAL_CONTEXT_ENV, 'AMI_SAVE_HOOK_ADDITIONAL_CONTEXT');
    assert.equal(isSaveHookAdditionalContextEnabled({ [SAVE_HOOK_ADDITIONAL_CONTEXT_ENV]: '1' }), true);
    assert.equal(isSaveHookAdditionalContextEnabled({ CLAUDE_CODE_SAVE_HOOK_ADDITIONAL_CONTEXT: 'true' }), true);
    assert.equal(isSaveHookAdditionalContextEnabled({ [SAVE_HOOK_ADDITIONAL_CONTEXT_ENV]: '0' }), false);
    assert.equal(isSaveHookAdditionalContextEnabled({ [SAVE_HOOK_ADDITIONAL_CONTEXT_ENV]: 'false' }), false);
  });
});

describe('isHookAdditionalContextMessage', () => {
  it('matches only a user message carrying the hookAdditionalContext marker', () => {
    assert.equal(isHookAdditionalContextMessage(hookCtx('x')), true);
    assert.equal(isHookAdditionalContextMessage(user('x')), false);
    assert.equal(isHookAdditionalContextMessage({ role: 'system', content: 'x' } as Message), false);
    assert.equal(isHookAdditionalContextMessage({ role: 'assistant', content: 'x' } as Message), false);
  });
});

describe('isLoggableMessage', () => {
  it('drops isMeta nudges regardless of the knob', () => {
    assert.equal(isLoggableMessage({ role: 'user', content: 'x', isMeta: true } as Message, {}), false);
    assert.equal(isLoggableMessage({ role: 'system', content: 'x', isMeta: true } as Message, { [SAVE_HOOK_ADDITIONAL_CONTEXT_ENV]: '1' }), false);
  });
  it('gates hook-additional-context on the knob', () => {
    assert.equal(isLoggableMessage(hookCtx('x'), {}), false);
    assert.equal(isLoggableMessage(hookCtx('x'), { [SAVE_HOOK_ADDITIONAL_CONTEXT_ENV]: '1' }), true);
    assert.equal(isLoggableMessage(hookCtx('x'), { CLAUDE_CODE_SAVE_HOOK_ADDITIONAL_CONTEXT: '1' }), true);
  });
  it('keeps ordinary messages', () => {
    assert.equal(isLoggableMessage(user('x'), {}), true);
    assert.equal(isLoggableMessage({ role: 'assistant', content: 'y' } as Message, {}), true);
    assert.equal(isLoggableMessage({ role: 'tool', tool_call_id: 't', content: 'z' } as Message, {}), true);
  });
});

describe('SessionManager.save persistence of hook additional context', () => {
  const ENV_KEY = SAVE_HOOK_ADDITIONAL_CONTEXT_ENV;
  function withEnv<T>(value: string | undefined, fn: () => T): T {
    const prev = process.env[ENV_KEY];
    if (value === undefined) delete process.env[ENV_KEY]; else process.env[ENV_KEY] = value;
    try { return fn(); } finally {
      if (prev === undefined) delete process.env[ENV_KEY]; else process.env[ENV_KEY] = prev;
    }
  }

  it('filters the hook-additional-context message out of the saved transcript by default', () => {
    withEnv(undefined, () => {
      const dir = mkdir();
      const sm = new SessionManager('/tmp/proj', dir);
      sm.save(session('s1', [user('hello'), hookCtx('project rules: be concise'), user('do it')]));
      const loaded = sm.load('s1');
      assert.ok(loaded);
      assert.equal(loaded!.messages.length, 2, 'hook-context message must be dropped on save');
      assert.equal(loaded!.messages.some(m => (m as { hookAdditionalContext?: boolean }).hookAdditionalContext), false);
      assert.deepEqual(loaded!.messages.map(m => m.content), ['hello', 'do it']);
    });
  });

  it('persists the hook-additional-context message when the knob is enabled (survives resume)', () => {
    withEnv('1', () => {
      const dir = mkdir();
      const sm = new SessionManager('/tmp/proj', dir);
      sm.save(session('s2', [user('hello'), hookCtx('project rules: be concise'), user('do it')]));
      const loaded = sm.load('s2');
      assert.ok(loaded);
      assert.equal(loaded!.messages.length, 3, 'hook-context message must be persisted when opted in');
      const kept = loaded!.messages.find(m => (m as { hookAdditionalContext?: boolean }).hookAdditionalContext);
      assert.ok(kept, 'the marked hook-context message survives a save/load round-trip');
      assert.equal(kept!.content, 'project rules: be concise');
    });
  });
});
