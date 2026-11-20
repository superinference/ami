/**
 * U007 (w111) — prompt hook conversation-history prepend.
 *
 * Parity: the reference's execPromptHook accepts an optional `messages[]` and, when
 * present, prepends the full conversation history before the hook's user message
 * (`messagesToQuery = [...messages, userMessage]`) so the LLM can judge the hook
 * condition against the ACTUAL conversation rather than the self-contained event
 * payload alone (execPromptHook.ts:L28,L44-48).
 *
 * In AMI, {@link HookManager.setConversationHistoryProvider} wires a thunk over the
 * live conversation; {@link runEngineHook} seeds the PROMPT hook sub-engine with it
 * via `initialMessages` (the engine appends the hook's composed user message on
 * submit). AGENT hooks are deliberately excluded — they read the transcript FILE via
 * a scoped Read grant instead, so they must NOT also receive the history inline.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type PreToolUseContext } from '../src/hooks';
import type { Message } from '../src/types';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

function tmpCwdWith(hook: Record<string, unknown>): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-prompthist-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({
    hooks: [{ event: 'preToolUse', hook }],
  }));
  return dir;
}

/** Engine stub that captures the factory config and returns an {ok:true} verdict. */
function captureEngine(sink: (cfg: any) => void) {
  return (cfg: any) => {
    sink(cfg);
    return { async *submit(_p: string) { yield { type: 'text_delta', text: '{"ok":true}' }; }, shutdown() {} };
  };
}

const history: Message[] = [
  { role: 'user', content: 'can I delete the temp cache?' },
  { role: 'assistant', content: 'yes, go ahead — you approved it.' },
];

describe('prompt hook conversation-history prepend (execPromptHook messages[] parity)', () => {
  it('seeds the prompt hook sub-engine with the provided conversation history as initialMessages', async () => {
    const dir = tmpCwdWith({ type: 'prompt', prompt: 'block only if the user did not already agree', timeout: 5 });
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setConversationHistoryProvider(() => history);
    let cfg: any = null;
    hm.setEngineFactory(captureEngine((c) => { cfg = c; }));
    await hm.executePreToolUse(preCtx);
    assert.deepEqual(cfg.initialMessages, history, 'the sub-engine is seeded with the full conversation history');
    // A COPY is passed, not the caller's live array (mutating later must not leak).
    assert.notEqual(cfg.initialMessages, history, 'history array is copied, not shared by reference');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('does NOT seed initialMessages when no conversation-history provider is wired (unchanged behavior)', async () => {
    // RED-GREEN discriminator: without the feature there is no provider and no seed;
    // this locks that the prepend is OPT-IN and never fabricates history.
    const dir = tmpCwdWith({ type: 'prompt', prompt: 'verify', timeout: 5 });
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    let cfg: any = null;
    hm.setEngineFactory(captureEngine((c) => { cfg = c; }));
    await hm.executePreToolUse(preCtx);
    assert.equal(cfg.initialMessages, undefined, 'no provider → no seeded messages');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('does NOT seed initialMessages when the provider returns an empty history', async () => {
    const dir = tmpCwdWith({ type: 'prompt', prompt: 'verify', timeout: 5 });
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setConversationHistoryProvider(() => []);
    let cfg: any = null;
    hm.setEngineFactory(captureEngine((c) => { cfg = c; }));
    await hm.executePreToolUse(preCtx);
    assert.equal(cfg.initialMessages, undefined, 'empty history → no seeded messages');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('does NOT seed AGENT hooks with inline history (they read the transcript file instead)', async () => {
    // Parity: only execPromptHook receives messages[]; execAgentHook inspects the
    // transcript FILE via a scoped Read grant, so an agent hook must never ALSO get
    // the history inlined (that would double the context).
    const dir = tmpCwdWith({ type: 'agent', prompt: 'verify', timeout: 5 });
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setConversationHistoryProvider(() => history);
    let cfg: any = null;
    hm.setEngineFactory(captureEngine((c) => { cfg = c; }));
    await hm.executePreToolUse(preCtx);
    assert.equal(cfg.initialMessages, undefined, 'agent hooks are not seeded with inline history');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('reflects the live conversation at fire time (thunk is re-read on each run)', async () => {
    const dir = tmpCwdWith({ type: 'prompt', prompt: 'verify', timeout: 5 });
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const live: Message[] = [{ role: 'user', content: 'turn 1' }];
    hm.setConversationHistoryProvider(() => live);
    let cfg: any = null;
    hm.setEngineFactory(captureEngine((c) => { cfg = c; }));

    await hm.executePreToolUse(preCtx);
    assert.equal(cfg.initialMessages.length, 1, 'first run sees one turn');

    // Simulate the conversation growing before the hook fires again.
    live.push({ role: 'assistant', content: 'reply 1' }, { role: 'user', content: 'turn 2' });
    await hm.executePreToolUse(preCtx);
    assert.equal(cfg.initialMessages.length, 3, 'second run sees the grown conversation');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('clears the provider when passed undefined', async () => {
    const dir = tmpCwdWith({ type: 'prompt', prompt: 'verify', timeout: 5 });
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setConversationHistoryProvider(() => history);
    hm.setConversationHistoryProvider(undefined);
    let cfg: any = null;
    hm.setEngineFactory(captureEngine((c) => { cfg = c; }));
    await hm.executePreToolUse(preCtx);
    assert.equal(cfg.initialMessages, undefined, 'cleared provider → no seeded messages');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
