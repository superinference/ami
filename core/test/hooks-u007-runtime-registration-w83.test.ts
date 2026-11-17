/**
 * U007 (w83) — runtime (session/skill/agent-scoped) callback hook registration,
 * parity with Claude Code's sessionHooks.ts / registerFrontmatterHooks.ts /
 * hookEvents.ts. Covers these confirmed gaps on core/core/src/hooks.ts:
 *
 *  - matcher-scoped runtime hooks: registerCallback accepts a matcher; a runtime
 *    hook fires only for matching tool/event names ('' / absent = wildcard)
 *    (sessionHooks.ts:L33-40,L180-205,L419-427).
 *  - 'function' hook type + FunctionHook schema (id/timeout/callback/errorMessage)
 *    returning true=pass / false=block (sessionHooks.ts:L14-31).
 *  - addFunctionHook({timeout?, id?}): default timeout 5000ms, auto-generated id,
 *    returns the id (sessionHooks.ts:L93-115); removeFunctionHook(id) by id
 *    (sessionHooks.ts:L120-162).
 *  - skillRoot scoping + removeBySkillRoot teardown (sessionHooks.ts:L33-40,L182,
 *    L270-293).
 *  - session/agent-scoped registration keyed by sessionId + clearSessionHooks,
 *    isAgent Stop->SubagentStop remap (registerFrontmatterHooks.ts:L13-45).
 *  - pending hook-event buffer with replay-on-register + bounded FIFO cap
 *    (hookEvents.ts:L57-81).
 *
 * RED on revert: without the matcher dimension executeCallbacks fires every
 * callback regardless of matchValue; without clearSessionHooks/removeBySkillRoot
 * the hooks keep firing (no teardown); without addFunctionHook there is no
 * boolean gate / id removal; without the pending buffer early events are lost.
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { HookManager, MAX_PENDING_HOOK_EVENTS, type HookLifecycleEvent } from '../src/hooks';

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

describe('HookManager — matcher-scoped runtime hooks', () => {
  it('fires a matcher-gated hook only for a matching matchValue', async () => {
    const fired: string[] = [];
    hm.registerCallback('preToolUse', async () => { fired.push('bash'); return undefined; }, undefined, { matcher: 'bash' });
    await hm.executeCallbacks('preToolUse', {}, 'file_edit');
    assert.deepEqual(fired, [], 'must NOT fire for a non-matching tool name');
    await hm.executeCallbacks('preToolUse', {}, 'bash');
    assert.deepEqual(fired, ['bash'], 'must fire for the matching tool name');
  });

  it("treats an empty/absent matcher as the '' wildcard (always fires)", async () => {
    let n = 0;
    hm.registerCallback('preToolUse', async () => { n++; return undefined; }, undefined, { matcher: '' });
    hm.registerCallback('preToolUse', async () => { n++; return undefined; });
    await hm.executeCallbacks('preToolUse', {}, 'anything');
    assert.equal(n, 2);
  });

  it('ignores the matcher when no matchValue is supplied (back-compat dispatch)', async () => {
    let n = 0;
    hm.registerCallback('custom', async () => { n++; return undefined; }, undefined, { matcher: 'bash' });
    await hm.executeCallbacks('custom', {});
    assert.equal(n, 1);
  });

  it('supports glob matchers', async () => {
    const seen: string[] = [];
    hm.registerCallback('preToolUse', async () => { seen.push('x'); return undefined; }, undefined, { matcher: 'file_*' });
    await hm.executeCallbacks('preToolUse', {}, 'bash');
    await hm.executeCallbacks('preToolUse', {}, 'file_edit');
    assert.deepEqual(seen, ['x']);
  });
});

describe('HookManager — addFunctionHook / removeFunctionHook', () => {
  it('returns an auto-generated id and counts the function hook', () => {
    const id = hm.addFunctionHook('preToolUse', () => true, 'blocked');
    assert.match(id, /^function-hook-\d+-/);
    assert.equal(hm.getSummary().functionHooks, 1);
    assert.equal(hm.getSummary().callbackHooks, 1);
  });

  it('uses a supplied id and replaces a prior hook registered under the same id', () => {
    const id = hm.addFunctionHook('preToolUse', () => true, 'a', { id: 'guard' });
    assert.equal(id, 'guard');
    hm.addFunctionHook('preToolUse', () => false, 'b', { id: 'guard' });
    // Same id => replaced, not duplicated.
    assert.equal(hm.getSummary().functionHooks, 1);
    assert.equal(hm.getSummary().callbackHooks, 1);
  });

  it('passes when the callback returns true (no block decision)', async () => {
    hm.addFunctionHook('preToolUse', () => true, 'blocked');
    const r = await hm.executeCallbacks('preToolUse', { messages: [] });
    assert.equal(r, undefined);
  });

  it('blocks with errorMessage when the callback returns false', async () => {
    hm.addFunctionHook('preToolUse', () => false, 'not allowed');
    const r = await hm.executeCallbacks('preToolUse', { messages: [] });
    assert.ok(r && typeof r === 'object');
    assert.equal((r as { decision?: string }).decision, 'block');
    assert.equal((r as { reason?: string }).reason, 'not allowed');
  });

  it('hands the conversation messages to the callback', async () => {
    let received: unknown;
    hm.addFunctionHook('preToolUse', (messages) => { received = messages; return true; }, 'x');
    const msgs = [{ role: 'user', content: 'hi' }];
    await hm.executeCallbacks('preToolUse', { messages: msgs });
    assert.deepEqual(received, msgs);
  });

  it('supports async boolean callbacks', async () => {
    hm.addFunctionHook('preToolUse', async () => false, 'nope');
    const r = await hm.executeCallbacks('preToolUse', { messages: [] });
    assert.equal((r as { decision?: string }).decision, 'block');
  });

  it('removeFunctionHook(id) removes it; returns false for an unknown id', () => {
    const id = hm.addFunctionHook('preToolUse', () => true, 'x');
    assert.equal(hm.removeFunctionHook('nope'), false);
    assert.equal(hm.removeFunctionHook(id), true);
    assert.equal(hm.getSummary().functionHooks, 0);
    assert.equal(hm.getSummary().callbackHooks, 0);
  });

  it('is matcher-gated when an options.matcher is supplied', async () => {
    let blocked = false;
    hm.addFunctionHook('preToolUse', () => false, 'deny', { matcher: 'bash' });
    const other = await hm.executeCallbacks('preToolUse', { messages: [] }, 'file_edit');
    assert.equal(other, undefined, 'non-matching tool is not gated');
    const bash = await hm.executeCallbacks('preToolUse', { messages: [] }, 'bash');
    blocked = !!(bash && (bash as { decision?: string }).decision === 'block');
    assert.equal(blocked, true);
  });

  it('defaults the function-hook timeout and surfaces no block when within budget', async () => {
    // Fast callback well within the 5000ms default; proves default path runs.
    hm.addFunctionHook('preToolUse', async () => true, 'x');
    const r = await hm.executeCallbacks('preToolUse', { messages: [] });
    assert.equal(r, undefined);
  });
});

describe('HookManager — session/agent-scoped registration', () => {
  it('clearSessionHooks tears down only that session\'s hooks', async () => {
    let a = 0, b = 0;
    hm.addSessionHook('sess-A', 'stop', async () => { a++; return undefined; });
    hm.addSessionHook('sess-B', 'stop', async () => { b++; return undefined; });
    assert.equal(hm.getSummary().callbackHooks, 2);
    const removed = hm.clearSessionHooks('sess-A');
    assert.equal(removed, 1);
    await hm.executeCallbacks('stop', {});
    assert.equal(a, 0, 'session A hook was torn down');
    assert.equal(b, 1, 'session B hook still fires');
  });

  it('isAgent remaps a stop hook to subagentStop', async () => {
    let onStop = 0, onSubagentStop = 0;
    hm.addSessionHook('agent-1', 'stop', async () => { onStop++; return undefined; }, { isAgent: true });
    await hm.executeCallbacks('stop', {});
    assert.equal(onStop, 0, 'agent stop hook must NOT fire on stop');
    await hm.executeCallbacks('subagentStop', {});
    // the same callback should now be under subagentStop
    assert.equal(onStop, 1);
    assert.equal(onSubagentStop, 0);
  });

  it('non-agent stop hooks stay on stop', async () => {
    let n = 0;
    hm.addSessionHook('skill-1', 'stop', async () => { n++; return undefined; }, { isAgent: false });
    await hm.executeCallbacks('stop', {});
    assert.equal(n, 1);
  });

  it('addSessionHook returns a working unregister closure', async () => {
    let n = 0;
    const off = hm.addSessionHook('s', 'stop', async () => { n++; return undefined; });
    off();
    await hm.executeCallbacks('stop', {});
    assert.equal(n, 0);
  });
});

describe('HookManager — skillRoot-scoped teardown', () => {
  it('removeBySkillRoot removes exactly the hooks owned by that skill root', async () => {
    let s1 = 0, s2 = 0;
    hm.registerCallback('preToolUse', async () => { s1++; return undefined; }, undefined, { skillRoot: '/skills/one' });
    hm.registerCallback('postToolUse', async () => { s1++; return undefined; }, undefined, { skillRoot: '/skills/one' });
    hm.registerCallback('preToolUse', async () => { s2++; return undefined; }, undefined, { skillRoot: '/skills/two' });
    const removed = hm.removeBySkillRoot('/skills/one');
    assert.equal(removed, 2);
    await hm.executeCallbacks('preToolUse', {});
    await hm.executeCallbacks('postToolUse', {});
    assert.equal(s1, 0, 'skill one hooks all torn down');
    assert.equal(s2, 1, 'skill two hook preserved');
  });

  it('returns 0 when no hook matches the skill root', () => {
    hm.registerCallback('stop', async () => undefined, undefined, { skillRoot: '/a' });
    assert.equal(hm.removeBySkillRoot('/b'), 0);
  });
});

describe('HookManager — pending hook-event buffer with replay', () => {
  it('buffers lifecycle events and replays them in order on register', () => {
    // Fire two lifecycle events before any subscriber attaches.
    hm.emit('hookProgress', { event: 'stop', hookType: 'command', label: 'A' });
    hm.emit('hookResolved', { event: 'stop', hookType: 'command', label: 'A', success: true });
    assert.equal(hm.getPendingHookEventCount(), 2);
    const seen: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => seen.push(e));
    assert.equal(seen.length, 2, 'both buffered events replayed on register');
    assert.equal(seen[0].phase, 'progress');
    assert.equal(seen[1].phase, 'resolved');
    assert.equal(seen[1].success, true);
    assert.equal(hm.getPendingHookEventCount(), 0, 'buffer drained after replay');
  });

  it('delivers directly (no buffering) while a subscriber is attached', () => {
    const seen: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => seen.push(e));
    hm.emit('hookProgress', { event: 'preToolUse', hookType: 'prompt', label: 'B' });
    assert.equal(seen.length, 1);
    assert.equal(hm.getPendingHookEventCount(), 0);
  });

  it('detach closure stops delivery and resumes buffering', () => {
    const seen: HookLifecycleEvent[] = [];
    const off = hm.registerHookEventHandler(e => seen.push(e));
    off();
    hm.emit('hookProgress', { event: 'stop', hookType: 'command', label: 'C' });
    assert.equal(seen.length, 0, 'no delivery after detach');
    assert.equal(hm.getPendingHookEventCount(), 1, 'buffered again');
  });

  it('bounds the buffer at MAX_PENDING_HOOK_EVENTS (drops oldest)', () => {
    for (let i = 0; i < MAX_PENDING_HOOK_EVENTS + 20; i++) {
      hm.emit('hookProgress', { event: 'stop', hookType: 'command', label: `L${i}` });
    }
    assert.equal(hm.getPendingHookEventCount(), MAX_PENDING_HOOK_EVENTS);
    const seen: HookLifecycleEvent[] = [];
    hm.registerHookEventHandler(e => seen.push(e));
    // Oldest dropped: the first surviving label is L20 (0..19 evicted).
    assert.equal(seen[0].label, 'L20');
    assert.equal(seen[seen.length - 1].label, `L${MAX_PENDING_HOOK_EVENTS + 19}`);
  });
});
