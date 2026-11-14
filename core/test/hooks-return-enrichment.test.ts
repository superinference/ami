/**
 * Parity tests for hook execute* RETURN-VALUE enrichments (Claude->AMI parity):
 *  - Stop hooks can block turn-end (blockingErrors) / prevent continuation
 *    (preventContinuation + stopReason) + stopHookActive re-entrancy guard.
 *  - PostToolUse hooks can block / prevent continuation.
 *  - SubagentStart hooks contribute additionalContexts.
 *  - PreCompact hooks inject/override compaction instructions + display message,
 *    and receive trigger + customInstructions.
 *  - TaskCompleted blocking hooks can veto the completion (blockingErrors).
 *  - Elicitation / ElicitationResult hooks return a programmatic response /
 *    override / blockingError.
 *  - watchPaths: hook-output-driven dynamic FileChanged watch registration.
 *  - Shared decoder helpers.
 */

import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  extractHookBlockingError,
  tryParseHookJson,
  mergeHookInstructions,
  parseWatchPaths,
  extractAdditionalContexts,
  parseElicitationResult,
  type HookContext,
} from '../src/hooks';

let hm: HookManager;
const ctx: HookContext = { messages: [], turnCount: 1 };

beforeEach(() => { hm = new HookManager(); });

// ---------------------------------------------------------------------------
// Shared decoder helpers (canonical — reused across execute* methods)
// ---------------------------------------------------------------------------

describe('shared hook-output decoders', () => {
  it('extractHookBlockingError pulls the reason from a DENY sentinel', () => {
    assert.equal(extractHookBlockingError('DENY: nope'), 'nope');
    assert.equal(extractHookBlockingError('SYSTEM: hi'), undefined);
    assert.equal(extractHookBlockingError(''), undefined);
    assert.equal(extractHookBlockingError('{"a":1}'), undefined);
  });

  it('tryParseHookJson decodes objects only', () => {
    assert.deepEqual(tryParseHookJson('{"a":1}'), { a: 1 });
    assert.equal(tryParseHookJson('[]'), undefined);
    assert.equal(tryParseHookJson('plain text'), undefined);
    assert.equal(tryParseHookJson('{bad'), undefined);
    assert.equal(tryParseHookJson(''), undefined);
  });

  it('mergeHookInstructions appends hook text blank-line separated', () => {
    assert.equal(mergeHookInstructions('a', 'b'), 'a\n\nb');
    assert.equal(mergeHookInstructions('a', undefined), 'a');
    assert.equal(mergeHookInstructions(undefined, 'b'), 'b');
    assert.equal(mergeHookInstructions(undefined, undefined), undefined);
    assert.equal(mergeHookInstructions('', ''), undefined);
    assert.equal(mergeHookInstructions(null, 'b'), 'b');
  });

  it('parseWatchPaths extracts non-empty string paths only', () => {
    assert.deepEqual(parseWatchPaths('{"watchPaths":["/a","/b"]}'), ['/a', '/b']);
    assert.deepEqual(parseWatchPaths('{"watchPaths":["/a","",3]}'), ['/a']);
    assert.deepEqual(parseWatchPaths('{"watchPaths":"nope"}'), []);
    assert.deepEqual(parseWatchPaths('plain'), []);
  });

  it('extractAdditionalContexts gathers from array/string/systemMessage/SYSTEM:', () => {
    assert.deepEqual(extractAdditionalContexts('{"additionalContexts":["x","y"]}'), ['x', 'y']);
    assert.deepEqual(extractAdditionalContexts('{"additionalContext":"z"}'), ['z']);
    assert.deepEqual(extractAdditionalContexts('{"systemMessage":"s"}'), ['s']);
    assert.deepEqual(extractAdditionalContexts('SYSTEM: hello'), ['hello']);
    assert.deepEqual(extractAdditionalContexts('nothing'), []);
  });

  it('parseElicitationResult decodes action/content/blockingError', () => {
    assert.deepEqual(parseElicitationResult('{"action":"accept","content":{"k":1}}'), { action: 'accept', content: { k: 1 } });
    assert.deepEqual(parseElicitationResult('DENY: no'), { blockingError: 'no' });
    assert.deepEqual(parseElicitationResult('{"action":"bogus"}'), undefined);
    assert.equal(parseElicitationResult('plain'), undefined);
  });
});

// ---------------------------------------------------------------------------
// Stop hooks (H1/H2)
// ---------------------------------------------------------------------------

describe('executeStop — blocking / preventContinuation', () => {
  it('returns a neutral result with no hooks', async () => {
    assert.deepEqual(await hm.executeStop(ctx), { blockingErrors: [], preventContinuation: false, stopReason: undefined });
  });

  it('aggregates blockingErrors from stop hooks', async () => {
    hm.onStop(async () => ({ blockingError: 'first' }));
    hm.onStop(async () => ({ blockingError: 'second' }));
    hm.onStop(async () => undefined);
    const r = await hm.executeStop(ctx);
    assert.deepEqual(r.blockingErrors, ['first', 'second']);
    assert.equal(r.preventContinuation, false);
  });

  it('honors preventContinuation + stopReason', async () => {
    hm.onStop(async () => ({ preventContinuation: true, stopReason: 'keep going' }));
    const r = await hm.executeStop(ctx);
    assert.equal(r.preventContinuation, true);
    assert.equal(r.stopReason, 'keep going');
  });

  it('stopHookActive guard prevents recursive re-entry', async () => {
    let innerResult: unknown;
    let ran = 0;
    hm.onStop(async () => {
      ran++;
      innerResult = await hm.executeStop(ctx); // re-entrant call
    });
    await hm.executeStop(ctx);
    assert.equal(ran, 1, 'hook runs once — inner call did not re-run hooks');
    assert.deepEqual(innerResult, { blockingErrors: [], preventContinuation: false });
  });

  it('a throwing stop hook fires stopFailure and does not abort others', async () => {
    const failures: number[] = [];
    hm.onStopFailure(async (c) => { failures.push(c.hookIndex); });
    hm.onStop(async () => { throw new Error('boom'); });
    hm.onStop(async () => ({ blockingError: 'after' }));
    const r = await hm.executeStop(ctx);
    assert.deepEqual(failures, [0]);
    assert.deepEqual(r.blockingErrors, ['after']);
  });
});

// ---------------------------------------------------------------------------
// PostToolUse hooks (H7)
// ---------------------------------------------------------------------------

describe('executePostToolUse — block / preventContinuation', () => {
  const ptuCtx = { ...ctx, toolName: 'bash', toolInput: {}, toolOutput: 'out', isError: false };

  it('surfaces a blockingError', async () => {
    hm.onPostToolUse(async () => ({ blockingError: 'bad output' }));
    const r = await hm.executePostToolUse(ptuCtx);
    assert.equal(r && r.blockingError, 'bad output');
  });

  it('surfaces preventContinuation + stopReason alongside context', async () => {
    hm.onPostToolUse(async () => ({ additionalContext: 'ctx', preventContinuation: true, stopReason: 'halt' }));
    const r = await hm.executePostToolUse(ptuCtx);
    assert.equal(r && r.additionalContext, 'ctx');
    assert.equal(r && r.preventContinuation, true);
    assert.equal(r && r.stopReason, 'halt');
  });

  it('returns void when no hook contributes', async () => {
    hm.onPostToolUse(async () => undefined);
    assert.equal(await hm.executePostToolUse(ptuCtx), undefined);
  });
});

// ---------------------------------------------------------------------------
// SubagentStart hooks (H8)
// ---------------------------------------------------------------------------

describe('executeSubagentStart — additionalContexts', () => {
  const sub = { parentSessionId: 'p', subagentSessionId: 's', prompt: 'go' };

  it('aggregates additionalContexts across hooks', async () => {
    hm.onSubagentStart(async () => ({ additionalContexts: ['a', 'b'] }));
    hm.onSubagentStart(async () => ({ additionalContexts: ['c'] }));
    hm.onSubagentStart(async () => undefined);
    const r = await hm.executeSubagentStart(sub);
    assert.deepEqual(r && r.additionalContexts, ['a', 'b', 'c']);
  });

  it('returns void when none contribute', async () => {
    hm.onSubagentStart(async () => undefined);
    assert.equal(await hm.executeSubagentStart(sub), undefined);
  });
});

// ---------------------------------------------------------------------------
// PreCompact hooks (H3)
// ---------------------------------------------------------------------------

describe('executePreCompact — instruction injection', () => {
  const cctx = { messageCount: 10, tokenEstimate: 1000, trigger: 'manual' as const, customInstructions: 'user' };

  it('merges newCustomInstructions and keeps last userDisplayMessage', async () => {
    hm.onPreCompact(async () => ({ newCustomInstructions: 'one', userDisplayMessage: 'm1' }));
    hm.onPreCompact(async () => ({ newCustomInstructions: 'two', userDisplayMessage: 'm2' }));
    const r = await hm.executePreCompact(cctx);
    assert.equal(r && r.newCustomInstructions, 'one\n\ntwo');
    assert.equal(r && r.userDisplayMessage, 'm2');
  });

  it('passes trigger + customInstructions to the hook context', async () => {
    let seen: { trigger?: string; customInstructions?: string | null } = {};
    hm.onPreCompact(async (c) => { seen = { trigger: c.trigger, customInstructions: c.customInstructions }; });
    await hm.executePreCompact(cctx);
    assert.equal(seen.trigger, 'manual');
    assert.equal(seen.customInstructions, 'user');
  });

  it('returns void when no hook injects', async () => {
    hm.onPreCompact(async () => undefined);
    assert.equal(await hm.executePreCompact(cctx), undefined);
  });
});

// ---------------------------------------------------------------------------
// TaskCompleted blocking hooks (H9)
// ---------------------------------------------------------------------------

describe('executeTaskCompleted — veto', () => {
  const tctx = { taskId: 't1', subject: 'Do it', status: 'completed' };

  it('collects blockingErrors that veto completion', async () => {
    hm.onTaskCompleted(async () => ({ blockingError: 'tests failing' }));
    const r = await hm.executeTaskCompleted(tctx);
    assert.deepEqual(r && r.blockingErrors, ['tests failing']);
  });

  it('returns void when no hook vetoes', async () => {
    hm.onTaskCompleted(async () => undefined);
    assert.equal(await hm.executeTaskCompleted(tctx), undefined);
  });
});

// ---------------------------------------------------------------------------
// Elicitation hooks (H4/H5)
// ---------------------------------------------------------------------------

describe('executeElicitation / executeElicitationResult', () => {
  const ectx = { mcpServerName: 'srv', message: 'approve?' };

  it('first hook with a response short-circuits', async () => {
    let secondRan = false;
    hm.onElicitation(async () => ({ action: 'accept', content: { ok: true } }));
    hm.onElicitation(async () => { secondRan = true; return { action: 'decline' }; });
    const r = await hm.executeElicitation(ectx);
    assert.deepEqual(r, { action: 'accept', content: { ok: true } });
    assert.equal(secondRan, false);
  });

  it('a blockingError short-circuits elicitation', async () => {
    hm.onElicitation(async () => ({ blockingError: 'not allowed' }));
    assert.deepEqual(await hm.executeElicitation(ectx), { blockingError: 'not allowed' });
  });

  it('elicitationResult can override action/content', async () => {
    hm.onElicitationResult(async () => ({ action: 'cancel' }));
    assert.deepEqual(await hm.executeElicitationResult(ectx), { action: 'cancel' });
  });

  it('returns void when no hook responds', async () => {
    hm.onElicitation(async () => undefined);
    assert.equal(await hm.executeElicitation(ectx), undefined);
  });
});

// ---------------------------------------------------------------------------
// UserPromptSubmit block / deny (H42/H43/H44)
// ---------------------------------------------------------------------------

describe('executeUserPromptSubmit — block / context', () => {
  const uctx = { prompt: 'do a thing', turnCount: 0 };

  it('injects additionalContext', async () => {
    hm.onUserPromptSubmit(async () => ({ additionalContext: 'extra' }));
    const r = await hm.executeUserPromptSubmit(uctx);
    assert.equal(r && r.additionalContext, 'extra');
  });

  it('blocks the prompt with a blockingError', async () => {
    hm.onUserPromptSubmit(async () => ({ blockingError: 'forbidden' }));
    const r = await hm.executeUserPromptSubmit(uctx);
    assert.equal(r && r.blockingError, 'forbidden');
  });

  it('returns void when no hook contributes', async () => {
    hm.onUserPromptSubmit(async () => undefined);
    assert.equal(await hm.executeUserPromptSubmit(uctx), undefined);
  });
});

// ---------------------------------------------------------------------------
// ConfigChange veto (H45)
// ---------------------------------------------------------------------------

describe('executeConfigChange — veto', () => {
  const cctx = { source: 'settings.json', filePath: '/x/.ami/config.json' };

  it('collects blockingErrors that veto the change', async () => {
    hm.onConfigChange(async () => ({ blockingError: 'invalid config' }));
    const r = await hm.executeConfigChange(cctx);
    assert.deepEqual(r && r.blockingErrors, ['invalid config']);
  });

  it('returns void when no hook vetoes', async () => {
    hm.onConfigChange(async () => undefined);
    assert.equal(await hm.executeConfigChange(cctx), undefined);
  });
});

// ---------------------------------------------------------------------------
// watchPaths dynamic registration (H10) — through a real command hook
// ---------------------------------------------------------------------------

describe('watchPaths registration via hook output', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-hooks-watch-'));
  after(() => { try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('registers watchPaths from a sessionStart command hook and emits an event', async () => {
    const amiDir = path.join(tmp, '.ami');
    fs.mkdirSync(amiDir, { recursive: true });
    fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
      hooks: [{
        event: 'sessionStart',
        hook: { type: 'command', command: `printf '{"watchPaths":["${tmp}/w1","${tmp}/w2"]}'` },
      }],
    }));

    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);

    const events: Array<{ event: string; paths: string[] }> = [];
    hm.on('watchPaths', (e) => events.push(e));

    const res = await hm.executeSessionStart({ sessionId: 'sess', cwd: tmp });
    assert.ok(res && Array.isArray(res.watchPaths), 'sessionStart returns watchPaths');
    assert.deepEqual(res!.watchPaths, [`${tmp}/w1`, `${tmp}/w2`]);
    assert.deepEqual(hm.getWatchedPaths(), [`${tmp}/w1`, `${tmp}/w2`]);
    assert.equal(events.length, 1);
    assert.equal(events[0].event, 'sessionStart');

    // Idempotent: re-running does not re-emit already-watched paths.
    await hm.executeSessionStart({ sessionId: 'sess', cwd: tmp });
    assert.equal(events.length, 1, 'no duplicate watchPaths event for same paths');
  });
});
