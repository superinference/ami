import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { HookManager } from '../src/hooks';

let hm: HookManager;
let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-hooks-lc-'));
  hm = new HookManager(tmpDir, 'test-session');
});

const sessionCtx = { sessionId: 'test', cwd: '/tmp' };
const subagentCtx = { sessionId: 'test', subagentId: 'sub-1', cwd: '/tmp' };
const compactCtx = { sessionId: 'test', cwd: '/tmp', messageCount: 10, tokenEstimate: 5000 };
const permCtx = { sessionId: 'test', cwd: '/tmp', toolName: 'bash', input: {} };
const promptCtx = { sessionId: 'test', cwd: '/tmp', prompt: 'hello' };
const taskCtx = { sessionId: 'test', cwd: '/tmp', taskId: 't-1', taskName: 'test' };

describe('HookManager — executeSessionStart', () => {
  it('returns void when no hooks registered', async () => {
    const result = await hm.executeSessionStart(sessionCtx as any);
    assert.equal(result, undefined);
  });

  it('returns initialUserMessage from hook', async () => {
    hm.onSessionStart(async () => ({ initialUserMessage: 'auto msg' }));
    const result = await hm.executeSessionStart(sessionCtx as any);
    assert.equal(result?.initialUserMessage, 'auto msg');
  });

  it('survives hook errors', async () => {
    hm.onSessionStart(async () => { throw new Error('boom'); });
    const result = await hm.executeSessionStart(sessionCtx as any);
    assert.equal(result, undefined);
  });
});

describe('HookManager — executeSessionEnd', () => {
  it('runs registered hooks', async () => {
    let called = false;
    hm.onSessionEnd(async () => { called = true; });
    await hm.executeSessionEnd(sessionCtx as any);
    assert.equal(called, true);
  });
});

describe('HookManager — executeSubagentStart/Stop', () => {
  it('runs subagent start hooks', async () => {
    let called = false;
    hm.onSubagentStart(async () => { called = true; });
    await hm.executeSubagentStart(subagentCtx as any);
    assert.equal(called, true);
  });

  it('runs subagent stop hooks', async () => {
    let called = false;
    hm.onSubagentStop(async () => { called = true; });
    await hm.executeSubagentStop(subagentCtx as any);
    assert.equal(called, true);
  });
});

describe('HookManager — executePreCompact/PostCompact', () => {
  it('runs pre-compact hooks', async () => {
    let called = false;
    hm.onPreCompact(async () => { called = true; });
    await hm.executePreCompact(compactCtx as any);
    assert.equal(called, true);
  });

  it('runs post-compact hooks', async () => {
    let called = false;
    hm.onPostCompact(async () => { called = true; });
    await hm.executePostCompact(compactCtx as any);
    assert.equal(called, true);
  });
});

describe('HookManager — executePermissionRequest', () => {
  it('returns first action from hook', async () => {
    hm.onPermissionRequest(async () => ({ action: 'allow' as const }));
    const result = await hm.executePermissionRequest(permCtx as any);
    assert.equal(result?.action, 'allow');
  });

  it('returns deny action', async () => {
    hm.onPermissionRequest(async () => ({ action: 'deny' as const, reason: 'nope' }));
    const result = await hm.executePermissionRequest(permCtx as any);
    assert.equal(result?.action, 'deny');
    assert.equal(result?.reason, 'nope');
  });

  it('returns void when hooks return no action', async () => {
    hm.onPermissionRequest(async () => undefined as any);
    const result = await hm.executePermissionRequest(permCtx as any);
    assert.equal(result, undefined);
  });
});

describe('HookManager — executePermissionDenied', () => {
  it('returns retry when hook allows', async () => {
    hm.onPermissionDenied(async () => ({ action: 'allow' } as any));
    const result = await hm.executePermissionDenied(permCtx as any);
    assert.equal(result?.retry, true);
  });

  it('returns void when no hooks override', async () => {
    const result = await hm.executePermissionDenied(permCtx as any);
    assert.equal(result, undefined);
  });
});

describe('HookManager — executeUserPromptSubmit', () => {
  it('returns additionalContext from hook', async () => {
    hm.onUserPromptSubmit(async () => ({ additionalContext: 'extra info' }));
    const result = await hm.executeUserPromptSubmit(promptCtx as any);
    assert.equal(result?.additionalContext, 'extra info');
  });

  it('returns void when no additional context', async () => {
    hm.onUserPromptSubmit(async () => undefined as any);
    const result = await hm.executeUserPromptSubmit(promptCtx as any);
    assert.equal(result, undefined);
  });
});

describe('HookManager — executeTaskCreated/Completed', () => {
  it('runs task created hooks', async () => {
    let called = false;
    hm.onTaskCreated(async () => { called = true; });
    await hm.executeTaskCreated(taskCtx as any);
    assert.equal(called, true);
  });

  it('runs task completed hooks', async () => {
    let called = false;
    hm.onTaskCompleted(async () => { called = true; });
    await hm.executeTaskCompleted(taskCtx as any);
    assert.equal(called, true);
  });
});
