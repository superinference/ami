/**
 * Unit U007 — hook output parsing/aggregation parity (Claude Code -> AMI).
 *
 * Gaps covered (all self-contained in core/core/src/hooks.ts):
 *  - [20] executePostToolUse / executeUserPromptSubmit aggregate ALL
 *    additionalContexts + blockingErrors from every matching hook (vs the old
 *    last-wins overwrite). Mirrors the reference's AggregatedHookResult which
 *    collects additionalContexts: string[] / blockingErrors: HookBlockingError[].
 *  - [17] PostToolUse hooks may emit `updatedMCPToolOutput` (Claude Code's key
 *    for MCP tool-output rewrites) as an alias of AMI's `updatedToolOutput`.
 *  - [26] Strict rejection of an unrecognized `decision` / `permissionDecision`
 *    value: a typo'd permission decision fails CLOSED (deny with a validation
 *    reason) instead of being silently ignored (Claude Code parseHookOutput
 *    throws "Unknown hook decision type").
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  unknownHookDecisionError,
  type PostToolUseContext,
  type UserPromptContext,
  type PermissionContext,
} from '../src/hooks';

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

function postCtx(overrides: Partial<PostToolUseContext> = {}): PostToolUseContext {
  return { messages: [], turnCount: 1, toolName: 'bash', toolInput: {}, toolOutput: 'ok', isError: false, ...overrides };
}
function promptCtx(overrides: Partial<UserPromptContext> = {}): UserPromptContext {
  return { prompt: 'hi', turnCount: 1, ...overrides };
}
function permCtx(overrides: Partial<PermissionContext> = {}): PermissionContext {
  return { toolName: 'bash', toolInput: { command: 'ls' }, ...overrides };
}

function writeTmpHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

// ---------------------------------------------------------------------------
// [20] Aggregate ALL additionalContexts + blockingErrors (vs last-wins)
// ---------------------------------------------------------------------------
describe('executePostToolUse — aggregates all hook contributions', () => {
  it('joins additionalContext from every hook (not last-wins)', async () => {
    hm.onPostToolUse(async () => ({ additionalContext: 'first' }));
    hm.onPostToolUse(async () => ({ additionalContext: 'second' }));
    const res = await hm.executePostToolUse(postCtx());
    assert.ok(res);
    assert.ok(res!.additionalContext!.includes('first'), 'keeps first hook context');
    assert.ok(res!.additionalContext!.includes('second'), 'keeps second hook context');
  });

  it('joins blockingError from every hook (not last-wins)', async () => {
    hm.onPostToolUse(async () => ({ blockingError: 'blockA' }));
    hm.onPostToolUse(async () => ({ blockingError: 'blockB' }));
    const res = await hm.executePostToolUse(postCtx());
    assert.ok(res);
    assert.ok(res!.blockingError!.includes('blockA'));
    assert.ok(res!.blockingError!.includes('blockB'));
  });
});

describe('executeUserPromptSubmit — aggregates all hook contributions', () => {
  it('joins additionalContext from every hook (not last-wins)', async () => {
    hm.onUserPromptSubmit(async () => ({ additionalContext: 'ctx1' }));
    hm.onUserPromptSubmit(async () => ({ additionalContext: 'ctx2' }));
    const res = await hm.executeUserPromptSubmit(promptCtx());
    assert.ok(res);
    assert.ok(res!.additionalContext!.includes('ctx1'));
    assert.ok(res!.additionalContext!.includes('ctx2'));
  });

  it('joins blockingError from every hook (not last-wins)', async () => {
    hm.onUserPromptSubmit(async () => ({ blockingError: 'eA' }));
    hm.onUserPromptSubmit(async () => ({ blockingError: 'eB' }));
    const res = await hm.executeUserPromptSubmit(promptCtx());
    assert.ok(res);
    assert.ok(res!.blockingError!.includes('eA'));
    assert.ok(res!.blockingError!.includes('eB'));
  });
});

// ---------------------------------------------------------------------------
// [17] PostToolUse updatedMCPToolOutput alias of updatedToolOutput
// ---------------------------------------------------------------------------
describe('postToolUse command hook — updatedMCPToolOutput alias', () => {
  it('accepts updatedMCPToolOutput as a string tool-output rewrite', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'printf \'{"updatedMCPToolOutput":"rewritten"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePostToolUse(postCtx());
    assert.ok(res);
    assert.equal(res!.updatedToolOutput, 'rewritten');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('coerces a non-string updatedMCPToolOutput to JSON', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'printf \'{"updatedMCPToolOutput":{"a":1}}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePostToolUse(postCtx());
    assert.ok(res);
    assert.equal(res!.updatedToolOutput, '{"a":1}');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('prefers updatedToolOutput when both keys are present', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'printf \'{"updatedToolOutput":"primary","updatedMCPToolOutput":"secondary"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePostToolUse(postCtx());
    assert.ok(res);
    assert.equal(res!.updatedToolOutput, 'primary');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// [26] Strict rejection of unknown decision / permissionDecision
// ---------------------------------------------------------------------------
describe('unknownHookDecisionError — validator', () => {
  it('flags an unknown decision value', () => {
    assert.match(unknownHookDecisionError({ decision: 'maybe' })!, /Unknown hook decision type/);
  });
  it('flags an unknown permissionDecision value', () => {
    assert.match(unknownHookDecisionError({ permissionDecision: 'perhaps' })!, /Unknown hook permissionDecision type/);
  });
  it('accepts valid decision values', () => {
    for (const d of ['approve', 'deny', 'ask', 'block']) {
      assert.equal(unknownHookDecisionError({ decision: d }), undefined);
    }
  });
  it('accepts valid permissionDecision values', () => {
    for (const d of ['allow', 'deny', 'ask']) {
      assert.equal(unknownHookDecisionError({ permissionDecision: d }), undefined);
    }
  });
  it('returns undefined when no decision field present / undefined input', () => {
    assert.equal(unknownHookDecisionError({}), undefined);
    assert.equal(unknownHookDecisionError(undefined), undefined);
  });
});

describe('permissionRequest command hook — unknown decision fails closed', () => {
  it('denies with a validation reason on an unrecognized permissionDecision', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionRequest', hook: { type: 'command', command: 'printf \'{"permissionDecision":"maybe"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePermissionRequest(permCtx());
    assert.ok(res);
    assert.equal(res!.action, 'deny');
    assert.match(res!.reason!, /Unknown hook permissionDecision type/);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('still allows a valid approve decision', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionRequest', hook: { type: 'command', command: 'printf \'{"decision":"approve"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePermissionRequest(permCtx());
    assert.ok(res);
    assert.equal(res!.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
