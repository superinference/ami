/**
 * Unit U007 (w80) — hook result parity (the reference -> AMI). All gaps are
 * self-contained in core/core/src/hooks.ts.
 *
 * Gaps covered:
 *  - SessionStart/Setup hook result `additionalContexts[]` are accumulated across
 *    EVERY hook and surfaced as one combined list for a single
 *    hook_additional_context attachment (Claude Code's SessionStart/Setup
 *    additionalContexts aggregation). Previously SessionStart only recognised
 *    initialUserMessage/systemMessage and Setup returned void.
 *  - PostCompact hook receives the generated `compactSummary` + `trigger`
 *    (Claude Code's executePostCompactHooks input) and may return a
 *    `userDisplayMessage` that is surfaced to the user. Previously PostCompact
 *    got only {messageCount, tokenEstimate} and returned void.
 *  - `decisionClassification` enum (user_temporary | user_permanent |
 *    user_reject) carried on BOTH allow and deny permission decisions, parsed
 *    leniently (a malformed value is dropped, not rejected).
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  extractSessionAdditionalContexts,
  isHookDecisionClassification,
  HOOK_DECISION_CLASSIFICATIONS,
  serializeHookEvent,
  getHookEventMetadata,
  type SessionContext,
  type CompactContext,
  type PermissionContext,
} from '../src/hooks';

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

function sessCtx(overrides: Partial<SessionContext> = {}): SessionContext {
  return { sessionId: 's1', cwd: process.cwd(), source: 'startup', ...overrides };
}
function compactCtx(overrides: Partial<CompactContext> = {}): CompactContext {
  return { messageCount: 10, tokenEstimate: 1000, ...overrides };
}
function permCtx(overrides: Partial<PermissionContext> = {}): PermissionContext {
  return { toolName: 'bash', toolInput: { command: 'ls' }, ...overrides };
}

function writeTmpHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w80-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

// ---------------------------------------------------------------------------
// extractSessionAdditionalContexts — unit
// ---------------------------------------------------------------------------
describe('extractSessionAdditionalContexts', () => {
  it('collects additionalContexts[] and additionalContext', () => {
    const got = extractSessionAdditionalContexts(JSON.stringify({ additionalContexts: ['a', 'b'], additionalContext: 'c' }));
    assert.deepEqual(got, ['a', 'b', 'c']);
  });
  it('EXCLUDES systemMessage (routed to initialUserMessage for SessionStart)', () => {
    const got = extractSessionAdditionalContexts(JSON.stringify({ systemMessage: 'warn', additionalContext: 'ctx' }));
    assert.deepEqual(got, ['ctx'], 'systemMessage must not leak into additionalContexts');
  });
  it('ignores the SYSTEM: sentinel and non-JSON input', () => {
    assert.deepEqual(extractSessionAdditionalContexts('SYSTEM:hello'), []);
    assert.deepEqual(extractSessionAdditionalContexts('plain text'), []);
  });
  it('drops non-string entries in additionalContexts[]', () => {
    const got = extractSessionAdditionalContexts(JSON.stringify({ additionalContexts: ['keep', 42, null, ''] }));
    assert.deepEqual(got, ['keep']);
  });
});

// ---------------------------------------------------------------------------
// executeSessionStart — additionalContexts aggregation
// ---------------------------------------------------------------------------
describe('executeSessionStart — additionalContexts aggregation', () => {
  it('accumulates additionalContexts from every hook into one list', async () => {
    hm.onSessionStart(async () => ({ additionalContexts: ['from-A'] }));
    hm.onSessionStart(async () => ({ additionalContexts: ['from-B1', 'from-B2'] }));
    const res = await hm.executeSessionStart(sessCtx());
    assert.ok(res);
    assert.deepEqual(res!.additionalContexts, ['from-A', 'from-B1', 'from-B2']);
  });

  it('returns additionalContexts alongside the joined initialUserMessage', async () => {
    hm.onSessionStart(async () => ({ initialUserMessage: 'msg', additionalContexts: ['ctx'] }));
    const res = await hm.executeSessionStart(sessCtx());
    assert.ok(res);
    assert.equal(res!.initialUserMessage, 'msg');
    assert.deepEqual(res!.additionalContexts, ['ctx']);
  });

  it('leaves additionalContexts undefined when no hook contributes one', async () => {
    hm.onSessionStart(async () => ({ initialUserMessage: 'just a message' }));
    const res = await hm.executeSessionStart(sessCtx());
    assert.ok(res);
    assert.equal(res!.additionalContexts, undefined);
  });

  it('extracts additionalContexts from a command hook result (full arm path)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'printf \'{"additionalContexts":["cmd-ctx"],"initialUserMessage":"hi"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executeSessionStart(sessCtx());
    assert.ok(res);
    assert.deepEqual(res!.additionalContexts, ['cmd-ctx']);
    assert.equal(res!.initialUserMessage, 'hi');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// executeSetup — additionalContexts aggregation
// ---------------------------------------------------------------------------
describe('executeSetup — additionalContexts aggregation', () => {
  it('accumulates additionalContexts across setup hooks', async () => {
    hm.onSetup(async () => ({ additionalContexts: ['s1'] }));
    hm.onSetup(async () => ({ additionalContexts: ['s2'] }));
    const res = await hm.executeSetup(sessCtx({ trigger: 'init' }));
    assert.ok(res);
    assert.deepEqual(res!.additionalContexts, ['s1', 's2']);
  });

  it('returns void when no setup hook contributes context', async () => {
    hm.onSetup(async () => undefined);
    const res = await hm.executeSetup(sessCtx({ trigger: 'init' }));
    assert.equal(res, undefined);
  });

  it('extracts additionalContexts[] from a setup command hook', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'setup', hook: { type: 'command', command: 'printf \'{"additionalContexts":["setup-ctx"]}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executeSetup(sessCtx({ trigger: 'init' }));
    assert.ok(res);
    assert.deepEqual(res!.additionalContexts, ['setup-ctx']);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// executePostCompact — compactSummary + trigger input, userDisplayMessage output
// ---------------------------------------------------------------------------
describe('executePostCompact — compactSummary + userDisplayMessage', () => {
  it('forwards compactSummary and trigger to the hook', async () => {
    let seen: CompactContext | undefined;
    hm.onPostCompact(async (ctx) => { seen = ctx; });
    await hm.executePostCompact(compactCtx({ compactSummary: 'the summary', trigger: 'auto' }));
    assert.ok(seen);
    assert.equal(seen!.compactSummary, 'the summary');
    assert.equal(seen!.trigger, 'auto');
  });

  it('joins userDisplayMessage from every post-compact hook', async () => {
    hm.onPostCompact(async () => ({ userDisplayMessage: 'line-A' }));
    hm.onPostCompact(async () => ({ userDisplayMessage: 'line-B' }));
    const res = await hm.executePostCompact(compactCtx({ compactSummary: 's' }));
    assert.ok(res);
    assert.equal(res!.userDisplayMessage, 'line-A\nline-B');
  });

  it('returns void when no hook surfaces a message', async () => {
    hm.onPostCompact(async () => undefined);
    const res = await hm.executePostCompact(compactCtx());
    assert.equal(res, undefined);
  });

  it('a post-compact command hook can emit userDisplayMessage', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postCompact', hook: { type: 'command', command: 'printf \'{"userDisplayMessage":"compacted ok"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePostCompact(compactCtx({ compactSummary: 'S', trigger: 'manual' }));
    assert.ok(res);
    assert.equal(res!.userDisplayMessage, 'compacted ok');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('serializes compact_summary into the hook stdin payload', () => {
    const payload = serializeHookEvent({ event: 'postCompact', messageCount: 3, trigger: 'auto', compactSummary: 'sum' });
    const obj = JSON.parse(payload);
    assert.equal(obj.compact_summary, 'sum');
    assert.equal(obj.compactSummary, 'sum');
    assert.equal(obj.trigger, 'auto');
  });

  it('documents trigger + compact_summary + userDisplayMessage in postCompact metadata', () => {
    const meta = getHookEventMetadata('postCompact');
    assert.ok(meta);
    assert.match(meta!.description, /compact_summary/);
    assert.match(meta!.description, /userDisplayMessage/);
  });
});

// ---------------------------------------------------------------------------
// decisionClassification enum
// ---------------------------------------------------------------------------
describe('isHookDecisionClassification', () => {
  it('accepts the three known classifications', () => {
    for (const c of HOOK_DECISION_CLASSIFICATIONS) assert.equal(isHookDecisionClassification(c), true);
    assert.deepEqual([...HOOK_DECISION_CLASSIFICATIONS], ['user_temporary', 'user_permanent', 'user_reject']);
  });
  it('rejects unknown / non-string values (lenient-drop)', () => {
    for (const v of ['user_maybe', '', 42, null, undefined, {}]) assert.equal(isHookDecisionClassification(v), false);
  });
});

describe('executePermissionRequest — decisionClassification', () => {
  it('carries decisionClassification on an allow decision', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionRequest', hook: { type: 'command', command: 'printf \'{"permissionDecision":"allow","decisionClassification":"user_permanent"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePermissionRequest(permCtx());
    assert.ok(res);
    assert.equal(res!.action, 'allow');
    assert.equal(res!.decisionClassification, 'user_permanent');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('carries decisionClassification on a deny decision', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionRequest', hook: { type: 'command', command: 'printf \'{"permissionDecision":"deny","reason":"no","decisionClassification":"user_reject"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePermissionRequest(permCtx());
    assert.ok(res);
    assert.equal(res!.action, 'deny');
    assert.equal(res!.decisionClassification, 'user_reject');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('drops a malformed decisionClassification instead of rejecting the decision', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionRequest', hook: { type: 'command', command: 'printf \'{"permissionDecision":"allow","decisionClassification":"garbage"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const res = await hm.executePermissionRequest(permCtx());
    assert.ok(res);
    assert.equal(res!.action, 'allow');
    assert.equal(res!.decisionClassification, undefined);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
