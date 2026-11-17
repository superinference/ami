// U007 (w82) — hooks.ts additive parity features:
//   - GAP 101/102: hasPendingToolCalls() idle-boundary helper (Claude Code's
//     hasToolCallsInLastAssistantTurn) + the `is_idle` stdin flag serialized to
//     pre/postSampling hooks so a background/maintenance hook can self-gate and
//     run ONLY when the agent is idle (not mid tool-chain).
//   - GAP 103: elicitation / elicitationResult hook context carries the full MCP
//     request params (mode, requestedSchema, url, elicitationId) and serializes
//     them to the hook stdin (requested_schema / elicitation_id snake aliases).
//   - GAP 119: PermissionRequest hook stdin carries the `permission_suggestions`
//     ('always allow' options) alias.
//   - GAP 148: a PermissionDenied COMMAND hook can grant a retry on exit 0 via
//     {"hookSpecificOutput":{"hookEventName":"PermissionDenied","retry":true}}
//     (or a flat top-level retry:true), mapped to { retry: true }.
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  HookManager,
  hasPendingToolCalls,
  serializeHookEvent,
  type HookContext,
  type PreSamplingContext,
  type ElicitationContext,
  type PermissionContext,
} from '../src/hooks';
import type { Message } from '../src/types';

function writeTmpHooks(entries: unknown[]): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w82-'));
  const dir = path.join(tmpDir, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

/** Build a capture command hook: it writes the full stdin payload it receives to
 *  `out`, so a test can assert exactly what was serialized to the hook. */
function captureHook(event: string, out: string, extra: Record<string, unknown> = {}): unknown {
  return { event, hook: { type: 'command', command: `cat > '${out}'`, ...extra } };
}

const asstWithCall: Message = {
  role: 'assistant',
  content: null,
  tool_calls: [{ id: 'call-1', type: 'function', function: { name: 'bash', arguments: '{}' } }],
} as Message;
const toolResult: Message = { role: 'tool', tool_call_id: 'call-1', content: 'ok' } as Message;
const asstNoCall: Message = { role: 'assistant', content: 'done' } as Message;
const userMsg: Message = { role: 'user', content: 'hi' } as Message;

let hm: HookManager;
beforeEach(() => { hm = new HookManager(); });

describe('w82 — hasPendingToolCalls (idle boundary)', () => {
  it('is idle (no pending) when there is no assistant message', () => {
    assert.equal(hasPendingToolCalls([]), false);
    assert.equal(hasPendingToolCalls([userMsg]), false);
  });
  it('is idle when the last assistant turn made no tool calls', () => {
    assert.equal(hasPendingToolCalls([userMsg, asstNoCall]), false);
  });
  it('reports pending when the last assistant turn has an unanswered tool call', () => {
    assert.equal(hasPendingToolCalls([userMsg, asstWithCall]), true);
  });
  it('is idle again once every tool call is answered', () => {
    assert.equal(hasPendingToolCalls([userMsg, asstWithCall, toolResult]), false);
  });
  it('uses the LAST assistant turn (an earlier answered call does not count)', () => {
    assert.equal(hasPendingToolCalls([asstWithCall, toolResult, asstNoCall]), false);
  });
  it('reports pending when ANY call in the last turn is unanswered', () => {
    const twoCalls: Message = {
      role: 'assistant', content: null, tool_calls: [
        { id: 'a', type: 'function', function: { name: 'x', arguments: '{}' } },
        { id: 'b', type: 'function', function: { name: 'y', arguments: '{}' } },
      ],
    } as Message;
    const onlyA: Message = { role: 'tool', tool_call_id: 'a', content: 'ok' } as Message;
    assert.equal(hasPendingToolCalls([twoCalls, onlyA]), true);
  });
});

describe('w82 — is_idle serialized to sampling hooks (GAP 101/102)', () => {
  it('serializeHookEvent emits the is_idle snake alias', () => {
    const parsed = JSON.parse(serializeHookEvent({ event: 'postSampling', isIdle: true }));
    assert.equal(parsed.isIdle, true);
    assert.equal(parsed.is_idle, true);
  });

  it('postSampling command hook receives is_idle:false while mid tool-chain, true when idle', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w82-idle-'));
    const out = path.join(tmpDir, 'payload');
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: [captureHook('postSampling', out)] }));
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostSampling({ messages: [userMsg, asstWithCall], turnCount: 1 } as HookContext);
    assert.equal(JSON.parse(fs.readFileSync(out, 'utf-8')).is_idle, false);

    await hm.executePostSampling({ messages: [userMsg, asstWithCall, toolResult], turnCount: 1 } as HookContext);
    assert.equal(JSON.parse(fs.readFileSync(out, 'utf-8')).is_idle, true);

    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preSampling command hook also receives is_idle', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w82-pre-'));
    const out = path.join(tmpDir, 'payload');
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: [captureHook('preSampling', out)] }));
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    await hm.executePreSampling({ messages: [userMsg, asstWithCall], turnCount: 1, apiMessages: [] } as PreSamplingContext);
    assert.equal(JSON.parse(fs.readFileSync(out, 'utf-8')).is_idle, false);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe('w82 — elicitation request params serialized (GAP 103)', () => {
  it('serializeHookEvent emits requested_schema / elicitation_id aliases + url + mode', () => {
    const parsed = JSON.parse(serializeHookEvent({
      event: 'elicitation', mcpServerName: 'srv', mode: 'form',
      requestedSchema: { type: 'object' }, url: 'https://x', elicitationId: 'el-7',
    }));
    assert.equal(parsed.mode, 'form');
    assert.equal(parsed.url, 'https://x');
    assert.deepEqual(parsed.requested_schema, { type: 'object' });
    assert.equal(parsed.elicitation_id, 'el-7');
  });

  it('elicitation command hook stdin carries the full request params', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w82-elicit-'));
    const out = path.join(tmpDir, 'payload');
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: [captureHook('elicitation', out)] }));
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const ctx: ElicitationContext = {
      mcpServerName: 'srv', message: 'pick', mode: 'form',
      requestedSchema: { type: 'object', properties: { x: { type: 'string' } } },
      url: 'https://example.test', elicitationId: 'el-42',
    };
    await hm.executeElicitation(ctx);
    const payload = JSON.parse(fs.readFileSync(out, 'utf-8'));
    assert.equal(payload.mode, 'form');
    assert.equal(payload.url, 'https://example.test');
    assert.equal(payload.elicitation_id, 'el-42');
    assert.deepEqual(payload.requested_schema.properties.x, { type: 'string' });
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe('w82 — permission_suggestions alias (GAP 119)', () => {
  it('serializeHookEvent emits permission_suggestions for the suggestions field', () => {
    const suggestions = [{ type: 'addRules', rules: [], behavior: 'allow' }];
    const parsed = JSON.parse(serializeHookEvent({ event: 'permissionRequest', toolName: 'bash', suggestions }));
    assert.deepEqual(parsed.permission_suggestions, suggestions);
  });

  it('permissionRequest command hook stdin carries permission_suggestions', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w82-perm-'));
    const out = path.join(tmpDir, 'payload');
    const dir = path.join(tmpDir, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: [captureHook('permissionRequest', out)] }));
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const ctx = {
      toolName: 'bash', toolInput: { command: 'ls' },
      suggestions: [{ type: 'addRules', rules: [{ toolName: 'bash' }], behavior: 'allow' }],
    } as unknown as PermissionContext;
    await hm.executePermissionRequest(ctx);
    const payload = JSON.parse(fs.readFileSync(out, 'utf-8'));
    assert.ok(Array.isArray(payload.permission_suggestions));
    assert.equal(payload.permission_suggestions[0].behavior, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe('w82 — PermissionDenied command-hook retry (GAP 148)', () => {
  it('maps hookSpecificOutput.retry:true to { retry: true }', async () => {
    const tmpDir = writeTmpHooks([{
      event: 'permissionDenied',
      hook: { type: 'command', command: `echo '{"hookSpecificOutput":{"hookEventName":"PermissionDenied","retry":true}}'` },
    }]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const result = await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as PermissionContext);
    assert.deepEqual(result, { retry: true });
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('maps a flat top-level retry:true to { retry: true }', async () => {
    const tmpDir = writeTmpHooks([{
      event: 'permissionDenied',
      hook: { type: 'command', command: `echo '{"retry":true}'` },
    }]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const result = await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as PermissionContext);
    assert.deepEqual(result, { retry: true });
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('does NOT retry when the hook emits nothing / no retry signal', async () => {
    const tmpDir = writeTmpHooks([{
      event: 'permissionDenied',
      hook: { type: 'command', command: `echo '{"foo":1}'` },
    }]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    const result = await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as PermissionContext);
    assert.equal(result, undefined);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
