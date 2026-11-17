/**
 * U022 (w81) — structured/typed `message` input: accept a DISCRIMINATED OBJECT, not
 * only a plain string.
 *
 * Mirrors Claude Code's SendMessage `Input.message`
 * (tools/SendMessageTool/UI.tsx L6-13), where `message` is a structured object
 * carrying a discriminated `type` field (e.g. `plan_approval_response`) plus typed
 * payload fields like `approve`, with `to` as a sibling. AMI previously typed
 * `message`/`content` as a plain string only and recovered typed intents by
 * JSON.parse-ing the string at runtime — so a model (or a cross-framework Claude
 * Code peer) that passed `message` as an OBJECT crashed (`.trim()` on a non-string)
 * and the inputSchema never advertised that structured message types exist.
 *
 * ADDITIVE: the canonical {@link coerceMessageContent} now serializes a structured
 * object to the JSON content the existing parser already routes, and the inputSchema
 * advertises the discriminated object form for schema-level discoverability. Plain
 * strings behave exactly as before.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  sendMessageTool,
  resetAllMailboxes,
  coerceMessageContent,
  generateRequestId,
  getMailbox,
  TEAM_LEAD_NAME,
} from '../src/tools/send-message';
import { resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

function ctx(agentId?: string): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, agentId } as ToolContext;
}

beforeEach(() => { resetAllMailboxes(); resetAgentRegistry(); });
afterEach(() => { resetAllMailboxes(); resetAgentRegistry(); });

describe('coerceMessageContent — structured-object + string normalization', () => {
  it('passes a plain string through unchanged (message alias wins over content)', () => {
    assert.equal(coerceMessageContent({ message: 'hello' }), 'hello');
    assert.equal(coerceMessageContent({ content: 'world' }), 'world');
    assert.equal(coerceMessageContent({ message: 'a', content: 'b' }), 'a');
  });

  it('serializes a discriminated object `message` to JSON content', () => {
    const out = coerceMessageContent({ message: { type: 'shutdown_request', reason: 'wrap up' } });
    assert.equal(out, JSON.stringify({ type: 'shutdown_request', reason: 'wrap up' }));
  });

  it('serializes an object `content` field too', () => {
    const out = coerceMessageContent({ content: { type: 'idle_notification', idleReason: 'available' } });
    assert.equal(out, JSON.stringify({ type: 'idle_notification', idleReason: 'available' }));
  });

  it('returns undefined for a missing message/content and for an unstringifiable object', () => {
    assert.equal(coerceMessageContent({}), undefined);
    assert.equal(coerceMessageContent({ message: 123 }), undefined);
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    assert.equal(coerceMessageContent({ message: circular }), undefined);
  });
});

describe('send_message execute — accepts a structured object message', () => {
  it('routes a structured-object shutdown_request exactly like the JSON-string form', async () => {
    const r = await sendMessageTool.execute(
      { to: 'worker', message: { type: 'shutdown_request', reason: 'please stop' } },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    // Correlation metadata proves it took the structured control path, not plain text.
    const meta = r.metadata as Record<string, unknown>;
    assert.equal(meta.target, 'worker');
    assert.equal(typeof meta.request_id, 'string');
    // The teammate actually received the structured payload.
    const box = getMailbox('worker');
    assert.equal(box.length, 1);
    const payload = JSON.parse(box[0].content);
    assert.equal(payload.type, 'shutdown_request');
    assert.equal(payload.reason, 'please stop');
  });

  it('routes a structured-object plan_approval_response (lead) from an object message', async () => {
    const rid = generateRequestId('plan', 'worker');
    const r = await sendMessageTool.execute(
      { to: 'worker', message: { type: 'plan_approval_response', requestId: rid, approve: true } },
      ctx(),
    );
    assert.equal(r.isError, undefined);
    const meta = r.metadata as Record<string, unknown>;
    assert.equal(meta.routing, true);
    assert.equal(meta.request_id, rid);
  });

  it('delivers a plain-text object-less send unchanged', async () => {
    const r = await sendMessageTool.execute({ to: 'worker', message: 'just chatting' }, ctx());
    assert.equal(r.isError, undefined);
    const box = getMailbox('worker');
    assert.equal(box[0].content, 'just chatting');
  });

  it('rejects an unstringifiable object message as empty content (no crash)', async () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const r = await sendMessageTool.execute({ to: 'worker', message: circular }, ctx());
    assert.ok(r.isError);
    assert.match(r.output, /must not be empty/);
  });
});

describe('send_message isReadOnlyForInput — structured object discrimination', () => {
  it('a structured-object control message is NOT read-only (stays on the permission path)', () => {
    const ro = sendMessageTool.isReadOnlyForInput?.({
      to: 'worker',
      message: { type: 'shutdown_request', reason: 'x' },
    });
    assert.equal(ro, false);
  });

  it('a plain-text object-less message remains read-only', () => {
    const ro = sendMessageTool.isReadOnlyForInput?.({ to: 'worker', message: 'hi' });
    assert.equal(ro, true);
  });
});

describe('send_message inputSchema — advertises the discriminated object form', () => {
  it('message property accepts an object (schema-level discoverability of type/approve)', () => {
    const msgProp = sendMessageTool.inputSchema.properties?.message as Record<string, unknown>;
    assert.ok(msgProp, 'expected a message property on the schema');
    const t = msgProp.type;
    // type is widened to allow an object, not only a string.
    assert.ok(Array.isArray(t) ? t.includes('object') : t === 'object',
      `expected message schema type to allow "object", got ${JSON.stringify(t)}`);
    // The description surfaces the discriminated structured-message protocol.
    assert.match(String(msgProp.description), /type/i);
  });

  it('keeps to + content required (backward-compatible with existing callers)', () => {
    assert.ok(sendMessageTool.inputSchema.required?.includes('to'));
    assert.ok(sendMessageTool.inputSchema.required?.includes('content'));
  });
});
