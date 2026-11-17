import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  mapChildToSubagentEvent,
  clampRelayPreview,
  summarizeErrorOutput,
  MAX_RELAY_PREVIEW_CHARS,
} from '../src/tools/subagent-relay';
import type { EngineEvent } from '../src/types';

const map = (e: EngineEvent) => mapChildToSubagentEvent(e, 'agent-1', 2);

/* ---------------------------------------------------------------- *
 * U237 / w89 — mapChildToSubagentEvent liveness parity
 *
 * Gaps implemented (all in the pure relay mapper so the live roster reflects a
 * sub-agent's in-flight progress / failures / retries as they happen):
 *   A: surface a bounded tool FAILURE reason, not just a ✗ glyph
 *   C: bound relayed preview text so a run-away child can't flood the roster
 *   D: relay tool_use_progress (long-running tool live output) instead of dark
 *   H: relay error (error activity) + retry_attempt/error_classified (retry/error)
 * ---------------------------------------------------------------- */

describe('U237/w89 GAP A — bounded tool failure reason', () => {
  it('appends the first output line as the failure reason on an error result', () => {
    const r = map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'command not found: foo', isError: true });
    assert.equal(r?.type === 'subagent_progress' && r.detail, 'Running ✗ — command not found: foo');
  });

  it('a successful result carries no reason (just the ✓ glyph)', () => {
    const r = map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'bash', output: 'all good', isError: false });
    assert.equal(r?.type === 'subagent_progress' && r.detail, 'Running ✓');
  });

  it('picks the first NON-EMPTY output line and bounds it to 120 chars', () => {
    const r = map({ type: 'tool_use_result', toolCallId: 'c', toolName: 'grep', output: '\n   \nerror: ' + 'z'.repeat(300), isError: true });
    const detail = r?.type === 'subagent_progress' ? r.detail : '';
    assert.ok(detail.startsWith('Searching ✗ — error: '), detail);
    assert.ok(detail.endsWith('…'), detail);
    // 'Searching ✗ — ' prefix + 120-char bounded reason.
    assert.equal(detail.length, 'Searching ✗ — '.length + 120);
  });

  it('summarizeErrorOutput returns "" for blank output', () => {
    assert.equal(summarizeErrorOutput('\n  \n\t'), '');
    assert.equal(summarizeErrorOutput(''), '');
  });
});

describe('U237/w89 GAP C — bounded relayed preview', () => {
  it('clampRelayPreview is a pure, ellipsis-appending bound', () => {
    assert.equal(clampRelayPreview('short'), 'short');
    const long = 'a'.repeat(MAX_RELAY_PREVIEW_CHARS + 50);
    const out = clampRelayPreview(long);
    assert.equal(out.length, MAX_RELAY_PREVIEW_CHARS + 1); // + the ellipsis
    assert.ok(out.endsWith('…'));
  });

  it('clamps a flood of streamed text_delta before it crosses the boundary', () => {
    const r = map({ type: 'text_delta', text: 'x'.repeat(MAX_RELAY_PREVIEW_CHARS + 1000) });
    const detail = r?.type === 'subagent_progress' ? r.detail : '';
    assert.equal(detail.length, MAX_RELAY_PREVIEW_CHARS + 1);
    assert.ok(detail.endsWith('…'));
  });

  it('clamps a huge thinking_delta too', () => {
    const r = map({ type: 'thinking_delta', text: 'y'.repeat(MAX_RELAY_PREVIEW_CHARS + 5) });
    assert.equal((r?.type === 'subagent_progress' ? r.detail : '').length, MAX_RELAY_PREVIEW_CHARS + 1);
  });

  it('leaves a short delta untouched (no spurious ellipsis)', () => {
    const r = map({ type: 'text_delta', text: ' keep spacing ' });
    assert.equal(r?.type === 'subagent_progress' && r.detail, ' keep spacing ');
  });
});

describe('U237/w89 GAP D — relay in-flight tool progress', () => {
  it('relays tool_use_progress as a bounded text activity (was dark/null)', () => {
    const r = map({ type: 'tool_use_progress', toolCallId: 'c', data: 'installing deps… 42%' });
    assert.ok(r, 'long-running tool progress must surface, not go dark');
    assert.equal(r.type === 'subagent_progress' && r.activity, 'text');
    assert.equal(r.type === 'subagent_progress' && r.detail, 'installing deps… 42%');
  });

  it('drops an empty / whitespace-only progress chunk (no blank flicker)', () => {
    assert.equal(map({ type: 'tool_use_progress', toolCallId: 'c', data: '' }), null);
    assert.equal(map({ type: 'tool_use_progress', toolCallId: 'c', data: '   \n' }), null);
  });

  it('bounds a huge progress chunk', () => {
    const r = map({ type: 'tool_use_progress', toolCallId: 'c', data: 'q'.repeat(MAX_RELAY_PREVIEW_CHARS + 10) });
    assert.equal((r?.type === 'subagent_progress' ? r.detail : '').length, MAX_RELAY_PREVIEW_CHARS + 1);
  });
});

describe('U237/w89 GAP H — error / retry liveness', () => {
  it('relays error as a dedicated error activity (bounded)', () => {
    const r = map({ type: 'error', error: 'boom' });
    assert.equal(r?.type === 'subagent_progress' && r.activity, 'error');
    assert.equal(r?.type === 'subagent_progress' && r.detail, '⚠ boom');
  });

  it('bounds a huge error message', () => {
    const r = map({ type: 'error', error: 'z'.repeat(MAX_RELAY_PREVIEW_CHARS + 100) });
    const detail = r?.type === 'subagent_progress' ? r.detail : '';
    assert.equal(detail.length, MAX_RELAY_PREVIEW_CHARS + 1);
    assert.ok(detail.endsWith('…'));
  });

  it('relays retry_attempt as a retry activity with attempt/limit + delay', () => {
    const r = map({ type: 'retry_attempt', attempt: 2, maxRetries: 5, delayMs: 1000, statusCode: 429 });
    assert.equal(r?.type === 'subagent_progress' && r.activity, 'retry');
    assert.equal(r?.type === 'subagent_progress' && r.detail, 'retry 2/5 in 1000ms');
  });

  it('retry_attempt omits the delay clause when delayMs is 0', () => {
    const r = map({ type: 'retry_attempt', attempt: 1, maxRetries: 3, delayMs: 0, statusCode: null });
    assert.equal(r?.type === 'subagent_progress' && r.detail, 'retry 1/3');
  });

  it('error_classified with a pending retry reads as retry liveness', () => {
    const r = map({ type: 'error_classified', category: 'rate_limit', retryable: true, willRetry: true });
    assert.equal(r?.type === 'subagent_progress' && r.activity, 'retry');
    assert.equal(r?.type === 'subagent_progress' && r.detail, 'rate_limit (retrying)');
  });

  it('error_classified with no retry reads as a terminal error', () => {
    const r = map({ type: 'error_classified', category: 'auth', retryable: false, willRetry: false });
    assert.equal(r?.type === 'subagent_progress' && r.activity, 'error');
    assert.equal(r?.type === 'subagent_progress' && r.detail, 'auth');
  });
});
