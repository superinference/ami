/**
 * U020 — LLM-generated past-tense tool-use summary label (core unit tests).
 *
 * Parity with the reference's `generateToolUseSummary`
 * (services/toolUseSummary/toolUseSummaryGenerator.ts): a cheap, deterministic aux
 * model call that produces a short, past-tense, git-commit-subject-style label
 * describing what a BATCH of completed tool calls accomplished, feeding each tool's
 * name/input/output plus a user-intent prefix, non-critical (empty on failure).
 *
 * AMI builds this on the SHARED auxiliary-query infrastructure (runAuxQuery /
 * reportAuxResult / sanitizeTitle / truncateTitle), so there is no second stream
 * loop or sanitiser — no semantic duplication.
 *
 * RED on revert: deleting generateToolUseSummaryLabel (or regressing it to a bare
 * slice / no sanitisation / no empty-batch short-circuit) fails these tests.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  generateToolUseSummaryLabel,
  buildToolUseSummaryPrompt,
  truncateJsonForSummary,
  TOOL_USE_SUMMARY_PROMPT,
  TOOL_USE_SUMMARY_LABEL_MAX,
  TOOL_USE_SUMMARY_IO_MAX,
} from '../src/title-generator';
import type { AuxQueryResult, ToolUseSummaryInput } from '../src/title-generator';
import type { ProviderConfig } from '../src/types';
import type { streamChatCompletion } from '../src/provider';

const provider: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'k',
  model: 'the-model',
  maxTokens: 50,
};

/** Streamer that emits `text` as one content_delta then a `done` chunk, and records
 *  the provider + messages it was called with. */
function seam(
  text: string,
  calls?: { provider?: ProviderConfig; messages?: unknown; count: number },
): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* (p: any, m: any) {
    if (calls) { calls.provider = p; calls.messages = m; calls.count++; }
    if (text) yield { type: 'content_delta', text } as any;
    yield { type: 'done', responseHeaders: { 'x-request-id': 'req-1' } } as any;
  }) as unknown as typeof streamChatCompletion;
}

/** Streamer that yields an error chunk (simulating a provider/stream failure). */
function errorSeam(errText: string): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* () {
    yield { type: 'error', error: errText } as any;
  }) as unknown as typeof streamChatCompletion;
}

const sampleTools: ToolUseSummaryInput[] = [
  { name: 'bash', input: { command: 'npm test' }, output: '3 failing' },
  { name: 'file_edit', input: { file_path: 'src/user.ts' }, output: 'ok' },
];

describe('U020 TOOL_USE_SUMMARY_PROMPT (verbatim Claude parity)', () => {
  it('is a past-tense, git-commit-subject label prompt with the reference examples', () => {
    assert.ok(TOOL_USE_SUMMARY_PROMPT.includes('short summary label'));
    assert.ok(TOOL_USE_SUMMARY_PROMPT.includes('truncates around 30 characters'));
    assert.ok(TOOL_USE_SUMMARY_PROMPT.includes('git-commit-subject'));
    assert.ok(TOOL_USE_SUMMARY_PROMPT.includes('past tense'));
    for (const ex of ['Searched in auth/', 'Fixed NPE in UserService', 'Created signup endpoint', 'Read config.json', 'Ran failing tests']) {
      assert.ok(TOOL_USE_SUMMARY_PROMPT.includes(ex), `missing example: ${ex}`);
    }
  });
});

describe('U020 truncateJsonForSummary', () => {
  it('passes a short value through unchanged', () => {
    assert.equal(truncateJsonForSummary({ a: 1 }), '{"a":1}');
  });
  it('truncates a long value with a trailing ellipsis to the cap', () => {
    const long = { s: 'x'.repeat(1000) };
    const out = truncateJsonForSummary(long, 20);
    assert.equal(out.length, 20);
    assert.ok(out.endsWith('...'));
  });
  it('uses the default IO cap', () => {
    const out = truncateJsonForSummary({ s: 'y'.repeat(5000) });
    assert.equal(out.length, TOOL_USE_SUMMARY_IO_MAX);
  });
  it('returns [unable to serialize] for a circular value', () => {
    const obj: Record<string, unknown> = {};
    obj.self = obj;
    assert.equal(truncateJsonForSummary(obj), '[unable to serialize]');
  });
  it('returns [unable to serialize] for a value that stringifies to undefined', () => {
    assert.equal(truncateJsonForSummary(undefined), '[unable to serialize]');
    assert.equal(truncateJsonForSummary(() => 1), '[unable to serialize]');
  });
});

describe('U020 buildToolUseSummaryPrompt', () => {
  it('emits one Tool/Input/Output block per completed call', () => {
    const p = buildToolUseSummaryPrompt(sampleTools);
    assert.ok(p.includes('Tool: bash'));
    assert.ok(p.includes('Input: {"command":"npm test"}'));
    assert.ok(p.includes('Output: "3 failing"'));
    assert.ok(p.includes('Tool: file_edit'));
    assert.ok(p.includes('Tools completed:'));
    assert.ok(p.trimEnd().endsWith('Label:'));
  });
  it('prepends the user-intent prefix (capped at 200 chars) when given assistant text', () => {
    const p = buildToolUseSummaryPrompt(sampleTools, 'Fix the failing user-service tests');
    assert.ok(p.startsWith("User's intent (from assistant's last message): Fix the failing user-service tests"));
    const longIntent = 'z'.repeat(500);
    const p2 = buildToolUseSummaryPrompt(sampleTools, longIntent);
    assert.ok(p2.includes('z'.repeat(200)));
    assert.ok(!p2.includes('z'.repeat(201)));
  });
  it('omits the prefix when there is no assistant text', () => {
    assert.ok(!buildToolUseSummaryPrompt(sampleTools).includes("User's intent"));
    assert.ok(!buildToolUseSummaryPrompt(sampleTools, '   ').includes("User's intent"));
  });
});

describe('U020 generateToolUseSummaryLabel', () => {
  it('returns the sanitised label and reports a tool_use_summary success record', async () => {
    const results: AuxQueryResult[] = [];
    const calls = { count: 0 } as { provider?: ProviderConfig; messages?: unknown; count: number };
    const label = await generateToolUseSummaryLabel(
      sampleTools, provider, undefined, seam('Fixed failing user tests', calls), undefined,
      r => results.push(r), 'Fix the user-service tests',
    );
    assert.equal(label, 'Fixed failing user tests');
    assert.equal(calls.count, 1, 'exactly one model call');
    assert.equal(results.length, 1);
    assert.equal(results[0].type, 'success');
    assert.equal(results[0].source, 'tool_use_summary');
    if (results[0].type === 'success') {
      assert.equal(results[0].result, 'Fixed failing user tests');
      assert.equal(results[0].model, 'the-model');
    }
  });

  it('forces a deterministic (temperature 0, small maxTokens) aux provider + system/user split', async () => {
    const calls = { count: 0 } as { provider?: ProviderConfig; messages?: unknown; count: number };
    await generateToolUseSummaryLabel(sampleTools, provider, 'cheap-model', seam('Ran tests', calls));
    assert.equal(calls.provider?.temperature, 0);
    assert.equal(calls.provider?.maxTokens, 50);
    assert.equal(calls.provider?.model, 'cheap-model');
    const msgs = calls.messages as Array<{ role: string; content: string }>;
    assert.equal(msgs[0].role, 'system');
    assert.equal(msgs[0].content, TOOL_USE_SUMMARY_PROMPT);
    assert.equal(msgs[1].role, 'user');
    assert.ok(msgs[1].content.includes('Tool: bash'));
  });

  it('short-circuits an empty batch with no model call (parity: tools.length===0 → null)', async () => {
    const calls = { count: 0 } as { provider?: ProviderConfig; messages?: unknown; count: number };
    const results: AuxQueryResult[] = [];
    const label = await generateToolUseSummaryLabel([], provider, undefined, seam('nope', calls), undefined, r => results.push(r));
    assert.equal(label, '');
    assert.equal(calls.count, 0, 'no query for an empty batch');
    assert.equal(results.length, 0, 'no aux result reported for an empty batch');
  });

  it('strips <think> blocks and surrounding quotes from the label', async () => {
    const label = await generateToolUseSummaryLabel(
      sampleTools, provider, undefined, seam('<think>deciding</think>"Created signup endpoint"'),
    );
    assert.equal(label, 'Created signup endpoint');
  });

  it('truncates an over-long label to the label cap with an ellipsis affordance', async () => {
    const verbose = 'Refactored the entire authentication subsystem end to end';
    const label = await generateToolUseSummaryLabel(sampleTools, provider, undefined, seam(verbose));
    assert.ok(label.length <= TOOL_USE_SUMMARY_LABEL_MAX, `len ${label.length} <= ${TOOL_USE_SUMMARY_LABEL_MAX}`);
    assert.ok(label.endsWith('…'), 'clipped label carries the ellipsis affordance');
  });

  it('returns empty + an error record when the stream fails (non-critical)', async () => {
    const results: AuxQueryResult[] = [];
    const label = await generateToolUseSummaryLabel(
      sampleTools, provider, undefined, errorSeam('boom'), undefined, r => results.push(r),
    );
    assert.equal(label, '');
    assert.equal(results.length, 1);
    assert.equal(results[0].type, 'error');
    assert.equal(results[0].source, 'tool_use_summary');
  });

  it('returns empty + an unparseable error record for pure-noise output', async () => {
    const results: AuxQueryResult[] = [];
    const label = await generateToolUseSummaryLabel(
      sampleTools, provider, undefined, seam('***'), undefined, r => results.push(r),
    );
    assert.equal(label, '');
    assert.equal(results[0].type, 'error');
    if (results[0].type === 'error') assert.equal(results[0].reason, 'unparseable');
  });
});
