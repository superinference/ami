import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  createAuxiliaryQuery,
  runAuxQuery,
  buildAuxResult,
} from '../src/title-generator';
import type { AuxQueryResult } from '../src/title-generator';
import type { ProviderConfig } from '../src/types';
import type { streamChatCompletion } from '../src/provider';

// ---------------------------------------------------------------------------
// U034 — generic declarative auxiliary-query factory (parity: Claude Code's
// createApiQueryHook<TResult> + ApiQueryHookConfig, utils/hooks/apiQueryHookHelper.ts).
// A single reusable factory drives ANY non-streaming side-query from a data
// structure (source tag, parseResponse, resultText, timeoutMs) over the shared
// runAuxQuery primitive, emitting the uniform AuxQueryResult introspection record.
// Also covers runAuxQuery's new overridable timeoutMs (untimed when <= 0).
// ---------------------------------------------------------------------------

const provider: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'k',
  model: 'the-model',
  maxTokens: 50,
};

/** Streamer that emits `text` as one content_delta then a `done` chunk. */
function seam(text: string, headers?: Record<string, string>): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* () {
    if (text) yield { type: 'content_delta', text } as any;
    yield { type: 'done', responseHeaders: headers } as any;
  }) as unknown as typeof streamChatCompletion;
}

/** Streamer that yields an error chunk (simulating a provider/stream failure). */
function errorSeam(errText: string): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* () {
    yield { type: 'error', error: errText } as any;
  }) as unknown as typeof streamChatCompletion;
}

/** Abort-aware delay used by the slow streamer below. */
function abortableDelay(ms: number, sig: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    const t = setTimeout(resolve, ms);
    if (sig) {
      if (sig.aborted) { clearTimeout(t); resolve(); return; }
      sig.addEventListener('abort', () => { clearTimeout(t); resolve(); }, { once: true });
    }
  });
}

/** Streamer that waits `delayMs` (respecting abort) before emitting its output. */
function slowSeam(delayMs: number, text: string): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* (_p: any, _m: any, _t: any, sig: AbortSignal) {
    await abortableDelay(delayMs, sig);
    if (sig?.aborted) throw new Error('aborted');
    yield { type: 'content_delta', text } as any;
    yield { type: 'done' } as any;
  }) as unknown as typeof streamChatCompletion;
}

describe('U034 buildAuxResult', () => {
  it('builds a success record when output is usable', () => {
    const r = buildAuxResult('compaction_summary', 'm', 'mid-1', undefined, 'ok');
    assert.equal(r.type, 'success');
    assert.equal(r.source, 'compaction_summary');
    assert.equal(r.model, 'm');
    if (r.type === 'success') {
      assert.equal(r.result, 'ok');
      assert.equal(r.messageId, 'mid-1');
      assert.ok(r.uuid);
    }
  });

  it('builds an unparseable error when the clean output is empty', () => {
    const r = buildAuxResult('compaction_summary', 'm', undefined, undefined, '');
    assert.equal(r.type, 'error');
    if (r.type === 'error') assert.equal(r.reason, 'unparseable');
  });

  it('propagates an outer failure reason/detail', () => {
    const r = buildAuxResult('session_title', 'm', undefined, { reason: 'stream_error', error: 'boom' }, '');
    assert.equal(r.type, 'error');
    if (r.type === 'error') {
      assert.equal(r.reason, 'stream_error');
      assert.equal(r.error, 'boom');
    }
  });
});

describe('U034 createAuxiliaryQuery', () => {
  it('runs a success query, parses, and emits a success record to onResult', async () => {
    const seen: AuxQueryResult[] = [];
    const run = createAuxiliaryQuery<number>({
      source: 'compaction_summary',
      parseResponse: (raw) => (raw.trim() ? raw.trim().length : undefined),
      resultText: (n) => `len=${n}`,
    });
    const { outcome, parsed, result } = await run(provider, [], seam('hello'), undefined, (r) => seen.push(r));
    assert.equal(outcome.failure, undefined);
    assert.equal(parsed, 'hello'.length);
    assert.equal(result.type, 'success');
    if (result.type === 'success') assert.equal(result.result, `len=${'hello'.length}`);
    assert.equal(seen.length, 1);
    assert.equal(seen[0].source, 'compaction_summary');
  });

  it('classifies a stream error as an outer failure (parsed undefined)', async () => {
    const seen: AuxQueryResult[] = [];
    const run = createAuxiliaryQuery<string>({
      source: 'compaction_summary',
      parseResponse: (raw) => raw || undefined,
    });
    const { outcome, parsed, result } = await run(provider, [], errorSeam('overloaded'), undefined, (r) => seen.push(r));
    assert.equal(parsed, undefined);
    assert.equal(outcome.failure?.reason, 'stream_error');
    assert.equal(result.type, 'error');
    if (result.type === 'error') {
      assert.equal(result.reason, 'stream_error');
      assert.equal(result.error, 'overloaded');
    }
    assert.equal(seen.length, 1);
  });

  it('reports a clean-but-unusable response as the two-tier unparseable inner error', async () => {
    const run = createAuxiliaryQuery<string>({
      source: 'compaction_summary',
      parseResponse: (raw) => raw || undefined,
    });
    const { parsed, result } = await run(provider, [], seam(''));
    assert.equal(parsed, undefined);
    assert.equal(result.type, 'error');
    if (result.type === 'error') assert.equal(result.reason, 'unparseable');
  });

  it('does not throw when the onResult sink throws (fire-and-forget)', async () => {
    const run = createAuxiliaryQuery<string>({
      source: 'compaction_summary',
      parseResponse: (raw) => raw || undefined,
    });
    await assert.doesNotReject(() =>
      run(provider, [], seam('x'), undefined, () => { throw new Error('sink blew up'); }),
    );
  });
});

describe('U034 runAuxQuery timeoutMs override', () => {
  it('times out a slow query when a short finite cap is given', async () => {
    const out = await runAuxQuery(provider, [], slowSeam(200, 'late'), undefined, 10);
    assert.equal(out.failure?.reason, 'timeout');
  });

  it('runs UNTIMED when timeoutMs is 0 (no premature abort)', async () => {
    const out = await runAuxQuery(provider, [], slowSeam(30, 'late'), undefined, 0);
    assert.equal(out.failure, undefined);
    assert.equal(out.raw, 'late');
  });
});
