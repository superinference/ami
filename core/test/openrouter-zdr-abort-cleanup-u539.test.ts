// U539 (w115) — openrouter-zdr `fetchZdrModelIds` must adopt the shared
// `createCombinedAbortSignal({ timeoutMs })` helper and free its timer the instant
// the fetch settles (parity with the reference's combinedAbortSignal.ts eager
// setTimeout+clearTimeout pattern), instead of `AbortSignal.timeout(ms)` whose
// timer is retained for the FULL timeout window even after the request resolves.
//
// Discriminating red-green: with the OLD `AbortSignal.timeout(timeoutMs)` the
// signal passed to fetch stays armed and ABORTS at `timeoutMs` even though the
// fetch already resolved; with the helper + `cleanup()` in a `finally`, the timer
// is cleared on early completion so the captured signal NEVER aborts.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fetchZdrModelIds } from '../src/openrouter-zdr';

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

test('fetchZdrModelIds clears the timeout timer on early completion (no late abort)', async () => {
  let captured: AbortSignal | undefined;
  const fakeFetch = (async (_url: string, init: { signal?: AbortSignal }) => {
    captured = init?.signal;
    // Resolve immediately — far before the (tiny) timeout would fire.
    return { ok: true, json: async () => ({ data: [{ model_id: 'vendor/m' }] }) };
  }) as unknown as typeof fetch;

  const timeoutMs = 20;
  const set = await fetchZdrModelIds({ fetchImpl: fakeFetch, timeoutMs });
  assert.ok(set.has('vendor/m'), 'parsed the ZDR model id');
  assert.ok(captured instanceof AbortSignal, 'an AbortSignal is passed to fetch');
  assert.equal(captured!.aborted, false, 'signal is not aborted at completion');

  // Wait past the timeout window: with AbortSignal.timeout the native timer would
  // still abort the signal here; with cleanup() the timer was already cleared.
  await sleep(timeoutMs + 60);
  assert.equal(
    captured!.aborted,
    false,
    'the timeout timer was freed on early completion, so the signal never aborts late',
  );
});

test('fetchZdrModelIds still frees the timer when the fetch throws (finally path)', async () => {
  let captured: AbortSignal | undefined;
  const fakeFetch = (async (_url: string, init: { signal?: AbortSignal }) => {
    captured = init?.signal;
    throw new Error('network down');
  }) as unknown as typeof fetch;

  const timeoutMs = 20;
  const set = await fetchZdrModelIds({ fetchImpl: fakeFetch, timeoutMs });
  assert.equal(set.size, 0, 'network error yields an empty set, never throws');
  assert.ok(captured instanceof AbortSignal);

  await sleep(timeoutMs + 60);
  assert.equal(
    captured!.aborted,
    false,
    'the timeout timer is cleared on the error path too (cleanup in finally)',
  );
});
