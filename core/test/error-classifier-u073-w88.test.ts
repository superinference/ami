import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  classifyError,
  friendlyErrorMessage,
} from '../src/error-classifier';

// ---------------------------------------------------------------------------
// U073 / w88 — Magnitude-aware context-overflow classification for the
// "input length and `max_tokens` exceed context limit: A + B > C" form.
//
// Gap (confirmed): AMI already parsed the "N tokens > M maximum" prompt-too-long
// form into ClassifiedError.{actualTokens,limitTokens,tokenGap}, but the OTHER
// authoritative context-overflow wording the API returns —
//   "input length and `max_tokens` exceed context limit: 180000 + 8000 > 185000"
// — classified as context_overflow with NO magnitude fields, so the engine's
// recovery loop could not size compaction and the user-facing message could not
// show how far over the limit the request was. This populates the same three
// fields from the max_tokens-exceed form (actual = input + max_tokens,
// limit = context limit, gap = actual - limit) so BOTH overflow forms carry
// magnitude. Parity: the reference's magnitude-aware overflow handling
// (services/api/errors.ts parse helpers), extended to the max_tokens form.
// ---------------------------------------------------------------------------
describe('U073/w88 magnitude-aware max_tokens context overflow', () => {
  const RAW =
    'input length and `max_tokens` exceed context limit: 180000 + 8000 > 185000, decrease input length or max_tokens';

  it('populates actual/limit/gap for the max_tokens-exceed overflow form', () => {
    const c = classifyError(RAW);
    assert.equal(c.category, 'context_overflow');
    // actual = input (180000) + max_tokens (8000); limit = context limit (185000)
    assert.equal(c.actualTokens, 188000);
    assert.equal(c.limitTokens, 185000);
    assert.equal(c.tokenGap, 3000);
  });

  it('populates magnitude when the form arrives as an Error object too', () => {
    const c = classifyError(new Error('API Error: ' + RAW));
    assert.equal(c.category, 'context_overflow');
    assert.equal(c.actualTokens, 188000);
    assert.equal(c.limitTokens, 185000);
    assert.equal(c.tokenGap, 3000);
  });

  it('surfaces the exact overflow in the user-facing message', () => {
    const c = classifyError(RAW);
    const msg = friendlyErrorMessage(c);
    assert.match(msg, /188000 > 185000 tokens/);
  });

  it('still returns no magnitude when the request is NOT over the limit', () => {
    // A well-formed but non-overflowing count (sum <= limit) yields no positive gap.
    const c = classifyError(
      'input length and `max_tokens` exceed context limit: 100000 + 8000 > 185000',
    );
    assert.equal(c.category, 'context_overflow');
    // actual/limit are still parseable, but the gap is not positive → omitted.
    assert.equal(c.tokenGap, undefined);
    assert.equal(c.actualTokens, 108000);
    assert.equal(c.limitTokens, 185000);
  });

  it('leaves the "N tokens > M maximum" form unchanged (regression lock)', () => {
    const c = classifyError('prompt is too long: 137500 tokens > 135000 maximum');
    assert.equal(c.category, 'context_overflow');
    assert.equal(c.actualTokens, 137500);
    assert.equal(c.limitTokens, 135000);
    assert.equal(c.tokenGap, 2500);
  });

  it('carries no magnitude for a context overflow with no parseable counts', () => {
    const c = classifyError('This model\'s maximum context length was exceeded');
    assert.equal(c.category, 'context_overflow');
    assert.equal(c.actualTokens, undefined);
    assert.equal(c.limitTokens, undefined);
    assert.equal(c.tokenGap, undefined);
  });
});
