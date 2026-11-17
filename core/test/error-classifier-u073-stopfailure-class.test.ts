/**
 * U073 — StopFailure error-class taxonomy (error-classifier.ts + hooks.ts wiring).
 *
 * Parity gap: Claude Code's StopFailure hook is matched on an enumerated error
 * taxonomy (rate_limit / authentication_failed / billing_error / invalid_request /
 * server_error / max_output_tokens / unknown). AMI advertised those exact matcher
 * values but scoped the StopFailure hook on the RAW error message, and the sole
 * internal emitter (a Stop hook that itself errors) passed that raw message as the
 * `error` field — so a hook configured with one of the advertised classes never
 * matched. `stopFailureErrorClass()` folds any classified error onto the taxonomy,
 * and the emitter now classifies before firing.
 *
 * RED (pre-fix) discriminators:
 *  - `stopFailureErrorClass` / `STOP_FAILURE_ERROR_CLASSES` did not exist (import
 *    throws) and `StopFailureContext.error` was a free string.
 *  - executeStop passed the raw thrown message as `error`, so the matcher-scoped
 *    StopFailure hook did not fire (marker never created).
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  stopFailureErrorClass,
  STOP_FAILURE_ERROR_CLASSES,
  type StopFailureErrorClass,
} from '../src/error-classifier';
import { HookManager, getHookEventMetadata, type StopFailureContext } from '../src/hooks';

describe('STOP_FAILURE_ERROR_CLASSES — mirrors Claude Code taxonomy', () => {
  it('is the exact reference set', () => {
    assert.deepEqual([...STOP_FAILURE_ERROR_CLASSES], [
      'rate_limit', 'authentication_failed', 'billing_error', 'invalid_request',
      'server_error', 'max_output_tokens', 'unknown',
    ]);
  });

  it('is the single source for the hook catalog matcher values', () => {
    const meta = getHookEventMetadata('stopFailure');
    assert.deepEqual(meta!.matcherValues, [...STOP_FAILURE_ERROR_CLASSES]);
  });
});

describe('stopFailureErrorClass — maps every error onto the taxonomy', () => {
  const cases: Array<[string, StopFailureErrorClass]> = [
    ['HTTP 429 - rate limit exceeded, too many requests', 'rate_limit'],
    ['HTTP 401 - invalid api key / authentication failed', 'authentication_failed'],
    ['Your credit balance is too low — billing limit reached', 'billing_error'],
    ['prompt is too long: 210000 tokens > 200000 maximum', 'invalid_request'],
    ['output too large — max_output exceeded', 'max_output_tokens'],
    ['HTTP 500 - internal server error', 'server_error'],
    ['Overloaded — model temporarily unavailable', 'server_error'],
    ['ECONNRESET: network socket hang up', 'server_error'],
    ['something totally unexpected happened', 'unknown'],
    ['boom', 'unknown'],
  ];
  for (const [input, expected] of cases) {
    it(`"${input.slice(0, 32)}…" -> ${expected}`, () => {
      assert.equal(stopFailureErrorClass(input), expected);
      // Only the enumerated classes are ever produced.
      assert.ok((STOP_FAILURE_ERROR_CLASSES as readonly string[]).includes(stopFailureErrorClass(input)));
    });
  }

  it('classifies an Error object (not just a string)', () => {
    assert.equal(stopFailureErrorClass(new Error('429 Too Many Requests')), 'rate_limit');
  });
});

// ---------------------------------------------------------------------------
// E2E — the real emitter path: a Stop hook that throws triggers a StopFailure
// whose matcher scopes on the classified failure class.
// ---------------------------------------------------------------------------

function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u073-sf-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}

function makeManager(dir: string): HookManager {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  return hm;
}

describe('StopFailure fires matched by the CLASSIFIED error class (E2E)', () => {
  it('a rate-limit stop-hook failure matches a `rate_limit` StopFailure matcher', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'stopFailure', matcher: 'rate_limit', hook: { type: 'command', command: `touch rl.txt` } },
      { event: 'stopFailure', matcher: 'authentication_failed', hook: { type: 'command', command: `touch auth.txt` } },
    ]);
    const hm = makeManager(dir);
    // A Stop hook that throws a rate-limit-shaped error drives the real emitter.
    hm.onStop(async () => { throw new Error('HTTP 429 - rate limit exceeded'); });

    await hm.executeStop({ messages: [] } as never);

    // RED without the fix: the raw 'HTTP 429 …' string was passed as `error`, so
    // neither matcher (rate_limit / authentication_failed) matched -> no marker.
    assert.equal(fs.existsSync(path.join(dir, 'rl.txt')), true, 'rate_limit matcher must fire');
    assert.equal(fs.existsSync(path.join(dir, 'auth.txt')), false, 'non-matching class must NOT fire');
  });

  it('executeStopFailure carries the raw detail in errorMessage while matching on the class', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'stopFailure', matcher: 'server_error', hook: { type: 'command', command: `touch srv.txt` } },
    ]);
    const hm = makeManager(dir);
    const ctx: StopFailureContext = { error: 'server_error', errorMessage: 'HTTP 500 internal', hookIndex: 0 };
    await hm.executeStopFailure(ctx);
    assert.equal(fs.existsSync(path.join(dir, 'srv.txt')), true);
  });
});
