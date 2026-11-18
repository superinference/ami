/**
 * U007-w107 — core unit coverage for the Stop-hook blocking-feedback formatter.
 *
 * Claude Code frames a Stop hook's blocking error with per-event wording before
 * re-injecting it into the model's context (`getStopHookMessage`,
 * utils/hooks.ts:L1894-L1896 → `Stop hook feedback:\n<error>`). The engine
 * (common/src/engine.ts) uses this helper so the model can attribute the
 * re-injected feedback to a Stop guard. Pure; the error body is verbatim.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { formatStopHookBlockingMessage } from '../src/hooks';

describe('U007-w107: formatStopHookBlockingMessage', () => {
  it('frames a blocking error with Claude Code getStopHookMessage wording', () => {
    assert.equal(
      formatStopHookBlockingMessage('tests are failing'),
      'Stop hook feedback:\ntests are failing',
    );
  });

  it('preserves the error body verbatim (no trimming)', () => {
    assert.equal(
      formatStopHookBlockingMessage('  spaced  '),
      'Stop hook feedback:\n  spaced  ',
    );
  });

  it('handles an empty error body', () => {
    assert.equal(formatStopHookBlockingMessage(''), 'Stop hook feedback:\n');
  });
});
