import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  classifyToolResultState,
  isUnsuccessfulToolResult,
  TOOL_RESULT_STATES,
} from '../src/message-display';
import type { ToolResult } from '../src/types';

// ===========================================================================
// U006-w84 — ToolResult `behavior: 'ask'` recoverable-validation render state.
//
// Parity with Claude Code's FileEditTool `behavior: 'ask' | 'error'` split: a
// recoverable validation failure (fix-and-retry) is classified distinctly from a
// fatal error so a frontend/transcript can present an "ask and retry" affordance
// instead of a hard stop. classifyToolResultState gains an `ask` state that wins
// over a plain `isError` but loses to the cancel/reject/interrupt markers.
// ===========================================================================

describe('U006-w84 — classifyToolResultState: behavior:ask recoverable state', () => {
  it('registers `ask` as a known tool-result state', () => {
    assert.ok(TOOL_RESULT_STATES.includes('ask'));
  });

  it('classifies a behavior:ask failure as `ask`, not `error`', () => {
    const r: ToolResult = { output: 'Error: old_string not found', isError: true, behavior: 'ask' };
    assert.equal(classifyToolResultState(r), 'ask');
  });

  it('still treats `ask` as an unsuccessful (retry-needed) outcome', () => {
    assert.equal(isUnsuccessfulToolResult('ask'), true);
  });

  it('a plain isError result with no behavior stays `error`', () => {
    const r: ToolResult = { output: 'Error: fatal', isError: true };
    assert.equal(classifyToolResultState(r), 'error');
  });

  it('behavior:error is treated as a hard error, not ask', () => {
    const r: ToolResult = { output: 'Error: fatal', isError: true, behavior: 'error' };
    assert.equal(classifyToolResultState(r), 'error');
  });

  it('cancel/reject/interrupt markers still win over ask', () => {
    assert.equal(
      classifyToolResultState({ output: 'x', isError: true, behavior: 'ask', canceled: true }),
      'canceled',
    );
    assert.equal(
      classifyToolResultState({ output: 'x', isError: true, behavior: 'ask', rejected: true }),
      'rejected',
    );
    assert.equal(
      classifyToolResultState({ output: 'x', isError: true, behavior: 'ask', interrupted: true }),
      'interrupted',
    );
  });

  it('a successful result is unaffected (ask is opt-in)', () => {
    assert.equal(classifyToolResultState({ output: 'ok' }), 'success');
  });
});
