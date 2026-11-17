import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  TOOL_RESULT_STATES,
  classifyToolResultState,
  isUnsuccessfulToolResult,
  TOOL_CALL_DISPLAY_STATES,
  deriveToolCallDisplayState,
  systemMessageDisplay,
  REDACTED_THINKING_PLACEHOLDER,
  isRedactedThinkingBlock,
  thinkingDisplayText,
} from '../src/message-display';
import { SYNTHETIC_TOOL_RESULT_PLACEHOLDER } from '../src/provider-transform';
import type { ToolResult } from '../src/types';

describe('message-display: classifyToolResultState', () => {
  it('discriminates all five mutually-exclusive states', () => {
    assert.equal(classifyToolResultState({ output: 'ok' }), 'success');
    assert.equal(classifyToolResultState({ output: 'boom', isError: true }), 'error');
    assert.equal(classifyToolResultState({ output: '', canceled: true }), 'canceled');
    assert.equal(classifyToolResultState({ output: '', rejected: true }), 'rejected');
    assert.equal(classifyToolResultState({ output: '', interrupted: true }), 'interrupted');
  });

  it('infers interrupted from the synthetic interrupt placeholder content', () => {
    const r: ToolResult = { output: SYNTHETIC_TOOL_RESULT_PLACEHOLDER };
    assert.equal(classifyToolResultState(r), 'interrupted');
  });

  it('applies precedence canceled > rejected > interrupted > error', () => {
    assert.equal(
      classifyToolResultState({ output: '', canceled: true, rejected: true, interrupted: true, isError: true }),
      'canceled',
    );
    assert.equal(
      classifyToolResultState({ output: '', rejected: true, interrupted: true, isError: true }),
      'rejected',
    );
    assert.equal(classifyToolResultState({ output: '', interrupted: true, isError: true }), 'interrupted');
  });

  it('every state is in TOOL_RESULT_STATES and isUnsuccessfulToolResult is only false for success', () => {
    for (const s of TOOL_RESULT_STATES) {
      assert.equal(isUnsuccessfulToolResult(s), s !== 'success');
    }
    assert.ok(TOOL_RESULT_STATES.includes('success'));
  });
});

describe('message-display: deriveToolCallDisplayState', () => {
  const inProgress = new Set(['run-1']);
  const resolved = new Set(['done-1']);

  it('returns queued when the id is in neither set', () => {
    assert.equal(deriveToolCallDisplayState('new-1', inProgress, resolved), 'queued');
  });
  it('returns running when only in the in-progress set', () => {
    assert.equal(deriveToolCallDisplayState('run-1', inProgress, resolved), 'running');
  });
  it('returns resolved when in the resolved set', () => {
    assert.equal(deriveToolCallDisplayState('done-1', inProgress, resolved), 'resolved');
  });
  it('resolved takes precedence over a stale in-progress entry', () => {
    const both = new Set(['x']);
    assert.equal(deriveToolCallDisplayState('x', both, new Set(['x'])), 'resolved');
  });
  it('TOOL_CALL_DISPLAY_STATES enumerates the three states', () => {
    assert.deepEqual([...TOOL_CALL_DISPLAY_STATES], ['queued', 'running', 'resolved']);
  });
});

describe('message-display: systemMessageDisplay', () => {
  it('suppresses info messages unless verbose or stop-hook summary', () => {
    assert.equal(systemMessageDisplay('info').suppressed, true);
    assert.equal(systemMessageDisplay('info', { verbose: true }).suppressed, false);
    assert.equal(systemMessageDisplay('info', { isStopHookSummary: true }).suppressed, false);
  });
  it('treats an absent level as info', () => {
    const d = systemMessageDisplay(undefined);
    assert.equal(d.suppressed, true);
    assert.equal(d.dimColor, true);
    assert.equal(d.showDot, false);
  });
  it('warning: dot shown, color warning, not dimmed, never suppressed', () => {
    const d = systemMessageDisplay('warning');
    assert.deepEqual(d, { suppressed: false, showDot: true, color: 'warning', dimColor: false });
  });
  it('error: dot shown, no color, not dimmed, never suppressed', () => {
    const d = systemMessageDisplay('error');
    assert.deepEqual(d, { suppressed: false, showDot: true, color: undefined, dimColor: false });
  });
});

describe('message-display: redacted thinking', () => {
  it('detects a redacted_thinking content block', () => {
    assert.equal(isRedactedThinkingBlock({ type: 'redacted_thinking', data: 'xxx' }), true);
    assert.equal(isRedactedThinkingBlock({ type: 'thinking', thinking: 'hi' }), false);
    assert.equal(isRedactedThinkingBlock(null), false);
    assert.equal(isRedactedThinkingBlock('redacted_thinking'), false);
    assert.equal(isRedactedThinkingBlock(undefined), false);
  });
  it('thinkingDisplayText uses the placeholder for redacted blocks', () => {
    assert.equal(thinkingDisplayText({ type: 'redacted_thinking' }), REDACTED_THINKING_PLACEHOLDER);
    assert.equal(REDACTED_THINKING_PLACEHOLDER, '✻ Thinking…');
  });
  it('thinkingDisplayText returns trimmed plaintext or undefined when empty', () => {
    assert.equal(thinkingDisplayText({ type: 'thinking' }, '  reasoning  '), 'reasoning');
    assert.equal(thinkingDisplayText({ type: 'thinking' }, '   '), undefined);
    assert.equal(thinkingDisplayText({ type: 'thinking' }), undefined);
  });
});
