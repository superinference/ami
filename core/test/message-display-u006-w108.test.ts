/**
 * U006 (w108): display discriminators added to message-display.ts —
 *  - shouldRenderToolResult: a tool can mark its own result non-rendering
 *    (parity: the reference's renderToolResultMessage returning null to suppress a
 *    redundant result covered by a batched lifecycle message).
 *  - summarizeToolInputFields: compact tool-use field summary
 *    (parity: SyntheticOutputTool.renderToolUseMessage — ≤3 keys inline,
 *    >3 keys collapsed to "N fields: a, b, c…").
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  shouldRenderToolResult,
  summarizeToolInputFields,
} from '../src/message-display';
import type { ToolResult } from '../src/types';

describe('shouldRenderToolResult', () => {
  it('renders a normal result (flag absent)', () => {
    assert.equal(shouldRenderToolResult({ output: 'done' }), true);
  });

  it('renders when suppressRender is explicitly false', () => {
    assert.equal(shouldRenderToolResult({ output: 'x', suppressRender: false }), true);
  });

  it('suppresses when suppressRender is true', () => {
    const r: ToolResult = { output: 'cleanup done', suppressRender: true };
    assert.equal(shouldRenderToolResult(r), false);
  });

  it('only true suppresses — a non-boolean/absent value still renders', () => {
    assert.equal(shouldRenderToolResult({ output: 'x', suppressRender: undefined }), true);
  });
});

describe('summarizeToolInputFields', () => {
  it('inlines up to three fields as "k: v, …"', () => {
    assert.equal(
      summarizeToolInputFields({ a: 1, b: 'two', c: true }),
      'a: 1, b: two, c: true',
    );
  });

  it('inlines a single field', () => {
    assert.equal(summarizeToolInputFields({ status: 'ok' }), 'status: ok');
  });

  it('collapses more than three fields to "N fields: a, b, c…"', () => {
    assert.equal(
      summarizeToolInputFields({ a: 1, b: 2, c: 3, d: 4, e: 5 }),
      '5 fields: a, b, c…',
    );
  });

  it('exactly three keys stays inline (boundary)', () => {
    assert.equal(summarizeToolInputFields({ x: 1, y: 2, z: 3 }), 'x: 1, y: 2, z: 3');
  });

  it('renders null / nested object / array values compactly', () => {
    assert.equal(
      summarizeToolInputFields({ n: null, o: { deep: 1 }, arr: [1, 2, 3] }),
      'n: null, o: {…}, arr: [3]',
    );
  });

  it('collapses newlines and truncates an over-long value', () => {
    const long = 'line one\nline two that keeps going and going and going for a while';
    const out = summarizeToolInputFields({ text: long });
    assert.ok(!out.includes('\n'), 'no raw newline in the one-line summary');
    assert.ok(out.endsWith('…'), 'over-long value is truncated with an ellipsis');
    assert.ok(out.startsWith('text: line one line two'));
  });

  it('honors a custom maxInlineKeys / maxValueChars', () => {
    assert.equal(
      summarizeToolInputFields({ a: 1, b: 2 }, { maxInlineKeys: 1 }),
      '2 fields: a, b…',
    );
    assert.equal(
      summarizeToolInputFields({ a: 'abcdef' }, { maxValueChars: 3 }),
      'a: ab…',
    );
  });

  it('returns empty string for an empty object', () => {
    assert.equal(summarizeToolInputFields({}), '');
  });

  it('returns empty string for non-object inputs (null, array, primitive)', () => {
    assert.equal(summarizeToolInputFields(null), '');
    assert.equal(summarizeToolInputFields([1, 2]), '');
    assert.equal(summarizeToolInputFields('str'), '');
    assert.equal(summarizeToolInputFields(42), '');
    assert.equal(summarizeToolInputFields(undefined), '');
  });
});
