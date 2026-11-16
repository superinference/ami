import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateAskUserQuestionInput,
  ASK_USER_MAX_HEADER,
  ASK_USER_PREVIEW_MULTISELECT_MESSAGE,
} from '../src/tools/ask-user';

// ---------------------------------------------------------------------------
// U078 — additive validator parity:
//   - header/chip width (<= 12, parity with Claude's ASK_USER_QUESTION_TOOL_CHIP_WIDTH)
//   - previews only on single-select questions (not multiSelect)
// ---------------------------------------------------------------------------

const okOptions = [
  { label: 'A', description: 'a' },
  { label: 'B', description: 'b' },
];

describe('validateAskUserQuestionInput — header/chip width', () => {
  it('accepts a header at the limit', () => {
    assert.equal(ASK_USER_MAX_HEADER, 12);
    const err = validateAskUserQuestionInput({
      questions: [{ question: 'Q?', header: 'x'.repeat(ASK_USER_MAX_HEADER), options: okOptions }],
    });
    assert.equal(err, null);
  });

  it('rejects a header over the limit (multi-question shape)', () => {
    const err = validateAskUserQuestionInput({
      questions: [{ question: 'Q?', header: 'x'.repeat(ASK_USER_MAX_HEADER + 1), options: okOptions }],
    });
    assert.ok(err !== null);
    assert.match(err!, /header must be <= 12 chars/);
    assert.match(err!, /got 13/);
  });

  it('rejects a header over the limit (single-question shorthand)', () => {
    const err = validateAskUserQuestionInput({
      question: 'Q?',
      header: 'this header is way too long',
      options: okOptions,
    });
    assert.ok(err !== null);
    assert.match(err!, /header must be <= 12 chars/);
  });

  it('accepts an absent header', () => {
    assert.equal(validateAskUserQuestionInput({ question: 'Q?', options: okOptions }), null);
  });
});

describe('validateAskUserQuestionInput — preview single-select-only', () => {
  it('rejects an option preview when multiSelect is true', () => {
    const err = validateAskUserQuestionInput({
      questions: [{
        question: 'Q?',
        multiSelect: true,
        options: [
          { label: 'A', description: 'a', preview: '<div>x</div>' },
          { label: 'B', description: 'b' },
        ],
      }],
    });
    assert.equal(err, ASK_USER_PREVIEW_MULTISELECT_MESSAGE);
  });

  it('rejects a previewFormat when multiSelect is true (even without option previews)', () => {
    const err = validateAskUserQuestionInput({
      questions: [{ question: 'Q?', multiSelect: true, previewFormat: 'markdown', options: okOptions }],
    });
    assert.equal(err, ASK_USER_PREVIEW_MULTISELECT_MESSAGE);
  });

  it('rejects preview+multiSelect via the single-question shorthand', () => {
    const err = validateAskUserQuestionInput({
      question: 'Q?',
      multiSelect: true,
      previewFormat: 'html',
      options: okOptions,
    });
    assert.equal(err, ASK_USER_PREVIEW_MULTISELECT_MESSAGE);
  });

  it('accepts a preview on a single-select question', () => {
    const err = validateAskUserQuestionInput({
      questions: [{
        question: 'Q?',
        multiSelect: false,
        previewFormat: 'markdown',
        options: [
          { label: 'A', description: 'a', preview: 'preview text' },
          { label: 'B', description: 'b' },
        ],
      }],
    });
    assert.equal(err, null);
  });

  it('accepts multiSelect with no preview at all', () => {
    const err = validateAskUserQuestionInput({
      questions: [{ question: 'Q?', multiSelect: true, options: okOptions }],
    });
    assert.equal(err, null);
  });
});
