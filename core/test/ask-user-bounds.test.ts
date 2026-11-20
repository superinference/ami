import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateAskUserQuestionInput,
  ASK_USER_MAX_QUESTIONS,
  ASK_USER_MIN_OPTIONS,
  ASK_USER_MAX_OPTIONS,
} from '../src/tools/ask-user';

// ---------------------------------------------------------------------------
// validateAskUserQuestionInput — cardinality bounds (parity with the reference's zod
// `.min(1).max(4)` on questions and `.min(2).max(4)` on options). JSON-Schema
// minItems/maxItems are advisory, so the validator fail-closes on bad bounds.
// ---------------------------------------------------------------------------

const okOptions = [
  { label: 'A', description: 'a' },
  { label: 'B', description: 'b' },
];

describe('validateAskUserQuestionInput — cardinality bounds', () => {
  it('exposes the reference-parity bound constants', () => {
    assert.equal(ASK_USER_MAX_QUESTIONS, 4);
    assert.equal(ASK_USER_MIN_OPTIONS, 2);
    assert.equal(ASK_USER_MAX_OPTIONS, 4);
  });

  it('accepts exactly 4 questions', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        { question: 'Q1?', options: okOptions },
        { question: 'Q2?', options: okOptions },
        { question: 'Q3?', options: okOptions },
        { question: 'Q4?', options: okOptions },
      ],
    });
    assert.equal(res, null);
  });

  it('rejects more than 4 questions with a model-facing message', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        { question: 'Q1?', options: okOptions },
        { question: 'Q2?', options: okOptions },
        { question: 'Q3?', options: okOptions },
        { question: 'Q4?', options: okOptions },
        { question: 'Q5?', options: okOptions },
      ],
    });
    assert.ok(res);
    assert.match(res!, /1-4 questions/);
    assert.match(res!, /got 5/);
  });

  it('rejects a question with fewer than 2 options', () => {
    const res = validateAskUserQuestionInput({
      questions: [{ question: 'Pick?', options: [{ label: 'Only', description: 'one' }] }],
    });
    assert.ok(res);
    assert.match(res!, /"Pick\?"/);
    assert.match(res!, /2-4 options/);
    assert.match(res!, /got 1/);
  });

  it('rejects a question with more than 4 options', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        {
          question: 'Pick?',
          options: [
            { label: 'A', description: 'a' },
            { label: 'B', description: 'b' },
            { label: 'C', description: 'c' },
            { label: 'D', description: 'd' },
            { label: 'E', description: 'e' },
          ],
        },
      ],
    });
    assert.ok(res);
    assert.match(res!, /2-4 options/);
    assert.match(res!, /got 5/);
  });

  it('rejects over-bound options in the single-question shorthand', () => {
    const res = validateAskUserQuestionInput({
      question: 'Pick?',
      options: [{ label: 'Only', description: 'one' }],
    });
    assert.ok(res);
    assert.match(res!, /2-4 options/);
  });

  it('accepts 2, 3 and 4 options', () => {
    for (const n of [2, 3, 4]) {
      const opts = Array.from({ length: n }, (_v, i) => ({ label: `L${i}`, description: `d${i}` }));
      assert.equal(
        validateAskUserQuestionInput({ questions: [{ question: 'Q?', options: opts }] }),
        null,
      );
    }
  });

  it('does NOT enforce bounds when options are omitted (engine default kicks in)', () => {
    // Shorthand with only a question — engine supplies default Continue/Cancel.
    assert.equal(validateAskUserQuestionInput({ question: 'Proceed?' }), null);
    // Multi-question with a question lacking options.
    assert.equal(
      validateAskUserQuestionInput({ questions: [{ question: 'Proceed?' }] }),
      null,
    );
  });
});
