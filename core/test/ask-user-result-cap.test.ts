import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  askUserQuestionTool,
  capAskUserResult,
  ASK_USER_MAX_RESULT_CHARS,
} from '../src/tools/ask-user';

// ---------------------------------------------------------------------------
// U078 — result-size cap parity (the reference's maxResultSizeChars: 100_000 on the
// AskUserQuestion tool). Bounds how much user/model-derived answer+annotation
// text flows back into the model context.
// ---------------------------------------------------------------------------

describe('AskUserQuestion result-size cap', () => {
  it('advertises maxResultSizeChars on the tool definition', () => {
    assert.equal(askUserQuestionTool.maxResultSizeChars, 100_000);
    assert.equal(ASK_USER_MAX_RESULT_CHARS, 100_000);
  });

  it('returns short output unchanged', () => {
    assert.equal(capAskUserResult('User answered: A'), 'User answered: A');
  });

  it('returns output exactly at the cap unchanged', () => {
    const atCap = 'y'.repeat(ASK_USER_MAX_RESULT_CHARS);
    assert.equal(capAskUserResult(atCap), atCap);
  });

  it('truncates oversized output to within the cap with a marker', () => {
    const over = 'z'.repeat(ASK_USER_MAX_RESULT_CHARS + 1234);
    const capped = capAskUserResult(over);
    assert.ok(capped.length <= ASK_USER_MAX_RESULT_CHARS);
    assert.match(capped, /truncated \d+ chars over the 100000-char AskUserQuestion result limit/);
  });

  it('honors a custom cap argument', () => {
    assert.equal(capAskUserResult('abcdef', 100), 'abcdef');
    const capped = capAskUserResult('abcdef'.repeat(100), 20);
    assert.ok(capped.length <= 20);
  });

  it('treats a non-positive cap as unbounded', () => {
    const s = 'hello world';
    assert.equal(capAskUserResult(s, 0), s);
    assert.equal(capAskUserResult(s, -5), s);
  });
});
