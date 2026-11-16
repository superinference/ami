import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  previewCommand,
  MAX_COMMAND_DISPLAY_LINES,
  MAX_COMMAND_DISPLAY_CHARS,
} from '../src/tools/command-preview';

describe('previewCommand (task command truncation parity)', () => {
  it('exposes Claude Code\'s display caps (2 lines / 160 chars)', () => {
    assert.equal(MAX_COMMAND_DISPLAY_LINES, 2);
    assert.equal(MAX_COMMAND_DISPLAY_CHARS, 160);
  });

  it('returns a short command unchanged and untruncated', () => {
    const r = previewCommand('sleep 30');
    assert.equal(r.text, 'sleep 30');
    assert.equal(r.truncated, false);
  });

  it('caps at the first N lines when more lines are present', () => {
    const r = previewCommand('one\ntwo\nthree\nfour');
    assert.equal(r.text, 'one\ntwo');
    assert.equal(r.truncated, true);
  });

  it('keeps exactly the max number of lines without truncating', () => {
    const r = previewCommand('one\ntwo');
    assert.equal(r.text, 'one\ntwo');
    assert.equal(r.truncated, false);
  });

  it('caps at the max character count', () => {
    const long = 'a'.repeat(200);
    const r = previewCommand(long);
    assert.equal(r.text.length, MAX_COMMAND_DISPLAY_CHARS);
    assert.equal(r.truncated, true);
  });

  it('a command exactly at the char cap is not truncated', () => {
    const exact = 'b'.repeat(MAX_COMMAND_DISPLAY_CHARS);
    const r = previewCommand(exact);
    assert.equal(r.text.length, MAX_COMMAND_DISPLAY_CHARS);
    assert.equal(r.truncated, false);
  });

  it('trims surrounding whitespace in the preview', () => {
    const r = previewCommand('  echo hi  \n');
    assert.equal(r.text, 'echo hi');
  });

  it('honors custom caps', () => {
    const r = previewCommand('abcdef', { maxChars: 3 });
    assert.equal(r.text, 'abc');
    assert.equal(r.truncated, true);
    const r2 = previewCommand('a\nb\nc', { maxLines: 1 });
    assert.equal(r2.text, 'a');
    assert.equal(r2.truncated, true);
  });

  it('reports truncation when both lines and chars are over the cap', () => {
    const r = previewCommand('x'.repeat(300) + '\n' + 'y'.repeat(300) + '\n' + 'z');
    assert.equal(r.truncated, true);
    assert.equal(r.text.length, MAX_COMMAND_DISPLAY_CHARS);
  });

  it('an empty command previews to empty, not truncated', () => {
    const r = previewCommand('');
    assert.equal(r.text, '');
    assert.equal(r.truncated, false);
  });
});
