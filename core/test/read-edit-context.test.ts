import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  extractContextWindow,
  renderContextWindow,
  DEFAULT_EDIT_CONTEXT_LINES,
} from '../src/tools/read-edit-context';

const FILE = Array.from({ length: 10 }, (_, i) => `line ${i + 1}`).join('\n');

describe('extractContextWindow', () => {
  it('centers a ±N window on the given 1-based line with true line numbers', () => {
    const w = extractContextWindow(FILE, 5, 2);
    assert.deepEqual(w.lines.map(l => l.lineNumber), [3, 4, 5, 6, 7]);
    assert.equal(w.lines[0].text, 'line 3');
    assert.equal(w.lines[2].text, 'line 5');
    assert.equal(w.omittedBefore, 2);
    assert.equal(w.omittedAfter, 3);
    assert.equal(w.truncated, true);
  });

  it('clamps the window to the file start and marks no lines omitted above', () => {
    const w = extractContextWindow(FILE, 1, 3);
    assert.equal(w.lines[0].lineNumber, 1);
    assert.equal(w.omittedBefore, 0);
    assert.equal(w.lines[w.lines.length - 1].lineNumber, 4);
    assert.equal(w.omittedAfter, 6);
    assert.equal(w.truncated, true);
  });

  it('clamps the window to EOF and marks no lines omitted below', () => {
    const w = extractContextWindow(FILE, 10, 2);
    assert.equal(w.lines[w.lines.length - 1].lineNumber, 10);
    assert.equal(w.omittedAfter, 0);
    assert.equal(w.omittedBefore, 7);
  });

  it('clamps an out-of-range center into the valid line range', () => {
    const over = extractContextWindow(FILE, 999, 1);
    assert.equal(over.lines[over.lines.length - 1].lineNumber, 10);
    const under = extractContextWindow(FILE, -5, 1);
    assert.equal(under.lines[0].lineNumber, 1);
  });

  it('is not truncated when the window covers the whole file', () => {
    const w = extractContextWindow(FILE, 5, 100);
    assert.equal(w.lines.length, 10);
    assert.equal(w.truncated, false);
    assert.equal(w.omittedBefore, 0);
    assert.equal(w.omittedAfter, 0);
  });

  it('treats a negative contextLines as zero (single line window)', () => {
    const w = extractContextWindow(FILE, 4, -3);
    assert.deepEqual(w.lines.map(l => l.lineNumber), [4]);
  });

  it('returns an empty window for an empty file', () => {
    const w = extractContextWindow('', 1, 3);
    assert.deepEqual(w.lines, []);
    assert.equal(w.truncated, false);
    assert.equal(w.omittedBefore, 0);
    assert.equal(w.omittedAfter, 0);
  });

  it('uses DEFAULT_EDIT_CONTEXT_LINES when contextLines is omitted', () => {
    const w = extractContextWindow(FILE, 5);
    assert.equal(w.lines.length, DEFAULT_EDIT_CONTEXT_LINES * 2 + 1);
  });
});

describe('renderContextWindow', () => {
  it('renders numbered lines bracketed by singular/plural omission markers', () => {
    const w = extractContextWindow(FILE, 5, 2);
    const out = renderContextWindow(w);
    assert.match(out, /\.\.\. \(2 lines above\)/);
    assert.match(out, /\.\.\. \(3 lines below\)/);
    assert.match(out, /^3: line 3$/m);
    assert.match(out, /^7: line 7$/m);
  });

  it('uses the singular "line" form when exactly one line is omitted', () => {
    const w = extractContextWindow(FILE, 3, 1); // lines 2-4 → omittedBefore === 1
    const out = renderContextWindow(w);
    assert.match(out, /\.\.\. \(1 line above\)/);
    assert.ok(!out.includes('1 lines above'));
  });

  it('omits both markers when nothing is truncated', () => {
    const w = extractContextWindow(FILE, 5, 100);
    const out = renderContextWindow(w);
    assert.ok(!out.includes('above'));
    assert.ok(!out.includes('below'));
  });
});
