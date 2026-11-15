import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  compactTestLog,
  foldToolOutputForPrompt,
  isTestRunnerCommand,
  looksLikeSuiteLog,
  parseTestSummary,
} from '../src/tools/test-log';

/**
 * Parity tests for test-log output handling. Each block targets one additive
 * parity fix and is written to FAIL if that fix is reverted (RED-GREEN
 * validated). None pass "both ways": the asserted values discriminate the new
 * behavior from the old.
 */
describe('test-log parity', () => {
  // [parity fix 1] Expanded runner recognition.
  it('recognizes JS/TS, Rust, Go, Elixir, Swift and E2E runners', () => {
    for (const cmd of [
      'npx jest',
      'vitest run',
      'bun test ./...',
      'deno test',
      'swift test',
      'flutter test',
      'mix test',
      'cypress run',
      'npx playwright test',
      'go test ./...',
      'pytest -q',
      'cargo test',
    ]) {
      assert.equal(isTestRunnerCommand(cmd), true, `expected runner: ${cmd}`);
    }
    // Non-runners stay unmatched; `ava` must not be found inside `java`.
    assert.equal(isTestRunnerCommand('java -jar build.jar'), false);
    assert.equal(isTestRunnerCommand('make build'), false);
    assert.equal(isTestRunnerCommand('echo hello'), false);
  });

  // [parity fix 2] Broadened failure markers (TAP `not ok`, traceback, glyphs).
  it('surfaces TAP and traceback failure lines from a long log', () => {
    const noise = 'x'.repeat(9_000);
    const log = `${noise}\nnot ok 1 - alpha check\nnot ok 2 - beta check\nnot ok 3 - gamma check\n`;
    const out = compactTestLog(log);
    // New markers pull the TAP failures out of a log the old marker set ignored.
    assert.match(out, /alpha check/);
    assert.match(out, /gamma check/);
    assert.equal(out.includes('x'.repeat(100)), false);

    const tb = `${noise}\nTraceback (most recent call last):\n  File "t.py", line 1\nException raised here\n✗ render\n`;
    const tbOut = compactTestLog(tb);
    assert.match(tbOut, /Traceback/);
    assert.match(tbOut, /Exception raised here/);
    assert.match(tbOut, /✗ render/);
  });

  // [parity fix 3] Skipped tests counted in the JSON suite summary.
  it('counts skipped tests in the go -json suite summary', () => {
    const lines = ['[Command: go test -json]'];
    for (let i = 0; i < 10; i++) lines.push(`{"Action":"pass","Test":"TestOk${i}"}`);
    for (let i = 0; i < 3; i++) lines.push(`{"Action":"skip","Test":"TestSkip${i}"}`);
    lines.push('{"Action":"output","Test":"TestBad","Output":"panic: boom"}');
    lines.push('{"Action":"fail","Test":"TestBad"}');
    const out = compactTestLog(lines.join('\n'));
    assert.match(out, /10 passed, 1 failed, 3 skipped/);
    // Skip events are summarized, never dumped into the body.
    assert.equal(out.includes('TestSkip0'), false);
  });

  // [parity fix 4] Failed count reported in the verbose (non-JSON) suite summary.
  it('reports a pass/fail breakdown for verbose --- PASS/FAIL logs', () => {
    const lines: string[] = [];
    for (let i = 0; i < 8; i++) lines.push(`--- PASS: TestOk${i} (0.00s)`);
    lines.push('--- FAIL: TestBad0 (0.01s)');
    lines.push('    bad_test.go:10: boom');
    lines.push('--- FAIL: TestBad1 (0.01s)');
    const out = compactTestLog(lines.join('\n'));
    assert.match(out, /Passed 8, Failed 2/);
  });

  // [parity fix 5] parseTestSummary + accurate header for summary-bearing logs.
  it('parses pytest/jest/mocha/cargo summary lines', () => {
    assert.deepEqual(parseTestSummary('===== 3 failed, 10 passed, 2 skipped in 1.23s ====='), {
      passed: 10,
      failed: 3,
      skipped: 2,
    });
    assert.deepEqual(parseTestSummary('Tests:       1 failed, 4 passed, 5 total'), {
      passed: 4,
      failed: 1,
      skipped: 0,
    });
    assert.deepEqual(parseTestSummary('  5 passing\n  2 failing\n  1 pending'), {
      passed: 5,
      failed: 2,
      skipped: 1,
    });
    assert.deepEqual(parseTestSummary('test result: FAILED. 7 passed; 1 failed; 2 ignored; 0 measured'), {
      passed: 7,
      failed: 1,
      skipped: 2,
    });
    assert.deepEqual(parseTestSummary('1 error, 2 passed'), { passed: 2, failed: 1, skipped: 0 });
    assert.equal(parseTestSummary('nothing to report here'), null);
    assert.equal(parseTestSummary(''), null);
  });

  it('uses the runner summary for the compacted header of a long pytest log', () => {
    const noise = 'x'.repeat(8_500);
    const log =
      `${noise}\nFAIL test_alpha\nFAIL test_beta\nFAIL test_gamma\n` +
      '===== 3 failed, 10 passed, 2 skipped in 1.23s =====';
    const out = compactTestLog(log);
    // Accurate counts from the summary line, not a bare `Passed 0` tally.
    assert.match(out, /Tests: 10 passed, 3 failed, 2 skipped/);
    assert.match(out, /test_alpha/);
    assert.equal(out.includes('x'.repeat(100)), false);
  });

  it('keeps existing behavior intact (no regressions)', () => {
    assert.equal(compactTestLog(''), '');
    assert.equal(compactTestLog('ok\n'), 'ok\n');
    assert.equal(looksLikeSuiteLog('ok\n'), false);
    const jsonLines = ['[Command: go test]'];
    for (let i = 0; i < 20; i++) jsonLines.push(`{"Action":"pass","Test":"TestOk${i}"}`);
    jsonLines.push('{"Action":"fail","Test":"TestBad"}');
    const out = foldToolOutputForPrompt(jsonLines.join('\n'));
    assert.match(out, /20 passed, 1 failed/);
    assert.equal(out.includes('TestOk0'), false);
  });
});
