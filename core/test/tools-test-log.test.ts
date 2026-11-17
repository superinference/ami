import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { compactTestLog, foldToolOutputForPrompt, isTestRunnerCommand, looksLikeSuiteLog } from '../src/tools/test-log';

describe('compactTestLog', () => {
  it('recognizes test runners and leaves other commands alone', () => {
    assert.equal(isTestRunnerCommand('go test -json ./...'), true);
    assert.equal(isTestRunnerCommand('npm test'), true);
    assert.equal(isTestRunnerCommand('make test'), true);
    assert.equal(isTestRunnerCommand('make check'), true);
    assert.equal(isTestRunnerCommand('make build'), false);
    assert.equal(isTestRunnerCommand('echo hello'), false);
    assert.equal(looksLikeSuiteLog('ok\n'), false);
  });

  it('returns empty and already-compact logs unchanged', () => {
    assert.equal(compactTestLog(''), '');
    assert.equal(compactTestLog('Showing failing output only.\npanic'), 'Showing failing output only.\npanic');
  });

  it('keeps a failing go test and drops the passing cases', () => {
    const lines = ['[Command: go test]'];
    for (let i = 0; i < 20; i++) lines.push(`{"Action":"pass","Test":"TestOk${i}"}`);
    lines.push('{"Action":"output","Test":"TestBLPModel","Output":"panic: nil pointer"}');
    lines.push('{"Action":"fail","Test":"TestBLPModel"}');
    const text = compactTestLog(lines.join('\n'));
    assert.match(text, /TestBLPModel/);
    assert.match(text, /20 passed, 1 failed/);
    assert.equal(text.includes('TestOk0'), false);
    assert.equal(text.includes('"Action":"pass"'), false);
  });

  it('drops spaced and single-line pass events', () => {
    const spaced = [
      ...Array.from({ length: 10 }, (_, i) => `{"Action": "pass", "Test": "TestOk${i}"}`),
      '{"Action": "output", "Test": "TestBLPModel", "Output": "panic: nil pointer"}',
      '{"Action": "fail", "Test": "TestBLPModel"}',
    ].join('\n');
    const spacedText = compactTestLog(spaced);
    assert.equal(spacedText.includes('TestOk0'), false);
    assert.match(spacedText, /TestBLPModel/);

    const blob = Array.from({ length: 30 }, (_, i) => `{"Action":"pass","Test":"TestOk${i}"}`).join('')
      + '{"Action":"fail","Test":"TestBLPModel"}';
    const blobText = foldToolOutputForPrompt(blob);
    assert.equal(blobText.includes('TestOk0'), false);
    assert.match(blobText, /TestBLPModel/);
    assert.ok(blobText.length < 13_000);
  });

  it('caps a long failure list', () => {
    const lines = [];
    for (let i = 0; i < 400; i++) {
      lines.push(`{"Action":"fail","Test":"TestFail${i}","Output":"${'x'.repeat(80)}"}`);
    }
    const text = compactTestLog(lines.join('\n'));
    assert.ok(text.length < 13_000);
    assert.match(text, /failing output truncated/);
  });

  it('returns a short non-json log as-is and trims a long one to failures', () => {
    assert.equal(compactTestLog('ok\n'), 'ok\n');
    const noisy = `${'x'.repeat(9_000)}\nFAIL TestBLPModel\npanic: nil pointer\nError: missing file\n`;
    const text = compactTestLog(noisy);
    assert.match(text, /TestBLPModel/);
    assert.equal(text.includes('x'.repeat(100)), false);

    // A >gate log with too few interesting lines is head-sliced to the gate AND
    // carries a quantitative truncation marker — never a silent hard cut.
    const boring = 'y'.repeat(9_000);
    const boringOut = compactTestLog(boring);
    assert.ok(boringOut.startsWith('y'.repeat(8_000)));
    assert.match(boringOut, /\n\.\.\.\[\d+ lines truncated\]$/);
    assert.equal(boringOut.includes('y'.repeat(8_050)), false);

    const longFails = Array.from({ length: 120 }, (_, i) => `FAIL Test${i} ${'z'.repeat(200)}`).join('\n');
    const capped = compactTestLog(`${'q'.repeat(8_000)}\n${longFails}`);
    assert.match(capped, /failing output truncated/);
    assert.equal(capped.includes('q'.repeat(100)), false);
  });
});

describe('compactTestLog — truncation marker on the head-slice fallback', () => {
  it('appends a quantitative "[N lines truncated]" marker (no silent cut)', () => {
    // 8 lines over the 8_000 gate, each boring (no FAIL text) → interesting<3 path.
    const boring = Array.from({ length: 8 }, () => 'y'.repeat(1_200)).join('\n');
    assert.ok(boring.length > 8_000);
    const out = compactTestLog(boring);
    assert.ok(out.startsWith('y'.repeat(1_200)));
    const m = out.match(/\n\.\.\.\[(\d+) lines truncated\]$/);
    assert.ok(m, 'expected a quantitative truncation marker');
    assert.ok(Number(m![1]) >= 1);
    // The marker reflects the lines dropped past the 8_000-char cut.
    assert.ok(out.length < boring.length);
  });

  it('adds NO marker when the log is exactly at the size gate (nothing dropped)', () => {
    const exact = 'y'.repeat(8_000);
    assert.equal(compactTestLog(exact), exact);
  });
});

describe('compactTestLog — operator-tunable env thresholds (bounded validation)', () => {
  const KEYS = ['AMI_TEST_LOG_SUITE_CAP', 'AMI_TEST_LOG_SIZE_GATE', 'AMI_TEST_LOG_BODY_LINES'];
  const saved: Record<string, string | undefined> = {};
  function set(k: string, v: string | undefined) {
    if (!(k in saved)) saved[k] = process.env[k];
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  afterEach(() => {
    for (const k of KEYS) {
      if (saved[k] === undefined) delete process.env[k];
      else process.env[k] = saved[k];
    }
    for (const k of Object.keys(saved)) delete saved[k];
  });

  it('AMI_TEST_LOG_SIZE_GATE widens the compaction threshold', () => {
    const boring = 'y'.repeat(9_000);
    // Default 8_000 gate → sliced + marker.
    assert.match(compactTestLog(boring), /lines truncated/);
    // Widened gate → passed through whole, untouched.
    set('AMI_TEST_LOG_SIZE_GATE', '20000');
    assert.equal(compactTestLog(boring), boring);
  });

  it('a non-numeric AMI_TEST_LOG_SIZE_GATE falls back to the default', () => {
    set('AMI_TEST_LOG_SIZE_GATE', 'not-a-number');
    assert.match(compactTestLog('y'.repeat(9_000)), /lines truncated/);
  });

  it('an over-limit AMI_TEST_LOG_SIZE_GATE is capped (not applied verbatim)', () => {
    // 999_999 > CHAR_CAP_UPPER_LIMIT(150_000) → capped to 150_000, so a 9_000 log
    // is still below the effective gate and passes through whole.
    set('AMI_TEST_LOG_SIZE_GATE', '999999');
    assert.equal(compactTestLog('y'.repeat(9_000)), 'y'.repeat(9_000));
  });

  it('AMI_TEST_LOG_SUITE_CAP widens the suite truncation cap', () => {
    const lines = [];
    for (let i = 0; i < 400; i++) {
      lines.push(`{"Action":"fail","Test":"TestFail${i}","Output":"${'x'.repeat(80)}"}`);
    }
    const log = lines.join('\n');
    assert.match(compactTestLog(log), /failing output truncated/); // default 12_000 cap
    set('AMI_TEST_LOG_SUITE_CAP', '500000');
    assert.equal(compactTestLog(log).includes('failing output truncated'), false);
  });

  it('AMI_TEST_LOG_BODY_LINES limits how many failing lines are kept', () => {
    const body = Array.from({ length: 40 }, (_, i) => `FAIL TestCase${i} boom`).join('\n');
    const log = `${'q'.repeat(8_001)}\n${body}`;
    const countFails = (s: string) => (s.match(/FAIL TestCase/g) ?? []).length;
    assert.ok(countFails(compactTestLog(log)) > 5); // default 120 keeps all 40
    set('AMI_TEST_LOG_BODY_LINES', '5');
    assert.equal(countFails(compactTestLog(log)), 5);
  });
});
