import { describe, it } from 'node:test';
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

    const boring = 'y'.repeat(9_000);
    assert.equal(compactTestLog(boring).length, 8_000);

    const longFails = Array.from({ length: 120 }, (_, i) => `FAIL Test${i} ${'z'.repeat(200)}`).join('\n');
    const capped = compactTestLog(`${'q'.repeat(8_000)}\n${longFails}`);
    assert.match(capped, /failing output truncated/);
    assert.equal(capped.includes('q'.repeat(100)), false);
  });
});
