import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { spillToolText } from '../src/tools/spill-output';

describe('spillToolText', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-test-'));
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  it('returns text unchanged when under limit', () => {
    const text = 'short text';
    assert.equal(spillToolText(tmpDir, 'test', text, 1000), text);
  });

  it('truncates text and writes spill file when over limit', () => {
    const text = 'x'.repeat(5000);
    const result = spillToolText(tmpDir, 'big', text, 1000);
    assert.ok(result.length < text.length);
    assert.ok(result.includes('truncated'));
    assert.ok(result.includes('file_read'));

    const spillDir = path.join(tmpDir, '.superinference', 'tool-results');
    assert.ok(fs.existsSync(spillDir));
    const files = fs.readdirSync(spillDir).filter(f => f.startsWith('big-'));
    assert.ok(files.length > 0);
    const spillContent = fs.readFileSync(path.join(spillDir, files[0]), 'utf-8');
    assert.equal(spillContent, text);
  });

  it('includes head and tail in truncated output', () => {
    const text = 'HEAD' + 'x'.repeat(5000) + 'TAIL';
    const result = spillToolText(tmpDir, 'ht', text, 1000);
    assert.ok(result.startsWith('HEAD'));
    assert.ok(result.endsWith('TAIL'));
  });

  it('reports omitted char count with persisted path on success', () => {
    const text = 'a'.repeat(3000);
    const result = spillToolText(tmpDir, 'cnt', text, 1000);
    assert.ok(result.includes('chars'));
    assert.ok(result.includes('persisted to'));
  });

  it('returns text exactly at limit', () => {
    const text = 'b'.repeat(500);
    assert.equal(spillToolText(tmpDir, 'eq', text, 500), text);
  });

  it('writes spill file on success and includes path in output', () => {
    const text = 'y'.repeat(2000);
    const result = spillToolText(tmpDir, 'happy', text, 500);
    const spillDir = path.join(tmpDir, '.superinference', 'tool-results');
    assert.ok(fs.existsSync(spillDir));
    const files = fs.readdirSync(spillDir).filter(f => f.startsWith('happy-'));
    assert.ok(files.length > 0, 'spill file should be created');
    assert.ok(result.includes('persisted to'));
    assert.ok(result.includes(spillDir));
    const content = fs.readFileSync(path.join(spillDir, files[0]), 'utf-8');
    assert.equal(content, text);
  });

  it('gracefully falls back on disk write failure instead of crashing', () => {
    const text = 'HEAD_MARKER' + 'x'.repeat(5000) + 'TAIL_MARKER';
    // /nonexistent/path cannot be created, so mkdirSync will throw EACCES
    // With the fix: function returns truncated text gracefully
    // Without the fix: function throws and crashes
    const result = spillToolText('/nonexistent/path/xyz', 'fail', text, 1000);
    assert.ok(result.length < text.length, 'output should be truncated');
    assert.ok(result.includes('truncated'), 'should mention truncation');
    assert.ok(!result.includes('persisted to /'), 'fallback must not contain spill file path');
    assert.ok(result.startsWith('HEAD_MARKER'), 'head content must be preserved');
    assert.ok(result.endsWith('TAIL_MARKER'), 'tail content must be preserved');
  });

  it('falls back gracefully when mkdir fails (file blocks dir creation)', () => {
    // Create a regular file at .superinference, blocking mkdirSync recursive
    const blockDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-block-'));
    fs.writeFileSync(path.join(blockDir, '.superinference'), 'blocker');
    try {
      const text = 'HEAD' + 'q'.repeat(5000) + 'TAIL';
      // mkdirSync will fail with ENOTDIR because .superinference is a file
      const result = spillToolText(blockDir, 'enotdir', text, 1000);
      assert.ok(result.includes('truncated'), 'result must mention truncation');
      assert.ok(!result.includes('persisted to /'), 'fallback must not contain spill file path');
      assert.ok(result.startsWith('HEAD'), 'head content must be preserved');
      assert.ok(result.endsWith('TAIL'), 'tail content must be preserved');
    } finally {
      fs.rmSync(blockDir, { recursive: true });
    }
  });

  it('falls back gracefully when write fails (read-only spill dir)', () => {
    // Create the spill directory structure but make tool-results read-only
    const roDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-ro-'));
    const spillDir = path.join(roDir, '.superinference', 'tool-results');
    fs.mkdirSync(spillDir, { recursive: true });
    fs.chmodSync(spillDir, 0o444);
    try {
      const text = 'FRONT' + 'w'.repeat(5000) + 'BACK';
      // writeFileSync will fail with EACCES on read-only directory
      const result = spillToolText(roDir, 'readonly', text, 1000);
      assert.ok(result.includes('truncated'), 'result must mention truncation');
      assert.ok(!result.includes('persisted to /'), 'fallback must not contain spill file path');
      assert.ok(result.startsWith('FRONT'), 'head content must be preserved');
      assert.ok(result.endsWith('BACK'), 'tail content must be preserved');
    } finally {
      fs.chmodSync(spillDir, 0o755);
      fs.rmSync(roDir, { recursive: true });
    }
  });

  it('falls back gracefully when cwd does not exist', () => {
    const text = 'r'.repeat(3000);
    const result = spillToolText('/nonexistent/path/xyz-' + Date.now(), 'nopath', text, 1000);
    assert.ok(result.includes('truncated') || result.includes('omitted'), 'result must mention truncation');
    assert.ok(result.includes('could not persist'), 'fallback must mention disk write failure');
    assert.ok(result.startsWith('r'), 'head content preserved');
    assert.ok(result.endsWith('r'), 'tail content preserved');
  });
});
