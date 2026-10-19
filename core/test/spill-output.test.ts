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

  it('reports omitted char count', () => {
    const text = 'a'.repeat(3000);
    const result = spillToolText(tmpDir, 'cnt', text, 1000);
    assert.ok(result.includes('chars persisted'));
  });

  it('returns text exactly at limit', () => {
    const text = 'b'.repeat(500);
    assert.equal(spillToolText(tmpDir, 'eq', text, 500), text);
  });
});
