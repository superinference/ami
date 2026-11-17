import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { generateTempFilePath } from '../src/utils/tempfile';

describe('generateTempFilePath (U105 — deterministic content-hash temp path)', () => {
  it('defaults to a unique randomUUID id each call, under the temp base', () => {
    const a = generateTempFilePath();
    const b = generateTempFilePath();
    assert.notEqual(a, b, 'random mode must yield distinct paths');
    assert.equal(path.dirname(a), os.tmpdir());
    // default prefix + .md extension
    assert.match(path.basename(a), /^ami-tmp-[0-9a-f-]{36}\.md$/);
  });

  it('honors a custom prefix and extension', () => {
    const p = generateTempFilePath('ami-commit', '.txt');
    assert.match(path.basename(p), /^ami-commit-[0-9a-f-]{36}\.txt$/);
  });

  it('supports an empty extension (extension-less name)', () => {
    const p = generateTempFilePath('ami-raw', '');
    assert.match(path.basename(p), /^ami-raw-[0-9a-f-]{36}$/);
  });

  it('content-hash mode is deterministic: same content → same path across calls', () => {
    const content = 'the-same-content-across-processes';
    const a = generateTempFilePath('ami-note', '.json', { contentHash: content });
    const b = generateTempFilePath('ami-note', '.json', { contentHash: content });
    assert.equal(a, b, 'identical content must yield the identical path (prompt-cache stable)');
    const want = createHash('sha256').update(content).digest('hex').slice(0, 16);
    assert.equal(path.basename(a), `ami-note-${want}.json`);
  });

  it('content-hash mode distinguishes different content', () => {
    const a = generateTempFilePath('ami-note', '.json', { contentHash: 'alpha' });
    const b = generateTempFilePath('ami-note', '.json', { contentHash: 'beta' });
    assert.notEqual(a, b);
  });

  it('honors AMI_TMPDIR override for the temp base', () => {
    const prev = process.env.AMI_TMPDIR;
    const custom = fs_realTmp();
    process.env.AMI_TMPDIR = custom;
    try {
      const p = generateTempFilePath('ami-x', '.md');
      assert.equal(path.dirname(p), custom);
    } finally {
      if (prev === undefined) delete process.env.AMI_TMPDIR;
      else process.env.AMI_TMPDIR = prev;
    }
  });
});

function fs_realTmp(): string {
  // A real, existing dir distinct from the default to prove the override is used.
  return path.join(os.tmpdir(), 'ami-tempfile-override-base');
}
