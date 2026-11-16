import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { formatFileSize } from '../src/utils/format';

describe('formatFileSize', () => {
  it('reports raw bytes with the B unit below 1 KiB', () => {
    assert.equal(formatFileSize(0), '0B');
    assert.equal(formatFileSize(1), '1B');
    assert.equal(formatFileSize(512), '512B');
    assert.equal(formatFileSize(1023), '1023B');
  });

  it('scales into KB with one decimal of precision', () => {
    assert.equal(formatFileSize(1024), '1KB');
    assert.equal(formatFileSize(1536), '1.5KB');
    // 256 KiB exactly — the file-read size cap.
    assert.equal(formatFileSize(256 * 1024), '256KB');
  });

  it('scales into MB, GB and TB', () => {
    assert.equal(formatFileSize(5 * 1024 * 1024), '5MB');
    assert.equal(formatFileSize(1.2 * 1024 * 1024 * 1024), '1.2GB');
    assert.equal(formatFileSize(3 * 1024 * 1024 * 1024 * 1024), '3TB');
  });

  it('drops a trailing .0 so whole values read cleanly', () => {
    assert.equal(formatFileSize(2 * 1024 * 1024), '2MB');
    assert.ok(!formatFileSize(2 * 1024 * 1024).includes('.0'));
  });

  it('defensively returns 0B for non-finite or negative input', () => {
    assert.equal(formatFileSize(Number.NaN), '0B');
    assert.equal(formatFileSize(Number.POSITIVE_INFINITY), '0B');
    assert.equal(formatFileSize(-10), '0B');
  });
});
