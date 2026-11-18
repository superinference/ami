import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  formatFileSize,
  formatRelativeTime,
  formatRelativeTimeAgo,
  formatLogMetadata,
} from '../src/utils/format';

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

// Relative-time formatting now lives in utils/format (its canonical leaf home,
// matching the reference's single utils/format.ts) and is consumed by
// formatLogMetadata below. memory.ts re-exports it for its diagnostics consumers;
// these assertions lock the behavior in the NEW import location.
describe('formatRelativeTime / formatRelativeTimeAgo (canonical home)', () => {
  const now = new Date('2026-01-15T12:00:00Z');

  it('narrow style: largest matching interval, "N<unit> ago" for the past', () => {
    assert.equal(formatRelativeTime(new Date('2026-01-12T12:00:00Z'), { now }), '3d ago');
    assert.equal(formatRelativeTime(new Date('2026-01-15T10:30:00Z'), { now }), '1h ago');
    assert.equal(formatRelativeTime(new Date('2025-11-15T12:00:00Z'), { now }), '2mo ago');
  });

  it('narrow style: future gaps render "in N<unit>"', () => {
    assert.equal(formatRelativeTime(new Date('2026-01-15T14:00:00Z'), { now }), 'in 2h');
    assert.equal(formatRelativeTime(new Date('2026-01-18T12:00:00Z'), { now }), 'in 3d');
  });

  it('narrow style: sub-second gaps degrade to 0s', () => {
    assert.equal(formatRelativeTime(new Date('2026-01-15T12:00:00Z'), { now }), '0s ago');
    // A sub-second FUTURE gap truncates to 0 and 0 <= 0, so it reads "0s ago".
    assert.equal(formatRelativeTime(new Date('2026-01-15T12:00:00.500Z'), { now }), '0s ago');
    // A whole-second future gap reads "in 0s" (loop still below the 1s interval).
    assert.equal(formatRelativeTime(new Date('2026-01-15T12:00:01Z'), { now }), 'in 1s');
  });

  it('short/long styles defer to the localized Intl formatter', () => {
    assert.match(formatRelativeTime(new Date('2026-01-15T11:58:00Z'), { now, style: 'short' }), /min/);
    assert.match(formatRelativeTime(new Date('2026-01-15T12:00:00Z'), { now, style: 'short' }), /sec/);
  });

  it('formatRelativeTimeAgo forces numeric:always for the past, passes future through', () => {
    assert.equal(formatRelativeTimeAgo(new Date('2026-01-14T12:00:00Z'), { now }), '1d ago');
    // A future date keeps the caller's options (narrow default) rather than "ago".
    assert.equal(formatRelativeTimeAgo(new Date('2026-01-16T12:00:00Z'), { now }), 'in 1d');
  });
});

describe('formatLogMetadata (session/log listing line)', () => {
  const now = new Date('2026-01-15T12:00:00Z');
  const modified = new Date('2026-01-15T09:00:00Z'); // 3h ago

  it('minimal row: relative time · message count', () => {
    assert.equal(
      formatLogMetadata({ modified, messageCount: 12 }, now),
      '3 hours ago · 12 messages',
    );
  });

  it('fileSize replaces the message-count cell with a humanized size', () => {
    assert.equal(
      formatLogMetadata({ modified, messageCount: 12, fileSize: 2048 }, now),
      '3 hours ago · 2KB',
    );
  });

  it('git branch is inserted between the time and the size/count', () => {
    assert.equal(
      formatLogMetadata({ modified, messageCount: 5, gitBranch: 'main' }, now),
      '3 hours ago · main · 5 messages',
    );
  });

  it('tag, agent, and PR (with and without repository) append in order', () => {
    assert.equal(
      formatLogMetadata(
        { modified, messageCount: 1, tag: 'wip', agentSetting: 'deploy', prNumber: 42 },
        now,
      ),
      '3 hours ago · 1 messages · #wip · @deploy · #42',
    );
    assert.equal(
      formatLogMetadata(
        { modified, messageCount: 1, prNumber: 7, prRepository: 'acme/app' },
        now,
      ),
      '3 hours ago · 1 messages · acme/app#7',
    );
  });

  it('composes every optional field together in the reference order', () => {
    assert.equal(
      formatLogMetadata(
        {
          modified,
          messageCount: 9,
          fileSize: 1024,
          gitBranch: 'feature/x',
          tag: 'review',
          agentSetting: 'build',
          prNumber: 3,
          prRepository: 'org/repo',
        },
        now,
      ),
      '3 hours ago · feature/x · 1KB · #review · @build · org/repo#3',
    );
  });
});
