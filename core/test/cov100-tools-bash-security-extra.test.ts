import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectPersistenceCommands } from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// detectPersistenceCommands — the `at -f <file> <time>` persistence vector.
//
// The function has two `at` checks:
//   (B) /(?:^|[\s;|&])at\s+-f\s/            // specific "at -f <file>" — runs FIRST
//   (A) /(?:^|[\s;|&])at\s+(?:now|midnight|noon|teatime|\d|[+-])/i  // generic
//
// (A) accepts `[+-]` as the first token after `at `, so it ALSO matches `at -f …`.
// The source orders (B) before (A) so the file-specific reason is reachable (prior
// to the fix the order was reversed, making (B) dead code — a defect found by the
// coverage sweep). These tests pin the fixed, intended behavior: `at -f <file>`
// yields the specific reason, other `at` forms yield the generic one, and all are
// blocked.
// ---------------------------------------------------------------------------

const GENERIC_AT_REASON =
  'at command schedules future command execution — potential persistence vector';
const AT_FROM_FILE_REASON =
  'at -f schedules future command execution from file — potential persistence vector';

describe('detectPersistenceCommands — `at -f` scheduling vector', () => {
  it('blocks `at -f <file> <time>` (persistence is still prevented)', () => {
    const r = detectPersistenceCommands('at -f job.txt now + 5 minutes');
    assert.equal(r.blocked, true);
    assert.ok(r.reason);
  });

  it('blocks `at -f` with a trailing space before the time spec', () => {
    const r = detectPersistenceCommands('at -f script.sh now');
    assert.equal(r.blocked, true);
  });

  it('blocks `at -f` when it follows a command separator', () => {
    assert.equal(detectPersistenceCommands('echo hi; at -f payload.sh noon').blocked, true);
    assert.equal(detectPersistenceCommands('true && at -f payload.sh midnight').blocked, true);
    assert.equal(detectPersistenceCommands('a | at -f payload.sh teatime').blocked, true);
  });

  it('reports the SPECIFIC "from file" reason for `at -f <file>` (fixed ordering)', () => {
    // After the fix, the specific `at -f` check runs before the generic one, so
    // the file-specific reason is produced (was dead code before — red-green).
    const r = detectPersistenceCommands('at -f job.txt now');
    assert.equal(r.reason, AT_FROM_FILE_REASON);
    assert.notEqual(r.reason, GENERIC_AT_REASON);
  });

  it('still reports the GENERIC reason for non-file `at` forms', () => {
    assert.equal(detectPersistenceCommands('at now + 1 hour').reason, GENERIC_AT_REASON);
    assert.equal(detectPersistenceCommands('at midnight').reason, GENERIC_AT_REASON);
    assert.equal(detectPersistenceCommands('at 10:00').reason, GENERIC_AT_REASON);
  });

  it('`at -f` with no whitespace after `-f` is still blocked by the generic check', () => {
    // Check (B) requires `-f\s` (trailing whitespace) and would NOT match here,
    // yet the command is still blocked — proof that (A) is what fires.
    const r = detectPersistenceCommands('at -f');
    assert.equal(r.blocked, true);
    assert.equal(r.reason, GENERIC_AT_REASON);
  });

  it('does not misfire on unrelated commands containing the substring "at"', () => {
    assert.equal(detectPersistenceCommands('cat -f file.txt').blocked, false);
    assert.equal(detectPersistenceCommands('stat -f /tmp').blocked, false);
  });
});
