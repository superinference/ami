import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { searchSessions } from '../src/session-search';
import { isValidSessionId } from '../src/session';
import { projectStateDir } from '../src/reference-journal';

/**
 * U077 — session-id validation of on-disk `*.json` filenames before enumeration.
 *
 * Parity with the reference's `listSessionsImpl` UUID-filename validation
 * (listSessionsImpl.ts): a stray `.json` file dropped in the sessions dir whose
 * stem is NOT a format-valid session id (empty stem `.json`, or a crafted
 * traversal stem `...json` → `..`) must NOT be scanned as a session. AMI's id
 * contract is {@link isValidSessionId} (ids are `session-…`, not UUIDs), so the
 * filter reuses that single canonical validator — no second id grammar.
 */
describe('searchSessions — session-id filename validation (U077)', () => {
  let tmpDir: string;
  let sessDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-u077-'));
    sessDir = projectStateDir(tmpDir, 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    // A real, valid session that legitimately matches the query.
    fs.writeFileSync(path.join(sessDir, 'session-real.json'), JSON.stringify({
      id: 'session-real',
      messages: [{ role: 'user', content: 'uniqueneedletoken' }],
    }));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('skips an empty-stem ".json" file (stem "" is not a valid session id)', () => {
    // A stray file literally named ".json" — stem is "", which isValidSessionId rejects.
    fs.writeFileSync(path.join(sessDir, '.json'), JSON.stringify({
      messages: [{ role: 'user', content: 'uniqueneedletoken' }],
    }));
    const results = searchSessions(tmpDir, 'uniqueneedletoken');
    // Only the real session must surface; the empty-stem file is never scanned.
    assert.equal(results.length, 1);
    assert.equal(results[0].sessionId, 'session-real');
  });

  it('skips a crafted traversal-stem file ("...json" → stem "..")', () => {
    // "...json" ends with ".json"; stripping the suffix yields "..", which
    // isValidSessionId rejects as a path-traversal stem.
    fs.writeFileSync(path.join(sessDir, '...json'), JSON.stringify({
      messages: [{ role: 'user', content: 'uniqueneedletoken' }],
    }));
    const results = searchSessions(tmpDir, 'uniqueneedletoken');
    assert.equal(results.length, 1);
    assert.equal(results[0].sessionId, 'session-real');
  });

  it('still scans files whose stem is a valid session id', () => {
    fs.writeFileSync(path.join(sessDir, 'session-second.json'), JSON.stringify({
      id: 'session-second',
      messages: [{ role: 'user', content: 'uniqueneedletoken again' }],
    }));
    const ids = searchSessions(tmpDir, 'uniqueneedletoken').map(r => r.sessionId).sort();
    assert.deepEqual(ids, ['session-real', 'session-second']);
  });

  it('isValidSessionId is the canonical validator the filter reuses', () => {
    // Guards against a divergent second id grammar being introduced.
    assert.equal(isValidSessionId(''), false);
    assert.equal(isValidSessionId('..'), false);
    assert.equal(isValidSessionId('session-real'), true);
  });
});
