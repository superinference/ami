import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { AdmissionLog } from '../src/admission-log';

// Wave 1 coverage: pin branch behavior not exercised by admission-log.test.ts.
// AdmissionLog is a security-audit ledger — these tests assert the guard-relevant
// semantics (cap enforcement, outcome counting, filter predicates) rather than
// merely touching lines.
describe('AdmissionLog wave1 coverage', () => {
  it('getErrorCount returns 0 when no error entries exist (filter false path)', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('tool', 'register', 'b', 'user', 'blocked');
    // No error outcomes at all — predicate must be false for every entry.
    assert.equal(log.getErrorCount(), 0);
  });

  it('getErrorCount on fully-empty log returns 0', () => {
    const log = new AdmissionLog();
    assert.equal(log.getErrorCount(), 0);
  });

  it('getErrorCount counts only error outcomes, excluding allowed and denied', () => {
    const log = new AdmissionLog();
    log.error('hook', 'activate', 'x', 'user', 'timeout');
    log.allowed('tool', 'register', 'y', 'core');
    log.denied('mcp_server', 'connect', 'z', 'user', 'blocked');
    log.error('plugin', 'load', 'w', 'marketplace', 'crash');
    // Exactly the two error entries — if the predicate were inverted or dropped,
    // this would return 0 or 4.
    assert.equal(log.getErrorCount(), 2);
  });

  it('getDeniedCount returns 0 when nothing is denied (filter false path)', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.error('plugin', 'load', 'b', 'user', 'crash');
    assert.equal(log.getDeniedCount(), 0);
  });

  it('cap of 0 stores nothing yet still returns the constructed entry', () => {
    const log = new AdmissionLog(0);
    const e = log.record({ subject: 'tool', action: 'register', name: 'n', source: 'core', outcome: 'allowed' });
    // 0 < 0 is false — nothing stored, but the ledger still hands back the record.
    assert.equal(log.getEntries().length, 0);
    assert.equal(e.name, 'n');
    assert.equal(e.outcome, 'allowed');
  });

  it('explicit non-default maxEntries is honored over the default', () => {
    const log = new AdmissionLog(2);
    log.allowed('tool', 'register', 'a', 'core');
    log.allowed('tool', 'register', 'b', 'core');
    log.allowed('tool', 'register', 'c', 'core');
    assert.equal(log.getEntries().length, 2);
  });

  it('getEntriesByOutcome returns empty array for an outcome with no matches', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    assert.deepEqual(log.getEntriesByOutcome('error'), []);
  });

  it('getEntriesBySubject returns empty array for a subject with no matches', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    assert.deepEqual(log.getEntriesBySubject('skill'), []);
  });
});
