import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { AdmissionLog } from '../src/admission-log';
import type { AdmissionEntry } from '../src/admission-log';

describe('AdmissionLog', () => {
  it('constructor defaults maxEntries to 10000', () => {
    const log = new AdmissionLog();
    // Fill beyond default — should cap at 10000
    for (let i = 0; i < 10_001; i++) {
      log.record({ subject: 'tool', action: 'register', name: `t${i}`, source: 'test', outcome: 'allowed' });
    }
    assert.equal(log.getEntries().length, 10_000);
  });

  it('record adds an entry with timestamp', () => {
    const log = new AdmissionLog();
    const before = Date.now();
    const entry = log.record({ subject: 'tool', action: 'register', name: 'bash', source: 'core', outcome: 'allowed' });
    const after = Date.now();
    assert.ok(entry.timestamp >= before && entry.timestamp <= after);
    assert.equal(log.getEntries().length, 1);
  });

  it('allowed creates an allowed entry', () => {
    const log = new AdmissionLog();
    const entry = log.allowed('tool', 'register', 'grep', 'core');
    assert.equal(entry.outcome, 'allowed');
    assert.equal(entry.subject, 'tool');
    assert.equal(entry.action, 'register');
    assert.equal(entry.name, 'grep');
    assert.equal(entry.source, 'core');
    assert.equal(entry.reason, undefined);
  });

  it('denied creates a denied entry with reason', () => {
    const log = new AdmissionLog();
    const entry = log.denied('mcp_server', 'connect', 'evil-server', 'user', 'not on allowlist');
    assert.equal(entry.outcome, 'denied');
    assert.equal(entry.reason, 'not on allowlist');
    assert.equal(entry.subject, 'mcp_server');
    assert.equal(entry.name, 'evil-server');
  });

  it('error creates an error entry with reason', () => {
    const log = new AdmissionLog();
    const entry = log.error('plugin', 'load', 'broken-plugin', 'marketplace', 'syntax error');
    assert.equal(entry.outcome, 'error');
    assert.equal(entry.reason, 'syntax error');
    assert.equal(entry.subject, 'plugin');
    assert.equal(entry.action, 'load');
  });

  it('getEntries returns all entries in order', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('tool', 'register', 'b', 'user', 'reserved');
    log.error('plugin', 'load', 'c', 'user', 'crash');
    const entries = log.getEntries();
    assert.equal(entries.length, 3);
    assert.equal(entries[0].name, 'a');
    assert.equal(entries[1].name, 'b');
    assert.equal(entries[2].name, 'c');
  });

  it('getEntriesByOutcome filters correctly for allowed', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('tool', 'register', 'b', 'user', 'reserved');
    log.allowed('tool', 'register', 'c', 'core');
    const allowed = log.getEntriesByOutcome('allowed');
    assert.equal(allowed.length, 2);
    assert.ok(allowed.every(e => e.outcome === 'allowed'));
  });

  it('getEntriesByOutcome filters correctly for denied', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('tool', 'register', 'b', 'user', 'reserved');
    log.denied('mcp_server', 'connect', 'c', 'user', 'blocked');
    const denied = log.getEntriesByOutcome('denied');
    assert.equal(denied.length, 2);
    assert.ok(denied.every(e => e.outcome === 'denied'));
  });

  it('getEntriesBySubject filters by tool', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('mcp_server', 'connect', 'b', 'user', 'blocked');
    log.allowed('tool', 'register', 'c', 'core');
    const tools = log.getEntriesBySubject('tool');
    assert.equal(tools.length, 2);
    assert.ok(tools.every(e => e.subject === 'tool'));
  });

  it('getEntriesBySubject filters by mcp_server', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('mcp_server', 'connect', 'b', 'user', 'blocked');
    const servers = log.getEntriesBySubject('mcp_server');
    assert.equal(servers.length, 1);
    assert.equal(servers[0].name, 'b');
  });

  it('getEntriesByName filters by name', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'bash', 'core');
    log.denied('tool', 'register', 'bash', 'user', 'conflict');
    log.allowed('tool', 'register', 'grep', 'core');
    const bashEntries = log.getEntriesByName('bash');
    assert.equal(bashEntries.length, 2);
    assert.ok(bashEntries.every(e => e.name === 'bash'));
  });

  it('getDeniedCount returns correct count', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('tool', 'register', 'b', 'user', 'r1');
    log.denied('tool', 'register', 'c', 'user', 'r2');
    log.error('plugin', 'load', 'd', 'user', 'crash');
    assert.equal(log.getDeniedCount(), 2);
  });

  it('getErrorCount returns correct count', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.error('plugin', 'load', 'b', 'user', 'crash');
    log.error('hook', 'activate', 'c', 'user', 'timeout');
    assert.equal(log.getErrorCount(), 2);
  });

  it('getSummary counts all outcomes', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.allowed('tool', 'register', 'b', 'core');
    log.denied('mcp_server', 'connect', 'c', 'user', 'blocked');
    log.error('plugin', 'load', 'd', 'user', 'crash');
    const summary = log.getSummary();
    assert.equal(summary.allowed, 2);
    assert.equal(summary.denied, 1);
    assert.equal(summary.error, 1);
  });

  it('clear resets entries', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'a', 'core');
    log.denied('tool', 'register', 'b', 'user', 'r');
    assert.equal(log.getEntries().length, 2);
    log.clear();
    assert.equal(log.getEntries().length, 0);
    assert.equal(log.getDeniedCount(), 0);
  });

  it('maxEntries cap is enforced', () => {
    const log = new AdmissionLog(3);
    log.allowed('tool', 'register', 'a', 'core');
    log.allowed('tool', 'register', 'b', 'core');
    log.allowed('tool', 'register', 'c', 'core');
    const extra = log.record({ subject: 'tool', action: 'register', name: 'd', source: 'core', outcome: 'allowed' });
    assert.equal(log.getEntries().length, 3);
    // The entry object is still returned even if not stored
    assert.equal(extra.name, 'd');
  });

  it('record with metadata preserves it', () => {
    const log = new AdmissionLog();
    const entry = log.record({
      subject: 'mcp_server',
      action: 'connect',
      name: 'my-server',
      source: 'user',
      outcome: 'denied',
      reason: 'not allowed',
      metadata: { port: 8080, retries: 3 },
    });
    assert.deepEqual(entry.metadata, { port: 8080, retries: 3 });
    assert.deepEqual(log.getEntries()[0].metadata, { port: 8080, retries: 3 });
  });

  it('multiple entries from different subjects/actions', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'bash', 'core');
    log.denied('mcp_server', 'connect', 'srv', 'user', 'blocked');
    log.error('plugin', 'load', 'plg', 'marketplace', 'crash');
    log.allowed('skill', 'activate', 'sk', 'builtin');
    log.denied('hook', 'register', 'hk', 'user', 'conflict');
    assert.equal(log.getEntries().length, 5);
    assert.equal(log.getEntriesBySubject('tool').length, 1);
    assert.equal(log.getEntriesBySubject('mcp_server').length, 1);
    assert.equal(log.getEntriesBySubject('plugin').length, 1);
    assert.equal(log.getEntriesBySubject('skill').length, 1);
    assert.equal(log.getEntriesBySubject('hook').length, 1);
  });

  it('getSummary returns zeros on empty log', () => {
    const log = new AdmissionLog();
    const summary = log.getSummary();
    assert.equal(summary.allowed, 0);
    assert.equal(summary.denied, 0);
    assert.equal(summary.error, 0);
  });

  it('getEntriesByName returns empty for unknown', () => {
    const log = new AdmissionLog();
    log.allowed('tool', 'register', 'bash', 'core');
    assert.equal(log.getEntriesByName('nonexistent').length, 0);
  });
});
