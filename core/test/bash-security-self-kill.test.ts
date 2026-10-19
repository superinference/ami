import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectSelfProcessKill, validateBashSecurity } from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// detectSelfProcessKill — unit tests
// ---------------------------------------------------------------------------

describe('detectSelfProcessKill', () => {
  // --- Blocked: pkill -f with CLI-matching patterns ---

  it('blocks pkill -f superinference', () => {
    const result = detectSelfProcessKill('pkill -f superinference');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('superinference'));
  });

  it('blocks pkill -f node', () => {
    const result = detectSelfProcessKill('pkill -f node');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('node'));
  });

  it('blocks pkill -f ami', () => {
    const result = detectSelfProcessKill('pkill -f ami');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('ami'));
  });

  it('blocks pkill -f tsx', () => {
    const result = detectSelfProcessKill('pkill -f tsx');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('tsx'));
  });

  it('blocks pkill --full superinference', () => {
    const result = detectSelfProcessKill('pkill --full superinference');
    assert.equal(result.blocked, true);
  });

  it('blocks pkill -9f superinference (combined flags)', () => {
    const result = detectSelfProcessKill('pkill -9f superinference');
    assert.equal(result.blocked, true);
  });

  it('blocks pkill -f -9 node (signal before pattern)', () => {
    const result = detectSelfProcessKill('pkill -f -9 node');
    assert.equal(result.blocked, true);
  });

  // --- Blocked: killall targeting CLI processes ---

  it('blocks killall node', () => {
    const result = detectSelfProcessKill('killall node');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('node'));
  });

  it('blocks killall superinference', () => {
    const result = detectSelfProcessKill('killall superinference');
    assert.equal(result.blocked, true);
  });

  it('blocks killall ami', () => {
    const result = detectSelfProcessKill('killall ami');
    assert.equal(result.blocked, true);
  });

  it('blocks killall -9 node (with signal flag)', () => {
    const result = detectSelfProcessKill('killall -9 node');
    assert.equal(result.blocked, true);
  });

  // --- Blocked: kill $(pgrep -f ...) ---

  it('blocks kill $(pgrep -f superinference)', () => {
    const result = detectSelfProcessKill('kill $(pgrep -f superinference)');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('superinference'));
  });

  it('blocks kill -9 $(pgrep -f node)', () => {
    const result = detectSelfProcessKill('kill -9 $(pgrep -f node)');
    assert.equal(result.blocked, true);
  });

  // --- Blocked: kill $$ (own process) ---

  it('blocks kill $$', () => {
    const result = detectSelfProcessKill('kill $$');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('$$'));
  });

  it('blocks kill -9 $$', () => {
    const result = detectSelfProcessKill('kill -9 $$');
    assert.equal(result.blocked, true);
  });

  it('blocks kill -SIGTERM $$', () => {
    const result = detectSelfProcessKill('kill -SIGTERM $$');
    assert.equal(result.blocked, true);
  });

  // --- Blocked: pkill -P $$ (kill children) ---

  it('blocks pkill -P $$', () => {
    const result = detectSelfProcessKill('pkill -P $$');
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('pkill -P $$'));
  });

  it('blocks pkill -9 -P $$', () => {
    const result = detectSelfProcessKill('pkill -9 -P $$');
    assert.equal(result.blocked, true);
  });

  // --- Allowed: safe commands ---

  it('allows pkill -f some-other-process', () => {
    const result = detectSelfProcessKill('pkill -f some-other-process');
    assert.equal(result.blocked, false);
  });

  it('allows killall some-other-process', () => {
    const result = detectSelfProcessKill('killall some-other-process');
    assert.equal(result.blocked, false);
  });

  it('allows kill 12345 (specific PID)', () => {
    const result = detectSelfProcessKill('kill 12345');
    assert.equal(result.blocked, false);
  });

  it('allows kill -9 12345 (specific PID with signal)', () => {
    const result = detectSelfProcessKill('kill -9 12345');
    assert.equal(result.blocked, false);
  });

  it('allows pkill -f my-app', () => {
    const result = detectSelfProcessKill('pkill -f my-app');
    assert.equal(result.blocked, false);
  });

  it('allows killall firefox', () => {
    const result = detectSelfProcessKill('killall firefox');
    assert.equal(result.blocked, false);
  });

  it('allows pgrep -f node (listing only, no kill)', () => {
    const result = detectSelfProcessKill('pgrep -f node');
    assert.equal(result.blocked, false);
  });

  it('allows empty string', () => {
    const result = detectSelfProcessKill('');
    assert.equal(result.blocked, false);
  });

  it('allows commands with no kill-related verbs', () => {
    const result = detectSelfProcessKill('ls -la /tmp');
    assert.equal(result.blocked, false);
  });
});

// ---------------------------------------------------------------------------
// validateBashSecurity integration — self-process kill check is wired in
// ---------------------------------------------------------------------------

describe('validateBashSecurity: self-process kill', () => {
  it('fails pkill -f superinference through validateBashSecurity', () => {
    const result = validateBashSecurity('pkill -f superinference');
    assert.equal(result.safe, false);
    assert.ok(result.message.includes('superinference'));
  });

  it('fails killall node through validateBashSecurity', () => {
    const result = validateBashSecurity('killall node');
    assert.equal(result.safe, false);
    assert.ok(result.message.includes('node'));
  });

  it('fails kill -9 $$ through validateBashSecurity', () => {
    const result = validateBashSecurity('kill -9 $$');
    assert.equal(result.safe, false);
    assert.ok(result.message.includes('$$'));
  });

  it('fails pkill -P $$ through validateBashSecurity', () => {
    const result = validateBashSecurity('pkill -P $$');
    assert.equal(result.safe, false);
  });

  it('passes pkill -f some-other-process through validateBashSecurity', () => {
    const result = validateBashSecurity('pkill -f some-other-process');
    assert.equal(result.safe, true);
  });

  it('passes kill 12345 through validateBashSecurity', () => {
    const result = validateBashSecurity('kill 12345');
    assert.equal(result.safe, true);
  });
});
