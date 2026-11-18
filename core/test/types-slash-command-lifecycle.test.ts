import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  SLASH_COMMAND_LIFECYCLE_EVENT,
  buildSlashCommandLifecycleEvent,
  type SlashCommandLifecycleEvent,
  type SlashCommandLifecycleState,
} from '../src/types';

test('SLASH_COMMAND_LIFECYCLE_EVENT is the canonical analytics event name', () => {
  assert.equal(SLASH_COMMAND_LIFECYCLE_EVENT, 'slash_command_lifecycle');
});

test('buildSlashCommandLifecycleEvent shapes a started payload', () => {
  const ev = buildSlashCommandLifecycleEvent('abc-123', '/status', 'started');
  assert.deepEqual(ev, { uuid: 'abc-123', command: '/status', state: 'started' });
});

test('buildSlashCommandLifecycleEvent shapes a completed payload', () => {
  const ev = buildSlashCommandLifecycleEvent('abc-123', '/status', 'completed');
  assert.deepEqual(ev, { uuid: 'abc-123', command: '/status', state: 'completed' });
});

test('started and completed records of one invocation differ only by state', () => {
  const started = buildSlashCommandLifecycleEvent('u1', '/help', 'started');
  const completed = buildSlashCommandLifecycleEvent('u1', '/help', 'completed');
  assert.equal(started.uuid, completed.uuid);
  assert.equal(started.command, completed.command);
  assert.notEqual(started.state, completed.state);
});

test('buildSlashCommandLifecycleEvent returns a fresh plain object each call', () => {
  const a = buildSlashCommandLifecycleEvent('u1', '/x', 'started');
  const b = buildSlashCommandLifecycleEvent('u1', '/x', 'started');
  assert.notEqual(a, b, 'distinct object identities');
  assert.deepEqual(a, b, 'equal by value');
  // Mutating one must not affect the other (no shared reference).
  a.command = '/mutated';
  assert.equal(b.command, '/x');
});

test('every lifecycle state is a valid discriminant', () => {
  const states: SlashCommandLifecycleState[] = ['started', 'completed'];
  for (const s of states) {
    const ev: SlashCommandLifecycleEvent = buildSlashCommandLifecycleEvent('u', '/c', s);
    assert.equal(ev.state, s);
  }
});
