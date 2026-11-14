import { test } from 'node:test';
import assert from 'node:assert/strict';
import { selectIncrementalWindow } from '../src/session-memory';
import type { Message } from '../src/types';

const msgs: Message[] = [
  { role: 'user', content: 'a', uuid: 'u1' },
  { role: 'assistant', content: 'b', uuid: 'a1' },
  { role: 'user', content: 'c', uuid: 'u2' },
  { role: 'assistant', content: 'd', uuid: 'a2' },
];

test('selectIncrementalWindow: returns only messages AFTER the cursor + the new cursor', () => {
  const r = selectIncrementalWindow(msgs, 'a1');
  assert.deepEqual(r.window.map(m => m.uuid), ['u2', 'a2']);
  assert.equal(r.nextCursor, 'a2');
});

test('selectIncrementalWindow: cursor at the last message yields an empty window (nothing new)', () => {
  const r = selectIncrementalWindow(msgs, 'a2');
  assert.deepEqual(r.window, []);
  assert.equal(r.nextCursor, 'a2');
});

test('selectIncrementalWindow: absent cursor falls back to the full transcript', () => {
  const r = selectIncrementalWindow(msgs, undefined);
  assert.equal(r.window.length, 4);
  assert.equal(r.nextCursor, 'a2');
});

test('selectIncrementalWindow: a cursor no longer present (compacted away) falls back to full scan', () => {
  const r = selectIncrementalWindow(msgs, 'gone');
  assert.equal(r.window.length, 4);
  assert.equal(r.nextCursor, 'a2');
});

test('selectIncrementalWindow: empty transcript yields empty window and no cursor', () => {
  const r = selectIncrementalWindow([], 'x');
  assert.deepEqual(r.window, []);
  assert.equal(r.nextCursor, undefined);
});

test('selectIncrementalWindow: nextCursor skips a trailing message that has no uuid', () => {
  const withTail: Message[] = [...msgs, { role: 'user', content: 'e' }];
  const r = selectIncrementalWindow(withTail, 'a2');
  // window is the uuid-less trailing message; nextCursor stays the last *uuid* seen.
  assert.equal(r.window.length, 1);
  assert.equal(r.nextCursor, 'a2');
});
