import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  SessionManager,
  detectTurnInterruption,
  type Session,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

function mkmgr(): { mgr: SessionManager; dir: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-fork-'));
  const mgr = new SessionManager(dir, path.join(dir, 'sessions'));
  return { mgr, dir };
}

function baseSession(messages: Message[]): Session {
  return {
    id: asSessionId('session-src'),
    messages,
    createdAt: '2020-01-01T00:00:00.000Z',
    updatedAt: '2020-01-01T00:00:00.000Z',
    provider: { model: 'm', baseUrl: 'b' },
  };
}

test('fork: remaps every message uuid while preserving the parentUuid chain', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([
      { role: 'user', content: 'hi', uuid: 'u1' },
      { role: 'assistant', content: 'yo', uuid: 'a1', parentUuid: 'u1' },
      { role: 'user', content: 'more', uuid: 'u2', parentUuid: 'a1' },
    ]);
    const forked = mgr.fork(src);
    // New session identity
    assert.notEqual(forked.id, src.id);
    assert.equal(forked.messages.length, 3);
    // Every uuid is remapped (none kept from the source)
    const newUuids = forked.messages.map(m => m.uuid);
    assert.ok(newUuids.every(u => typeof u === 'string' && u.length > 0));
    assert.ok(!newUuids.includes('u1'));
    assert.ok(!newUuids.includes('a1'));
    assert.ok(!newUuids.includes('u2'));
    // parentUuid chain preserved under the remap
    assert.equal(forked.messages[0].parentUuid, undefined);
    assert.equal(forked.messages[1].parentUuid, forked.messages[0].uuid);
    assert.equal(forked.messages[2].parentUuid, forked.messages[1].uuid);
    // Persisted to disk under a new file
    const onDisk = mgr.load(forked.id);
    assert.ok(onDisk);
    assert.equal(onDisk!.messages.length, 3);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: upToMessageId truncates the prefix at the cut node (inclusive)', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([
      { role: 'user', content: 'a', uuid: 'u1' },
      { role: 'assistant', content: 'b', uuid: 'a1', parentUuid: 'u1' },
      { role: 'user', content: 'c', uuid: 'u2', parentUuid: 'a1' },
      { role: 'assistant', content: 'd', uuid: 'a2', parentUuid: 'u2' },
    ]);
    const forked = mgr.fork(src, { upToMessageId: 'a1' });
    assert.equal(forked.messages.length, 2);
    assert.equal(forked.messages[1].parentUuid, forked.messages[0].uuid);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: unknown upToMessageId throws', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([{ role: 'user', content: 'a', uuid: 'u1' }]);
    assert.throws(() => mgr.fork(src, { upToMessageId: 'nope' }), /not found/);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('detectTurnInterruption: flags assistant tool_calls with no matching tool result', () => {
  const messages: Message[] = [
    { role: 'user', content: 'run it' },
    {
      role: 'assistant',
      content: null,
      tool_calls: [
        { id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } },
        { id: 'tc2', type: 'function', function: { name: 'bash', arguments: '{}' } },
      ],
    },
    { role: 'tool', tool_call_id: 'tc1', content: 'done' },
  ];
  const r = detectTurnInterruption(messages);
  assert.equal(r.interrupted, true);
  assert.deepEqual(r.pendingToolCallIds, ['tc2']);
});

test('detectTurnInterruption: no interruption when every tool_call is answered', () => {
  const messages: Message[] = [
    {
      role: 'assistant',
      content: null,
      tool_calls: [{ id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{}' } }],
    },
    { role: 'tool', tool_call_id: 'tc1', content: 'ok' },
  ];
  const r = detectTurnInterruption(messages);
  assert.equal(r.interrupted, false);
  assert.deepEqual(r.pendingToolCallIds, []);
});

test('detectTurnInterruption: empty / tool-call-free transcript is not interrupted', () => {
  assert.deepEqual(detectTurnInterruption([]), { interrupted: false, pendingToolCallIds: [] });
  assert.deepEqual(
    detectTurnInterruption([{ role: 'user', content: 'hi' }]),
    { interrupted: false, pendingToolCallIds: [] },
  );
});
