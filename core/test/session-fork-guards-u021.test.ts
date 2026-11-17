import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { SessionManager, type Session } from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

/**
 * U021 — "Nothing to branch" guards for {@link SessionManager.fork}.
 *
 * Parity: Claude's createFork (commands/branch/branch.ts L77-115) surfaces three
 * clean errors at the boundary of forking a brand-new / empty session:
 *   - unreadable transcript file        -> `No conversation to branch`
 *   - zero-length transcript file       -> `No conversation to branch`
 *   - no main-conversation entries      -> `No messages to branch`
 * AMI's fork() takes a loaded Session, so the first two collapse to "no
 * transcript at all" (missing/empty `messages`) and the third to "every entry is
 * a subagent sidechain".
 *
 * RED-GREEN: before the guard, fork() produced a 0-message fork (or threw the
 * less-clean `upToMessageId not found`) instead of these clean errors.
 */

function mkmgr(): { mgr: SessionManager; dir: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-forkguard-'));
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

test('fork: an empty transcript throws "No conversation to branch"', () => {
  const { mgr, dir } = mkmgr();
  try {
    assert.throws(() => mgr.fork(baseSession([])), /No conversation to branch/);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: a source with a missing/non-array messages field throws "No conversation to branch"', () => {
  const { mgr, dir } = mkmgr();
  try {
    // Simulate a corrupt/partial source whose messages field is not an array.
    const bad = baseSession([]);
    (bad as { messages?: unknown }).messages = undefined;
    assert.throws(() => mgr.fork(bad as Session), /No conversation to branch/);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: a sidechain-only transcript throws "No messages to branch"', () => {
  const { mgr, dir } = mkmgr();
  try {
    // Entries exist, but every one is a subagent sidechain, so the main
    // conversation set is empty — distinct from "no conversation at all".
    const src = baseSession([
      { role: 'user', content: 'sub q', uuid: 's1', isSidechain: true },
      { role: 'assistant', content: 'sub a', uuid: 's2', parentUuid: 's1', isSidechain: true },
    ]);
    assert.throws(() => mgr.fork(src), /No messages to branch/);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: the two empty cases surface DISTINCT messages', () => {
  const { mgr, dir } = mkmgr();
  try {
    let noConversation = '';
    let noMessages = '';
    try { mgr.fork(baseSession([])); } catch (e) { noConversation = (e as Error).message; }
    try {
      mgr.fork(baseSession([{ role: 'user', content: 'x', uuid: 's1', isSidechain: true }]));
    } catch (e) { noMessages = (e as Error).message; }
    assert.equal(noConversation, 'No conversation to branch');
    assert.equal(noMessages, 'No messages to branch');
    assert.notEqual(noConversation, noMessages);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: a single main message still branches (guard does not over-fire)', () => {
  const { mgr, dir } = mkmgr();
  try {
    // One main message mixed with a sidechain: fork succeeds with just the main turn.
    const src = baseSession([
      { role: 'user', content: 'real', uuid: 'u1' },
      { role: 'assistant', content: 'sub', uuid: 's1', parentUuid: 'u1', isSidechain: true },
    ]);
    const forked = mgr.fork(src);
    assert.equal(forked.messages.length, 1);
    assert.equal(forked.messages[0].content, 'real');
    // Persisted and reloadable.
    assert.ok(mgr.load(forked.id));
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: an empty source is rejected BEFORE the upToMessageId lookup', () => {
  const { mgr, dir } = mkmgr();
  try {
    // With no transcript, the clean "No conversation" error wins over the
    // "upToMessageId not found" error (the guard runs first).
    assert.throws(
      () => mgr.fork(baseSession([]), { upToMessageId: 'whatever' }),
      /No conversation to branch/,
    );
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
