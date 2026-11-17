import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  SessionManager,
  collectContentReplacements,
  forkContentReplacements,
  type Session,
  type ContentReplacementEntry,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

/**
 * U021 — Content-replacement record persistence + fork preservation.
 *
 * Parity:
 *  - types/logs.ts `ContentReplacementEntry` — persisted replacement decisions with
 *    a main-thread (`agentId` absent) vs sidechain (`agentId` set) discriminator,
 *    replayed on resume for prompt-cache stability.
 *  - commands/branch/branch.ts L98-158 — `createFork` carries the ORIGINAL session's
 *    main-thread replacement records across the fork, re-emitted as a SINGLE entry
 *    under the fork's sessionId.
 *
 * RED-GREEN: before this unit `collectContentReplacements` / `forkContentReplacements`
 * did not exist and `fork()` dropped content-replacement records entirely (a forked
 * session resumed with an empty replacement set → stubbed tool_results re-sent in
 * full → prompt-cache miss). The fork-preservation test below fails on revert of the
 * `forkContentReplacements` wiring in `SessionManager.fork`.
 */

function mkmgr(): { mgr: SessionManager; dir: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ctrepl-'));
  const mgr = new SessionManager(dir, path.join(dir, 'sessions'));
  return { mgr, dir };
}

function baseSession(id: string, messages: Message[], extra: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages,
    createdAt: '2020-01-01T00:00:00.000Z',
    updatedAt: '2020-01-01T00:00:00.000Z',
    provider: { model: 'm', baseUrl: 'b' },
    ...extra,
  };
}

const rec = (toolUseId: string, replacement: string) => ({ kind: 'tool-result' as const, toolUseId, replacement });

// --- collectContentReplacements -------------------------------------------

test('collectContentReplacements: undefined / non-array input yields []', () => {
  assert.deepEqual(collectContentReplacements(undefined), []);
  assert.deepEqual(collectContentReplacements(null as unknown as ContentReplacementEntry[]), []);
  assert.deepEqual(collectContentReplacements([] as ContentReplacementEntry[]), []);
});

test('collectContentReplacements: main-thread read takes ONLY entries with no agentId', () => {
  const entries: ContentReplacementEntry[] = [
    { sessionId: 's1', replacements: [rec('t1', 'stub1')] },
    { sessionId: 's1', agentId: 'agent-A', replacements: [rec('t2', 'stub2')] },
  ];
  const main = collectContentReplacements(entries);
  assert.deepEqual(main, [rec('t1', 'stub1')]);
});

test('collectContentReplacements: sidechain read takes ONLY the matching agentId', () => {
  const entries: ContentReplacementEntry[] = [
    { sessionId: 's1', replacements: [rec('t1', 'stub1')] },
    { sessionId: 's1', agentId: 'agent-A', replacements: [rec('t2', 'stub2')] },
    { sessionId: 's1', agentId: 'agent-B', replacements: [rec('t3', 'stub3')] },
  ];
  assert.deepEqual(collectContentReplacements(entries, { agentId: 'agent-A' }), [rec('t2', 'stub2')]);
  assert.deepEqual(collectContentReplacements(entries, { agentId: 'missing' }), []);
});

test('collectContentReplacements: sessionId selector keys the lookup', () => {
  const entries: ContentReplacementEntry[] = [
    { sessionId: 's1', replacements: [rec('t1', 'a')] },
    { sessionId: 's2', replacements: [rec('t2', 'b')] },
  ];
  assert.deepEqual(collectContentReplacements(entries, { sessionId: 's2' }), [rec('t2', 'b')]);
});

test('collectContentReplacements: drops malformed records from a tampered entry', () => {
  const entries = [
    { sessionId: 's1', replacements: [rec('t1', 'ok'), { kind: 'image', toolUseId: 'x', replacement: 'y' }, { toolUseId: 42 }, null] },
    { sessionId: 's1', replacements: 'not-an-array' },
  ] as unknown as ContentReplacementEntry[];
  assert.deepEqual(collectContentReplacements(entries), [rec('t1', 'ok')]);
});

// --- forkContentReplacements ----------------------------------------------

test('forkContentReplacements: re-emits original main-thread records under the fork id', () => {
  const entries: ContentReplacementEntry[] = [
    { sessionId: 'src', replacements: [rec('t1', 'a'), rec('t2', 'b')] },
    { sessionId: 'src', agentId: 'sub', replacements: [rec('t3', 'c')] }, // sidechain — excluded
    { sessionId: 'other', replacements: [rec('t4', 'd')] },               // other session — excluded
  ];
  const out = forkContentReplacements(entries, 'src', 'fork-1');
  assert.equal(out.length, 1);
  assert.equal(out[0].sessionId, 'fork-1');
  assert.equal(out[0].agentId, undefined);
  assert.deepEqual(out[0].replacements, [rec('t1', 'a'), rec('t2', 'b')]);
});

test('forkContentReplacements: returns [] when there is nothing to carry', () => {
  assert.deepEqual(forkContentReplacements(undefined, 'src', 'fork-1'), []);
  assert.deepEqual(forkContentReplacements([{ sessionId: 'other', replacements: [rec('t1', 'a')] }], 'src', 'fork-1'), []);
  assert.deepEqual(forkContentReplacements([{ sessionId: 'src', agentId: 'sub', replacements: [rec('t1', 'a')] }], 'src', 'fork-1'), []);
});

// --- fork() integration + round-trip --------------------------------------

test('fork: preserves content-replacement records, rewritten to the fork id', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession('session-src', [
      { role: 'user', content: 'q', uuid: 'u1' },
      { role: 'assistant', content: 'a', uuid: 'u2', parentUuid: 'u1' },
    ], {
      contentReplacements: [
        { sessionId: 'session-src', replacements: [rec('tool-1', '[stub]')] },
        { sessionId: 'session-src', agentId: 'sub', replacements: [rec('tool-2', '[sub-stub]')] },
      ],
    });
    const forked = mgr.fork(src);
    assert.ok(forked.contentReplacements, 'fork must carry content-replacement records');
    assert.equal(forked.contentReplacements!.length, 1);
    assert.equal(forked.contentReplacements![0].sessionId, forked.id);
    // Sidechain record (agentId 'sub') is NOT forked; only the main-thread stub.
    assert.deepEqual(forked.contentReplacements![0].replacements, [rec('tool-1', '[stub]')]);
    // The resume reader finds them keyed on the fork's id.
    const replay = collectContentReplacements(forked.contentReplacements, { sessionId: forked.id });
    assert.deepEqual(replay, [rec('tool-1', '[stub]')]);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: records survive save/load round-trip under the fork id', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession('session-src2', [{ role: 'user', content: 'q', uuid: 'u1' }], {
      contentReplacements: [{ sessionId: 'session-src2', replacements: [rec('tool-9', '[s]')] }],
    });
    const forked = mgr.fork(src);
    const reloaded = mgr.load(forked.id);
    assert.ok(reloaded);
    assert.ok(reloaded!.contentReplacements);
    assert.equal(reloaded!.contentReplacements![0].sessionId, forked.id);
    assert.deepEqual(reloaded!.contentReplacements![0].replacements, [rec('tool-9', '[s]')]);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: a source with no content-replacement records leaves the field absent', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession('session-src3', [{ role: 'user', content: 'q', uuid: 'u1' }]);
    const forked = mgr.fork(src);
    assert.equal(forked.contentReplacements, undefined);
    const reloaded = mgr.load(forked.id);
    assert.equal(reloaded!.contentReplacements, undefined);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: a source whose only records are a different session is NOT carried', () => {
  const { mgr, dir } = mkmgr();
  try {
    // A stray entry stamped with a foreign sessionId (e.g. copied in by hand) must
    // not leak into the fork — only the source session's own records are carried.
    const src = baseSession('session-src4', [{ role: 'user', content: 'q', uuid: 'u1' }], {
      contentReplacements: [{ sessionId: 'some-other-session', replacements: [rec('tool-x', '[x]')] }],
    });
    const forked = mgr.fork(src);
    assert.equal(forked.contentReplacements, undefined);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('session: content-replacement records round-trip through save/load directly', () => {
  const { mgr, dir } = mkmgr();
  try {
    const s = baseSession('session-rt', [{ role: 'user', content: 'q', uuid: 'u1' }], {
      contentReplacements: [
        { sessionId: 'session-rt', replacements: [rec('t1', 'a')] },
        { sessionId: 'session-rt', agentId: 'ag', replacements: [rec('t2', 'b')] },
      ],
    });
    mgr.save(s);
    const loaded = mgr.load(asSessionId('session-rt'));
    assert.ok(loaded);
    assert.equal(loaded!.contentReplacements!.length, 2);
    assert.deepEqual(collectContentReplacements(loaded!.contentReplacements, { sessionId: 'session-rt' }), [rec('t1', 'a')]);
    assert.deepEqual(collectContentReplacements(loaded!.contentReplacements, { sessionId: 'session-rt', agentId: 'ag' }), [rec('t2', 'b')]);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
