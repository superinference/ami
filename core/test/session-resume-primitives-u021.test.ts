/**
 * U021 — additive session-resume primitives (parity with the reference's resume path):
 *  - buildConversationChain follows `logicalParentUuid` across a deliberate
 *    session-break nullification (parentUuid === undefined) so lineage spanning a
 *    `/clear`-style break is still reconstructed (logs.ts TranscriptMessage
 *    logicalParentUuid preservation).
 *  - SessionManager.loadLatest({ skipLocked }) skips auto-resuming a store a
 *    DIFFERENT live engine is actively holding (--continue liveness guard;
 *    conversationRecovery.ts L487-512).
 *  - readSessionFile treats a zero-byte file as not-found; loadAcrossProjects
 *    resolves a session id that lives under ANOTHER project dir
 *    (sessionStoragePortable.resolveSessionFilePath zero-byte + all-projects scan).
 *  - resolveWorktreeStateForResume: tri-state worktree restore with a
 *    disk-existence fallback (sessionRestore.restoreWorktreeForResume).
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  buildConversationChain,
  resolveWorktreeStateForResume,
  SESSION_LOCK_FILE,
  type Session,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(prefix = 'ami-u021p-'): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function u(uuid: string, parentUuid?: string, extra: Partial<Message> = {}): Message {
  return { role: 'user', content: `m-${uuid}`, uuid, parentUuid, ...extra } as Message;
}

function makeSession(id: string, messages: Message[] = [{ role: 'user', content: 'hi' } as Message]): Session {
  return {
    id: asSessionId(id),
    messages,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'b' },
  };
}

describe('buildConversationChain — logicalParentUuid across a session break', () => {
  it('spans a nullified parentUuid via logicalParentUuid to reconstruct full lineage', () => {
    const msgs: Message[] = [
      u('a'),                                    // pre-break root
      u('b', 'a'),                               // pre-break turn
      // session break: parentUuid deliberately nullified, logical parent preserved
      { role: 'user', content: 'm-c', uuid: 'c', logicalParentUuid: 'b' } as Message,
      u('d', 'c'),                               // post-break newest leaf
    ];
    const chain = buildConversationChain(msgs);
    assert.deepEqual(chain.map(m => m.uuid), ['a', 'b', 'c', 'd']);
  });

  it('is still cycle-safe when the logical link forms a loop', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'x', uuid: 'a', logicalParentUuid: 'b' } as Message,
      { role: 'user', content: 'y', uuid: 'b', logicalParentUuid: 'a' } as Message,
    ];
    const chain = buildConversationChain(msgs, 'b');
    // partial chain, no infinite loop
    assert.ok(chain.length <= 2 && chain.length >= 1);
  });

  it('prefers a real parentUuid over the logical link when both resolve', () => {
    const msgs: Message[] = [
      u('a'),
      u('b', 'a'),
      { role: 'user', content: 'm-c', uuid: 'c', parentUuid: 'b', logicalParentUuid: 'a' } as Message,
    ];
    const chain = buildConversationChain(msgs, 'c');
    assert.deepEqual(chain.map(m => m.uuid), ['a', 'b', 'c']);
  });
});

describe('SessionManager.loadLatest — skipLocked liveness guard', () => {
  function writeForeignLock(dir: string, pid: number): void {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, SESSION_LOCK_FILE),
      JSON.stringify({ pid, startedAt: Date.now(), execPath: '/usr/bin/ami' }),
    );
  }

  it('returns null when another live engine holds the store lock', () => {
    const dir = mkdir();
    const mgr = new SessionManager('/x', dir);
    mgr.save(makeSession('session-lock-a'));
    writeForeignLock(dir, 999001);
    const deps = { isPidAlive: () => true, getProcessCommand: () => 'ami-foreign', now: () => Date.now() };
    assert.equal(mgr.loadLatest({ skipLocked: true, lockDeps: deps }), null);
  });

  it('still resolves the latest session when the foreign lock is dead (reclaimable)', () => {
    const dir = mkdir();
    const mgr = new SessionManager('/x', dir);
    mgr.save(makeSession('session-lock-b'));
    writeForeignLock(dir, 999002);
    const deps = { isPidAlive: () => false, getProcessCommand: () => null, now: () => Date.now() };
    const loaded = mgr.loadLatest({ skipLocked: true, lockDeps: deps });
    assert.equal(loaded?.id, 'session-lock-b');
  });

  it('ignores the lock when skipLocked is not set (default behaviour unchanged)', () => {
    const dir = mkdir();
    const mgr = new SessionManager('/x', dir);
    mgr.save(makeSession('session-lock-c'));
    writeForeignLock(dir, 999003);
    assert.equal(mgr.loadLatest()?.id, 'session-lock-c');
  });
});

describe('SessionManager — zero-byte skip + cross-project resolution', () => {
  it('treats a zero-byte session file as not-found', () => {
    const dir = mkdir();
    const mgr = new SessionManager('/x', dir);
    fs.writeFileSync(path.join(dir, 'session-empty.json'), '');
    assert.equal(mgr.load('session-empty'), null);
  });

  it('loadAcrossProjects finds a session stored under another project dir', () => {
    const configDir = mkdir('ami-u021cfg-');
    const env = { ...process.env, AMI_DATA_DIR: configDir, SUPERINFERENCE_DATA_DIR: configDir } as NodeJS.ProcessEnv;
    const projRoot = SessionManager.projectsRoot(env);
    const projA = path.join(projRoot, 'proj-a', 'sessions');
    const mgrA = new SessionManager('/a', projA);
    mgrA.save(makeSession('session-cross-xyz'));

    const found = SessionManager.loadAcrossProjects('session-cross-xyz', env);
    assert.equal(found?.id, 'session-cross-xyz');
    assert.equal(SessionManager.loadAcrossProjects('session-missing-000', env), null);
    assert.equal(SessionManager.loadAcrossProjects('../bad', env), null);
  });
});

describe('resolveWorktreeStateForResume — tri-state + disk-existence fallback', () => {
  it('returns none when the session never entered a worktree (undefined)', () => {
    assert.deepEqual(resolveWorktreeStateForResume(undefined), { action: 'none' });
  });

  it('returns none when the session explicitly exited its worktree (null)', () => {
    assert.deepEqual(resolveWorktreeStateForResume(null), { action: 'none' });
  });

  it('returns cleared when the recorded worktree dir no longer exists on disk', () => {
    const state = { originalCwd: '/repo', worktreePath: '/repo/.wt/gone', branch: 'feat' };
    assert.deepEqual(resolveWorktreeStateForResume(state, () => false), { action: 'cleared' });
  });

  it('returns reenter with the worktree coordinates when the dir exists', () => {
    const dir = mkdir('ami-u021wt-');
    const state = { originalCwd: '/repo', worktreePath: dir, branch: 'feat' };
    assert.deepEqual(resolveWorktreeStateForResume(state), {
      action: 'reenter', worktreePath: dir, originalCwd: '/repo', branch: 'feat',
    });
  });
});
