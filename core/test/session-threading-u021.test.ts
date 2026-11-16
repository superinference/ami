/**
 * U021 — session.ts transcript-threading + resume parity additions:
 *  - selectNewestLeaf / buildConversationChain: parentUuid-DAG reconstruction
 *    (newest non-sidechain leaf, chronological order, cycle-safe, branch-aware);
 *  - getSessionMessages: includeSystemMessages / limit / offset filtering;
 *  - appendResumeSentinel + NO_RESPONSE_REQUESTED: synthetic assistant sentinel
 *    after a trailing user prompt on resume;
 *  - getSessionDisplayTitle: deterministic fallback chain (summary > defaultTitle >
 *    autonomous > sessionId.slice(0,8));
 *  - SessionManager.regenerateSessionId + parentSessionId lineage round-trip
 *    through save()/list();
 *  - getSessionLockInfo / getAllLockInfo: read-only lock diagnostics.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  selectNewestLeaf,
  buildConversationChain,
  getSessionMessages,
  appendResumeSentinel,
  NO_RESPONSE_REQUESTED,
  getSessionDisplayTitle,
  getSessionLockInfo,
  getAllLockInfo,
  acquireSessionLock,
  type Session,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u021t-'));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function u(uuid: string, parentUuid?: string, extra: Partial<Message> = {}): Message {
  return { role: 'user', content: `m-${uuid}`, uuid, parentUuid, ...extra } as Message;
}

describe('selectNewestLeaf', () => {
  it('returns the last non-sidechain message with no child', () => {
    const msgs = [u('a'), u('b', 'a'), u('c', 'b')];
    assert.equal(selectNewestLeaf(msgs)?.uuid, 'c');
  });

  it('skips sidechain leaves and picks the main-thread leaf', () => {
    const msgs = [
      u('a'),
      u('b', 'a'),
      { role: 'user', content: 'side', uuid: 's1', parentUuid: 'a', isSidechain: true } as Message,
    ];
    assert.equal(selectNewestLeaf(msgs)?.uuid, 'b');
  });

  it('returns undefined for an empty list and for sidechain-only', () => {
    assert.equal(selectNewestLeaf([]), undefined);
    assert.equal(
      selectNewestLeaf([{ role: 'user', content: 'x', uuid: 's', isSidechain: true } as Message]),
      undefined,
    );
  });
});

describe('buildConversationChain', () => {
  it('reconstructs root→leaf order from the newest leaf', () => {
    const msgs = [u('c', 'b'), u('a'), u('b', 'a')]; // out of order on disk
    const chain = buildConversationChain(msgs);
    assert.deepEqual(chain.map(m => m.uuid), ['a', 'b', 'c']);
  });

  it('honours an explicit leafUuid and excludes a divergent branch', () => {
    // a → b → c (main) ; a → d (fork branch)
    const msgs = [u('a'), u('b', 'a'), u('c', 'b'), u('d', 'a')];
    const chain = buildConversationChain(msgs, 'd');
    assert.deepEqual(chain.map(m => m.uuid), ['a', 'd']);
  });

  it('is cycle-safe: a parentUuid cycle returns a partial chain, never loops', () => {
    // a ↔ b cycle
    const msgs = [u('a', 'b'), u('b', 'a')];
    const chain = buildConversationChain(msgs, 'b');
    // walk: b → a → (b already seen) stop → reversed
    assert.deepEqual(chain.map(m => m.uuid), ['a', 'b']);
  });

  it('returns [] when there is no resolvable leaf', () => {
    assert.deepEqual(buildConversationChain([]), []);
    assert.deepEqual(buildConversationChain([u('a')], 'missing'), []);
  });
});

describe('getSessionMessages', () => {
  const msgs: Message[] = [
    { role: 'system', content: 'sys' } as Message,
    { role: 'user', content: 'q1' } as Message,
    { role: 'assistant', content: 'a1' } as Message,
    { role: 'user', content: 'q2' } as Message,
  ];

  it('excludes system messages by default', () => {
    assert.deepEqual(getSessionMessages(msgs).map(m => m.role), ['user', 'assistant', 'user']);
  });

  it('includes system messages when asked', () => {
    assert.equal(getSessionMessages(msgs, { includeSystemMessages: true }).length, 4);
  });

  it('applies offset then limit (over the filtered set)', () => {
    const page = getSessionMessages(msgs, { offset: 1, limit: 1 });
    assert.equal(page.length, 1);
    assert.equal(page[0].content, 'a1');
  });

  it('treats a non-positive offset as zero and a zero limit as empty', () => {
    assert.equal(getSessionMessages(msgs, { offset: -5 }).length, 3);
    assert.equal(getSessionMessages(msgs, { limit: 0 }).length, 0);
  });
});

describe('appendResumeSentinel', () => {
  it('appends NO_RESPONSE_REQUESTED after a trailing user prompt', () => {
    const out = appendResumeSentinel([
      { role: 'user', content: 'hi' } as Message,
    ]);
    assert.equal(out.length, 2);
    assert.equal(out[1].role, 'assistant');
    assert.equal(out[1].content, NO_RESPONSE_REQUESTED);
  });

  it('skips trailing system messages when locating the last turn', () => {
    const out = appendResumeSentinel([
      { role: 'user', content: 'hi' } as Message,
      { role: 'system', content: 'note' } as Message,
    ]);
    assert.equal(out.length, 3);
    assert.equal(out[1].role, 'assistant'); // inserted right after the user turn
    assert.equal(out[1].content, NO_RESPONSE_REQUESTED);
    assert.equal(out[2].role, 'system');
  });

  it('does not append when the last turn is assistant or tool', () => {
    const asst = [{ role: 'user', content: 'hi' } as Message, { role: 'assistant', content: 'yo' } as Message];
    assert.deepEqual(appendResumeSentinel(asst), asst);
  });

  it('does not append for a meta or compaction-summary trailing user turn', () => {
    const meta = [{ role: 'user', content: 'ctrl', isMeta: true } as Message];
    assert.deepEqual(appendResumeSentinel(meta), meta);
    const summary = [{ role: 'user', content: '[Previous conversation summary] …' } as Message];
    assert.deepEqual(appendResumeSentinel(summary), summary);
  });

  it('returns the input unchanged for an empty transcript and does not mutate input', () => {
    assert.deepEqual(appendResumeSentinel([]), []);
    const input = [{ role: 'user', content: 'hi' } as Message];
    appendResumeSentinel(input);
    assert.equal(input.length, 1); // original untouched
  });
});

describe('getSessionDisplayTitle', () => {
  it('prefers the resolved summary (customTitle wins)', () => {
    assert.equal(
      getSessionDisplayTitle({ id: 'session-x', customTitle: 'My work', aiTitle: 'ai' }),
      'My work',
    );
  });

  it('falls back to lastPrompt/firstPrompt via the summary chain', () => {
    assert.equal(
      getSessionDisplayTitle({ id: 'session-x' }, { firstPrompt: 'do a thing' }),
      'do a thing',
    );
  });

  it('uses defaultTitle when no summary resolves', () => {
    assert.equal(getSessionDisplayTitle({ id: 'session-x' }, {}, 'Default'), 'Default');
  });

  it('labels a detached session with no summary as Autonomous session', () => {
    assert.equal(getSessionDisplayTitle({ id: 'session-x', entrypoint: 'detached' }), 'Autonomous session');
  });

  it('falls back to the first 8 chars of the session id', () => {
    assert.equal(getSessionDisplayTitle({ id: 'session-abcdef-123' }), 'session-');
  });

  it('strips display tags from the summary', () => {
    const out = getSessionDisplayTitle({ id: 'session-x', customTitle: '<ide_opened_file>a.ts</ide_opened_file>real' });
    assert.ok(!out.includes('<ide_opened_file>'));
  });
});

describe('SessionManager.regenerateSessionId + parentSessionId lineage', () => {
  it('records the current session as parent when requested', () => {
    const current = asSessionId('session-parent-1');
    const { id, parentSessionId } = SessionManager.regenerateSessionId({ setCurrentAsParent: true, currentId: current });
    assert.notEqual(id, current);
    assert.equal(parentSessionId, current);
  });

  it('omits parent when not requested or when no currentId given', () => {
    assert.equal(SessionManager.regenerateSessionId().parentSessionId, undefined);
    assert.equal(SessionManager.regenerateSessionId({ setCurrentAsParent: true }).parentSessionId, undefined);
  });

  it('round-trips parentSessionId through save()/load()/list() end to end', () => {
    const dir = mkdir();
    const mgr = new SessionManager(dir, dir);
    const parent = asSessionId('session-old-abc');
    const { id, parentSessionId } = SessionManager.regenerateSessionId({ setCurrentAsParent: true, currentId: parent });
    const session: Session = {
      id,
      parentSessionId,
      messages: [{ role: 'user', content: 'fresh start' } as Message],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      provider: { model: 'm', baseUrl: 'b' },
    };
    mgr.save(session);
    assert.equal(mgr.load(String(id))?.parentSessionId, parent);
    const entry = mgr.list().find(e => String(e.id) === String(id));
    assert.equal(entry?.parentSessionId, parent);
  });
});

describe('session lock diagnostics (getSessionLockInfo / getAllLockInfo)', () => {
  it('returns null when there is no lock', () => {
    const dir = mkdir();
    assert.equal(getSessionLockInfo(dir), null);
  });

  it('reports an active self-held lock and clears on release', () => {
    const dir = mkdir();
    const handle = acquireSessionLock(dir);
    assert.ok(handle.acquired);
    const info = getSessionLockInfo(dir);
    assert.ok(info);
    assert.equal(info!.pid, process.pid);
    assert.equal(info!.self, true);
    assert.equal(info!.active, true);
    assert.ok(info!.ageMs >= 0);
    handle.release();
    assert.equal(getSessionLockInfo(dir), null);
  });

  it('reports a dead holder as inactive (reclaimable)', () => {
    const dir = mkdir();
    acquireSessionLock(dir); // real file on disk
    const info = getSessionLockInfo(dir, { isPidAlive: () => false });
    assert.ok(info);
    assert.equal(info!.active, false);
  });

  it('aggregates across dirs and isolates per-dir errors', () => {
    const d1 = mkdir();
    const d2 = mkdir();
    const h1 = acquireSessionLock(d1);
    const all = getAllLockInfo([d1, d2, path.join(d2, 'does-not-exist')]);
    assert.equal(all.length, 1);
    assert.equal(all[0].path, path.join(d1, '.session.lock'));
    h1.release();
  });
});
