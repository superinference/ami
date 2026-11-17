/**
 * U021 session.ts parity additions — cross-project & picker-scope listing:
 *  - SessionListEntry.projectPath surfaced by list() (parity: Claude's SessionInfo.cwd);
 *  - list({ filterByBranch }) — the data-model side of LogSelector's Ctrl+B branch filter;
 *  - list({ filterByTag }) + SessionManager.listTags() — the TagTabs All/#tag tab model;
 *  - SessionManager.projectsRoot / listProjectSessionDirs / listAcrossProjects — the
 *    all-projects (dir-omitted) / showAllProjects cross-project aggregation;
 *  - getSessionMessages({ excludeSidechain, buildChain }) — parentUuid-chain transcript
 *    reconstruction with sidechain exclusion;
 *  - compareSessionEntriesByDate — the shared most-recent-first comparator.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  getSessionMessages,
  compareSessionEntriesByDate,
  type Session,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u021-xp-'));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function session(id: string, over: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'u' },
    // Explicit branch so save() never spawns git for determinism.
    gitBranch: 'main',
    ...over,
  } as Session;
}
const user = (content: string, extra: Partial<Message> = {}): Message =>
  ({ role: 'user', content, ...extra } as Message);

describe('list(): projectPath surfaced per entry', () => {
  it('carries the session projectPath into the list entry', () => {
    const dir = mkdir();
    const mgr = new SessionManager('', dir);
    mgr.save(session('session-1', { projectPath: '/home/u/repo-a', messages: [user('hi')] }));
    const [e] = mgr.list();
    assert.equal(e.projectPath, '/home/u/repo-a');
  });
});

describe('list({ filterByBranch })', () => {
  it('keeps only sessions on the given branch; no-branch sessions never match', () => {
    const dir = mkdir();
    const mgr = new SessionManager('', dir);
    mgr.save(session('session-a', { gitBranch: 'feature/x', messages: [user('a')] }));
    mgr.save(session('session-b', { gitBranch: 'main', messages: [user('b')] }));
    mgr.save(session('session-c', { gitBranch: undefined, messages: [user('c')] }));

    const onFeature = mgr.list({ filterByBranch: 'feature/x' });
    assert.deepEqual(onFeature.map(e => String(e.id)), ['session-a']);

    // A branch nothing is on → empty, and the no-branch session is excluded.
    assert.equal(mgr.list({ filterByBranch: 'nope' }).length, 0);
    // No filter → all three listed.
    assert.equal(mgr.list().length, 3);
  });
});

describe('list({ filterByTag }) + listTags()', () => {
  it('filters to one tag and enumerates tags most-used first', () => {
    const dir = mkdir();
    const mgr = new SessionManager('', dir);
    mgr.save(session('session-1', { tag: 'bug', messages: [user('1')] }));
    mgr.save(session('session-2', { tag: 'bug', messages: [user('2')] }));
    mgr.save(session('session-3', { tag: 'feat', messages: [user('3')] }));
    mgr.save(session('session-4', { messages: [user('4')] })); // untagged

    const bugs = mgr.list({ filterByTag: 'bug' });
    assert.deepEqual(bugs.map(e => String(e.id)).sort(), ['session-1', 'session-2']);

    // Empty/whitespace tag filter is ignored (no constraint).
    assert.equal(mgr.list({ filterByTag: '   ' }).length, 4);

    const tags = mgr.listTags();
    assert.deepEqual(tags, [
      { tag: 'bug', count: 2 },
      { tag: 'feat', count: 1 },
    ]);
  });

  it('listTags() ties broken alphabetically', () => {
    const dir = mkdir();
    const mgr = new SessionManager('', dir);
    mgr.save(session('session-1', { tag: 'zebra', messages: [user('1')] }));
    mgr.save(session('session-2', { tag: 'alpha', messages: [user('2')] }));
    assert.deepEqual(mgr.listTags(), [
      { tag: 'alpha', count: 1 },
      { tag: 'zebra', count: 1 },
    ]);
  });
});

describe('cross-project listing', () => {
  function seedTwoProjects(): { env: NodeJS.ProcessEnv; dataDir: string } {
    const dataDir = mkdir();
    const env = { ...process.env, SUPERINFERENCE_DATA_DIR: dataDir };
    const dirA = path.join(SessionManager.projectsRoot(env), 'proj-a', 'sessions');
    const dirB = path.join(SessionManager.projectsRoot(env), 'proj-b', 'sessions');
    new SessionManager('', dirA).save(session('session-a', { projectPath: '/p/a', messages: [user('from a')] }));
    new SessionManager('', dirB).save(session('session-b', { projectPath: '/p/b', messages: [user('from b')] }));
    return { env, dataDir };
  }

  it('projectsRoot resolves under the configured data dir', () => {
    const dataDir = mkdir();
    const env = { ...process.env, SUPERINFERENCE_DATA_DIR: dataDir };
    assert.equal(SessionManager.projectsRoot(env), path.join(dataDir, 'projects'));
  });

  it('listProjectSessionDirs enumerates every project sessions dir, skips non-dirs, [] for missing root', () => {
    const { env } = seedTwoProjects();
    // A project slug with no sessions subdir is skipped.
    fs.mkdirSync(path.join(SessionManager.projectsRoot(env), 'proj-c'), { recursive: true });
    const dirs = SessionManager.listProjectSessionDirs(env).map(d => path.basename(path.dirname(d))).sort();
    assert.deepEqual(dirs, ['proj-a', 'proj-b']);

    const empty = { ...process.env, SUPERINFERENCE_DATA_DIR: mkdir() };
    assert.deepEqual(SessionManager.listProjectSessionDirs(empty), []);
  });

  it('listAcrossProjects aggregates sessions from all projects, each tagged with projectPath', () => {
    const { env } = seedTwoProjects();
    const all = SessionManager.listAcrossProjects({ env });
    const byId = new Map(all.map(e => [String(e.id), e]));
    assert.equal(byId.size, 2);
    assert.equal(byId.get('session-a')!.projectPath, '/p/a');
    assert.equal(byId.get('session-b')!.projectPath, '/p/b');
  });

  it('listAcrossProjects forwards per-store filters (filterByBranch) and paginates the merged set', () => {
    const dataDir = mkdir();
    const env = { ...process.env, SUPERINFERENCE_DATA_DIR: dataDir };
    const dirA = path.join(SessionManager.projectsRoot(env), 'proj-a', 'sessions');
    const dirB = path.join(SessionManager.projectsRoot(env), 'proj-b', 'sessions');
    new SessionManager('', dirA).save(session('session-a', { gitBranch: 'main', projectPath: '/p/a', messages: [user('a')] }));
    new SessionManager('', dirB).save(session('session-b', { gitBranch: 'dev', projectPath: '/p/b', messages: [user('b')] }));

    const onMain = SessionManager.listAcrossProjects({ env, filterByBranch: 'main' });
    assert.deepEqual(onMain.map(e => String(e.id)), ['session-a']);

    // limit applies to the MERGED list (spanning projects).
    assert.equal(SessionManager.listAcrossProjects({ env, limit: 1 }).length, 1);
    assert.equal(SessionManager.listAcrossProjects({ env, offset: 2 }).length, 0);
  });

  it('listAcrossProjects accepts an explicit dir set (scoped listing)', () => {
    const { env } = seedTwoProjects();
    const dirA = path.join(SessionManager.projectsRoot(env), 'proj-a', 'sessions');
    const scoped = SessionManager.listAcrossProjects({ env, dirs: [dirA] });
    assert.deepEqual(scoped.map(e => String(e.id)), ['session-a']);
  });
});

describe('getSessionMessages({ excludeSidechain, buildChain })', () => {
  it('excludeSidechain drops subagent turns', () => {
    const msgs: Message[] = [
      user('main-1'),
      user('side', { isSidechain: true } as Partial<Message>),
      { role: 'assistant', content: 'ok' } as Message,
    ];
    const out = getSessionMessages(msgs, { excludeSidechain: true });
    assert.deepEqual(out.map(m => m.content), ['main-1', 'ok']);
    // Without the flag the sidechain turn is retained.
    assert.equal(getSessionMessages(msgs).length, 3);
  });

  it('buildChain linearizes the parentUuid DAG to the newest main-thread leaf', () => {
    // root -> child-a (abandoned branch), root -> child-b (newest leaf).
    const msgs: Message[] = [
      { role: 'user', content: 'root', uuid: 'r' } as Message,
      { role: 'assistant', content: 'branch-a', uuid: 'a', parentUuid: 'r' } as Message,
      { role: 'assistant', content: 'branch-b', uuid: 'b', parentUuid: 'r' } as Message,
    ];
    const chain = getSessionMessages(msgs, { buildChain: true, includeSystemMessages: true });
    // Newest leaf is 'b'; chain is root -> b in chronological order (branch-a excluded).
    assert.deepEqual(chain.map(m => m.content), ['root', 'branch-b']);
  });

  it('buildChain + offset/limit pagination still applies', () => {
    const msgs: Message[] = [
      { role: 'user', content: 'r', uuid: 'r' } as Message,
      { role: 'user', content: 'm', uuid: 'm', parentUuid: 'r' } as Message,
      { role: 'user', content: 'leaf', uuid: 'l', parentUuid: 'm' } as Message,
    ];
    const out = getSessionMessages(msgs, { buildChain: true, offset: 1, limit: 1 });
    assert.deepEqual(out.map(m => m.content), ['m']);
  });
});

describe('compareSessionEntriesByDate', () => {
  it('orders most-recent first, ties broken on id descending', () => {
    const older = { id: asSessionId('session-1'), date: '2026-01-01T00:00:00.000Z' };
    const newer = { id: asSessionId('session-2'), date: '2026-02-01T00:00:00.000Z' };
    assert.ok(compareSessionEntriesByDate(newer, older) < 0);
    assert.ok(compareSessionEntriesByDate(older, newer) > 0);

    const tieLow = { id: asSessionId('session-a'), date: '2026-01-01T00:00:00.000Z' };
    const tieHigh = { id: asSessionId('session-b'), date: '2026-01-01T00:00:00.000Z' };
    // Same date → higher id sorts first.
    assert.ok(compareSessionEntriesByDate(tieHigh, tieLow) < 0);
  });

  it('treats a missing/invalid date as oldest', () => {
    const dated = { id: asSessionId('session-1'), date: '2026-01-01T00:00:00.000Z' };
    const undatedEntry = { id: asSessionId('session-2') };
    assert.ok(compareSessionEntriesByDate(dated, undatedEntry) < 0);
  });
});
