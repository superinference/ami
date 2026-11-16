import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { SessionManager, type Session } from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

function mkmgr(): { mgr: SessionManager; dir: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-forkname-'));
  const mgr = new SessionManager(dir, path.join(dir, 'sessions'));
  return { mgr, dir };
}

function baseSession(messages: Message[], extra: Partial<Session> = {}): Session {
  return {
    id: asSessionId('session-src'),
    messages,
    createdAt: '2020-01-01T00:00:00.000Z',
    updatedAt: '2020-01-01T00:00:00.000Z',
    provider: { model: 'm', baseUrl: 'b' },
    ...extra,
  };
}

// --- Gap: Fork transcript rebuild — sidechain/non-message filtering -----------

test('fork: excludes sidechain messages (only the main conversation is branched)', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([
      { role: 'user', content: 'main q', uuid: 'u1' },
      { role: 'assistant', content: 'sub thinking', uuid: 's1', parentUuid: 'u1', isSidechain: true },
      { role: 'assistant', content: 'main answer', uuid: 'a1', parentUuid: 'u1' },
    ]);
    const forked = mgr.fork(src);
    // The sidechain message must not be carried into the fork.
    assert.equal(forked.messages.length, 2);
    assert.ok(forked.messages.every(m => !m.isSidechain));
    const contents = forked.messages.map(m => m.content);
    assert.deepEqual(contents, ['main q', 'main answer']);
    // Reloaded from disk it is still sidechain-free.
    const onDisk = mgr.load(forked.id);
    assert.equal(onDisk!.messages.length, 2);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// --- Gap: Unique fork-name collision resolution ((Branch), (Branch 2), ...) ---

test('fork: assigns a "(Branch)" title derived from the source title', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([{ role: 'user', content: 'hello', uuid: 'u1' }], {
      customTitle: 'My Feature',
    });
    const forked = mgr.fork(src);
    assert.equal(forked.customTitle, 'My Feature (Branch)');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: dedups the branch title with a numbered suffix on collision', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([{ role: 'user', content: 'hello', uuid: 'u1' }], {
      customTitle: 'Feat',
    });
    const f1 = mgr.fork(src);
    const f2 = mgr.fork(src);
    const f3 = mgr.fork(src);
    assert.equal(f1.customTitle, 'Feat (Branch)');
    assert.equal(f2.customTitle, 'Feat (Branch 2)');
    assert.equal(f3.customTitle, 'Feat (Branch 3)');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: an explicit title overrides the derived base name', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([{ role: 'user', content: 'hello', uuid: 'u1' }], {
      customTitle: 'Ignored',
    });
    const forked = mgr.fork(src, { title: 'Custom' });
    assert.equal(forked.customTitle, 'Custom (Branch)');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: derives the base name from the first user message when untitled', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([
      { role: 'system', content: 'sys', uuid: 's0' },
      { role: 'user', content: '  Build   a widget\nplease  ', uuid: 'u1' },
    ]);
    const forked = mgr.fork(src);
    // Whitespace collapsed + trimmed, no "..." suffix.
    assert.equal(forked.customTitle, 'Build a widget please (Branch)');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('fork: falls back to "Branched conversation" when there is nothing to derive', () => {
  const { mgr, dir } = mkmgr();
  try {
    const src = baseSession([{ role: 'assistant', content: 'only assistant', uuid: 'a1' }]);
    const forked = mgr.fork(src);
    assert.equal(forked.customTitle, 'Branched conversation (Branch)');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// --- searchByTitle helper (exact + prefix) -----------------------------------

test('searchByTitle: matches by exact customTitle and by prefix', () => {
  const { mgr, dir } = mkmgr();
  try {
    mgr.save(baseSession([{ role: 'user', content: 'a' }], { id: asSessionId('s1'), customTitle: 'Alpha (Branch)' }));
    mgr.save(baseSession([{ role: 'user', content: 'b' }], { id: asSessionId('s2'), customTitle: 'Alpha (Branch 2)' }));
    mgr.save(baseSession([{ role: 'user', content: 'c' }], { id: asSessionId('s3'), customTitle: 'Beta' }));
    const exact = mgr.searchByTitle('Alpha (Branch)', { exact: true });
    assert.equal(exact.length, 1);
    assert.equal(exact[0].customTitle, 'Alpha (Branch)');
    const prefix = mgr.searchByTitle('Alpha (Branch');
    assert.equal(prefix.length, 2);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('getUniqueBranchName: skips used numbers (regex-escaped base) and picks the gap', () => {
  const { mgr, dir } = mkmgr();
  try {
    // A base name containing regex metacharacters must be escaped.
    const base = 'a.b+c';
    mgr.save(baseSession([{ role: 'user', content: 'x' }], { id: asSessionId('s1'), customTitle: `${base} (Branch)` }));
    mgr.save(baseSession([{ role: 'user', content: 'y' }], { id: asSessionId('s2'), customTitle: `${base} (Branch 3)` }));
    // (Branch) and (Branch 3) are used → next free is 2.
    assert.equal(mgr.getUniqueBranchName(base), `${base} (Branch 2)`);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
