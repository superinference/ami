import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as http from 'http';

import {
  SessionManager,
  SessionDiffTracker,
  assertPathWithin,
  safeCharSlice,
  isPidAlive,
  acquireSessionLock,
  isSessionLocked,
  type Session,
  type FileChange,
} from '../src/session';
import { SessionMemoryExtractor } from '../src/session-memory';
import { MemoryManager } from '../src/memory';
import type { ProviderConfig, Message } from '../src/types';
import type { SessionFact } from '../src/session-memory';
import type { StreamChunk } from '../src/types';

const IS_ROOT = typeof process.getuid === 'function' && process.getuid() === 0;

// ---------------------------------------------------------------------------
// temp-dir helpers
// ---------------------------------------------------------------------------
const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'sm-cov-'));
  tmpDirs.push(d);
  return d;
}
after(() => {
  for (const d of tmpDirs) {
    try { fs.chmodSync(d, 0o755); } catch { /* ignore */ }
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

function baseSession(over: Partial<Session> = {}): Session {
  return {
    id: 'sess-1',
    messages: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'http://x' },
    ...over,
  };
}

function findDeadPid(): number {
  for (const p of [999999, 888888, 777777, 4194303, 1234567]) {
    if (!isPidAlive(p)) return p;
  }
  return 999999;
}

// ===========================================================================
// session.ts — SessionManager basics + truncation
// ===========================================================================
describe('SessionManager — default dir, ensureDir, truncation, preview', () => {
  it('uses the default .superinference/sessions dir when no override given', () => {
    const cwd = mkTmp();
    const mgr = new SessionManager(cwd);
    assert.equal(mgr.getSessionDir(), path.join(cwd, '.superinference', 'sessions'));
  });

  it('creates the session dir on save when it does not exist', () => {
    const cwd = mkTmp();
    const mgr = new SessionManager(cwd);
    assert.equal(fs.existsSync(mgr.getSessionDir()), false);
    mgr.save(baseSession());
    assert.equal(fs.existsSync(mgr.getSessionDir()), true);
    assert.equal(mgr.load('sess-1')!.id, 'sess-1');
  });

  it('truncates long user/system/tool content and exempts compaction summaries', () => {
    const mgr = new SessionManager('/unused', mkTmp());
    const long = 'x'.repeat(6000);
    mgr.save(baseSession({
      messages: [
        { role: 'user', content: long },
        { role: 'system', content: long },
        { role: 'tool', content: long, toolCallId: 't1' } as Message,
        { role: 'system', content: '[Previous conversation summary]' + 'y'.repeat(6000) },
        { role: 'assistant', content: 'short' },
      ],
    }));
    const [u, s, t, summary, a] = mgr.load('sess-1')!.messages;
    assert.ok((u.content as string).endsWith('... (truncated)'));
    assert.ok((s.content as string).endsWith('... (truncated)'));
    assert.ok((t.content as string).endsWith('... (truncated)'));
    assert.ok((u.content as string).length < 6000);
    assert.equal((summary.content as string).length, '[Previous conversation summary]'.length + 6000);
    assert.equal(a.content, 'short');
  });

  it('trims a trailing high surrogate at the truncation boundary', () => {
    const mgr = new SessionManager('/unused', mkTmp());
    const content = 'a'.repeat(4999) + '😀' + 'b'.repeat(2000);
    mgr.save(baseSession({ messages: [{ role: 'user', content }] }));
    const body = (mgr.load('sess-1')!.messages[0].content as string).replace('\n... (truncated)', '');
    assert.equal(body.length, 4999);
    assert.equal(body.charCodeAt(body.length - 1), 'a'.charCodeAt(0));
  });

  it('trims a leading low surrogate at the start of truncated content', () => {
    const mgr = new SessionManager('/unused', mkTmp());
    const content = '\uDC00' + 'x'.repeat(6000); // lone leading low surrogate
    mgr.save(baseSession({ messages: [{ role: 'user', content }] }));
    const body = (mgr.load('sess-1')!.messages[0].content as string).replace('\n... (truncated)', '');
    assert.equal(body[0], 'x'); // leading low surrogate stripped
  });

  it('extractPreview shortens long first user messages and reports no-messages', () => {
    const mgr = new SessionManager('/unused', mkTmp());
    mgr.save(baseSession({ id: 'a', messages: [{ role: 'user', content: 'word '.repeat(60) }] }));
    mgr.save(baseSession({ id: 'b', messages: [{ role: 'assistant', content: 'hi' }] }));
    const list = mgr.list();
    const a = list.find(e => e.id === 'a')!;
    assert.ok(a.preview.endsWith('...'));
    assert.equal(a.preview.length, 103);
    assert.equal(list.find(e => e.id === 'b')!.preview, '(no messages)');
  });

  it('list() sorts across missing / empty / invalid date fields', () => {
    const dir = mkTmp();
    const mgr = new SessionManager('/unused', dir);
    fs.mkdirSync(dir, { recursive: true });
    const write = (id: string, obj: Record<string, unknown>) =>
      fs.writeFileSync(path.join(dir, `${id}.json`),
        JSON.stringify({ id, messages: [{ role: 'user', content: id }], provider: { model: 'm', baseUrl: 'u' }, ...obj }));
    write('no-updated', { createdAt: '2020-01-01T00:00:00Z' });       // date <- createdAt fallback
    write('empty-dates', { createdAt: '', updatedAt: '' });           // falsy date -> 0 (a & b arms)
    write('empty-dates2', { createdAt: '', updatedAt: '' });          // second falsy so both a/b compared
    write('bad-date', { updatedAt: 'not-a-real-date' });              // truthy but NaN -> 0 (bTime arm)
    write('bad-date2', { updatedAt: 'also-not-a-date' });             // truthy but NaN -> 0 (aTime arm)
    const list = mgr.list();
    assert.equal(list.length, 5);
    for (const id of ['no-updated', 'empty-dates', 'empty-dates2', 'bad-date', 'bad-date2']) {
      assert.ok(list.some(e => e.id === id));
    }
  });
});

// ===========================================================================
// session.ts — assertPathWithin guard (path traversal: allow vs block)
// ===========================================================================
describe('safeCharSlice — surrogate-safe slicing', () => {
  it('returns an empty string unchanged', () => {
    assert.equal(safeCharSlice('', 0, 10), '');
    assert.equal(safeCharSlice('abc', 2, 2), ''); // zero-length slice
  });

  it('trims a split surrogate pair at both ends', () => {
    // '😀' = high(0xD83D) + low(0xDE00). Slice that ends on the high half -> dropped.
    const s = 'a😀b';
    assert.equal(safeCharSlice(s, 0, 2), 'a');  // drops lone high surrogate at end
    assert.equal(safeCharSlice(s, 2, 4), 'b');  // drops lone low surrogate at start
  });
});

describe('assertPathWithin — path traversal guard', () => {
  it('allows a path inside the base dir and the base dir itself', () => {
    assert.doesNotThrow(() => assertPathWithin('/base/dir/file.json', '/base/dir'));
    assert.doesNotThrow(() => assertPathWithin('/base/dir', '/base/dir'));
  });

  it('blocks a path that escapes the base dir', () => {
    assert.throws(
      () => assertPathWithin('/base/other/file.json', '/base/dir'),
      /Path traversal blocked/,
    );
    assert.throws(
      () => assertPathWithin('/base/dir/../escape.json', '/base/dir'),
      /Path traversal blocked/,
    );
  });
});

// ===========================================================================
// session.ts — loadLatest resilience (unstatable + unparseable files)
// ===========================================================================
describe('SessionManager.loadLatest — resilience', () => {
  it('survives a file that throws during the mtime sort and returns an intact one', () => {
    const dir = mkTmp();
    const mgr = new SessionManager('/unused', dir);
    mgr.save(baseSession({ id: 'good' }));
    // Dangling symlink named *.json: statSync (during sort) and readFileSync both throw.
    fs.symlinkSync(path.join(dir, 'does-not-exist.json'), path.join(dir, 'aaa.json'));
    const latest = mgr.loadLatest();
    assert.ok(latest);
    assert.equal(latest!.id, 'good');
  });

  it('returns null when every session file is unparseable', () => {
    const dir = mkTmp();
    const mgr = new SessionManager('/unused', dir);
    fs.writeFileSync(path.join(dir, 'bad.json'), 'this is not json {{{');
    assert.equal(mgr.loadLatest(), null);
  });

  it('returns null when there are no session files', () => {
    const mgr = new SessionManager('/unused', mkTmp());
    assert.equal(mgr.loadLatest(), null);
  });
});

// ===========================================================================
// session.ts — SessionDiffTracker
// ===========================================================================
describe('SessionDiffTracker — full surface', () => {
  it('records, queries, summarizes, and clears', () => {
    const t = new SessionDiffTracker();
    assert.equal(t.hasChanges(), false);
    assert.deepEqual(t.getChanges(), []);
    assert.deepEqual(t.getModifiedPaths(), []);

    const mk = (p: string, type: FileChange['type'], ts: number): FileChange => ({ path: p, type, timestamp: ts });
    assert.equal(t.recordChange(mk('/a', 'create', 100)), true);
    assert.equal(t.recordChange(mk('/a', 'modify', 200)), true);
    assert.equal(t.recordChange(mk('/b', 'delete', 300)), true);
    assert.equal(t.recordChange(mk('/c', 'rename', 400)), true);

    assert.equal(t.hasChanges(), true);
    assert.equal(t.getChangeCount(), 4);
    assert.equal(t.getChanges().length, 4);
    assert.deepEqual(t.getModifiedPaths().sort(), ['/a', '/b', '/c']);
    assert.equal(t.getChangesForPath('/a').length, 2);
    assert.equal(t.getChangesSince(300).length, 2);
    assert.equal(t.getChangesByType('create').length, 1);
    assert.deepEqual(t.getSummary(), { creates: 1, modifies: 1, deletes: 1, renames: 1, uniquePaths: 3 });

    t.clear();
    assert.equal(t.hasChanges(), false);
    assert.equal(t.getChangeCount(), 0);
    assert.deepEqual(t.getModifiedPaths(), []);
  });

  it('rejects changes once max is reached', () => {
    const t = new SessionDiffTracker(1);
    assert.equal(t.recordChange({ path: '/a', type: 'create', timestamp: 1 }), true);
    assert.equal(t.recordChange({ path: '/b', type: 'create', timestamp: 2 }), false);
    assert.equal(t.getChangeCount(), 1);
  });
});

// ===========================================================================
// session.ts — PID liveness + session lock defensive paths
// ===========================================================================
describe('isPidAlive — error classification', () => {
  it('reports the current process as alive', () => {
    assert.equal(isPidAlive(process.pid), true);
  });

  it('reports a non-existent high pid as dead (ESRCH)', () => {
    assert.equal(isPidAlive(findDeadPid()), false);
  });

  it('returns false for an invalid pid (non-ESRCH/EPERM error)', () => {
    // process.kill(NaN, 0) throws a TypeError with no ESRCH/EPERM code -> fallthrough.
    assert.equal(isPidAlive(NaN), false);
  });

  it('treats pid 1 (init) as alive (EPERM for a non-owned process)', () => {
    // As non-root, signalling pid 1 -> EPERM -> alive. As root it is signalable -> alive.
    assert.equal(isPidAlive(1), true);
  });
});

describe('acquireSessionLock / isSessionLocked — defensive paths', () => {
  it('reclaims a lock whose contents cannot be read (dangling symlink)', () => {
    const dir = mkTmp();
    const lockPath = path.join(dir, '.session.lock');
    // Dangling symlink: writeFileSync(wx) -> EEXIST, readFileSync -> ENOENT -> unlink + retry.
    fs.symlinkSync(path.join(dir, 'nope'), lockPath);
    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    assert.equal(fs.readFileSync(lockPath, 'utf-8').trim(), String(process.pid));
    lock.release();
    assert.equal(fs.existsSync(lockPath), false);
  });

  it('release() swallows the error when the reclaimed lock file is already gone', () => {
    const dir = mkTmp();
    fs.mkdirSync(dir, { recursive: true });
    const lockPath = path.join(dir, '.session.lock');
    // Stale lock held by a dead pid -> gets reclaimed, returning a live release().
    fs.writeFileSync(lockPath, String(findDeadPid()));
    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    // Remove the file out from under release(): unlinkSync throws, caught silently.
    fs.unlinkSync(lockPath);
    assert.doesNotThrow(() => lock.release());
  });

  it('isSessionLocked returns false when the lock path is unreadable (a directory)', () => {
    const dir = mkTmp();
    fs.mkdirSync(path.join(dir, '.session.lock'));
    // existsSync true, but readFileSync on a directory throws EISDIR -> catch -> false.
    assert.equal(isSessionLocked(dir), false);
  });

  it('isSessionLocked returns false when there is no lock file', () => {
    assert.equal(isSessionLocked(mkTmp()), false);
  });

  it('a second acquire by a live holder fails with a no-op release', () => {
    const dir = mkTmp();
    const first = acquireSessionLock(dir);
    assert.equal(first.acquired, true);
    const second = acquireSessionLock(dir); // held by this (alive) process
    assert.equal(second.acquired, false);
    assert.equal(second.pid, process.pid);
    assert.doesNotThrow(() => second.release()); // no-op release
    first.release();
  });

  it('rethrows a non-EEXIST write failure (unwritable session dir)', () => {
    if (IS_ROOT) return; // root bypasses directory permissions
    const dir = mkTmp();
    fs.mkdirSync(dir, { recursive: true });
    fs.chmodSync(dir, 0o555); // read+exec, no write -> writeFileSync EACCES (not EEXIST)
    try {
      assert.throws(() => acquireSessionLock(dir), /EACCES/);
    } finally {
      fs.chmodSync(dir, 0o755);
    }
  });
});

// ===========================================================================
// session-memory.ts — message serialization + outer catch
// ===========================================================================
function sseServer(responseText: string): Promise<{ server: http.Server; port: number }> {
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      for await (const _c of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      const escaped = responseText.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
      res.write(`data: {"choices":[{"delta":{"content":"${escaped}"},"index":0}]}\n\n`);
      res.write('data: {"choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });
    server.listen(0, '127.0.0.1', () => resolve({ server, port: (server.address() as { port: number }).port }));
  });
}

describe('SessionMemoryExtractor — content serialization + failure', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    const json = JSON.stringify([{ fact: 'User prefers tabs', category: 'user_preference', confidence: 0.95 }]);
    const m = await sseServer(json);
    server = m.server; port = m.port;
  });
  after(() => server.close());

  it('serializes array content and tolerates missing content', async () => {
    const provider = { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: 'k', model: 'm' } as ProviderConfig;
    const extractor = new SessionMemoryExtractor(provider);
    // JSON.parse guarantees a message object with NO `content` property at runtime
    // (so m.content is genuinely undefined -> JSON.stringify -> undefined -> `?? ''`).
    const noContent = JSON.parse('{"role":"assistant"}') as Message;
    const messages: Message[] = [
      { role: 'user', content: [{ type: 'text', text: 'array content' }] } as unknown as Message,
      noContent,
    ];
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      messages,
    );
    assert.ok(facts.some(f => f.fact === 'User prefers tabs'));
  });

  it('returns [] when the stream throws (outer catch)', async () => {
    const provider = { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: 'k', model: 'm' } as ProviderConfig;
    const extractor = new SessionMemoryExtractor(provider);
    // Inject a stream that rejects -> the for-await throws -> outer catch returns [].
    extractor._streamFn = async function* (): AsyncGenerator<StreamChunk> {
      throw new Error('stream exploded');
      // eslint-disable-next-line no-unreachable
      yield { type: 'content_delta', text: '' } as StreamChunk;
    };
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'hi' }],
    );
    assert.deepEqual(facts, []);
  });

  it('fires the watchdog abort when extraction overruns the timeout', async () => {
    const provider = { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: 'k', model: 'm' } as ProviderConfig;
    const extractor = new SessionMemoryExtractor(provider);
    extractor._timeoutMs = 1; // fire the abort almost immediately
    extractor._streamFn = async function* (): AsyncGenerator<StreamChunk> {
      await new Promise(r => setTimeout(r, 25)); // outlive the 1ms watchdog -> abort callback runs
      yield { type: 'content_delta', text: '[]' } as StreamChunk;
    };
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'hi' }],
    );
    assert.ok(Array.isArray(facts));
  });
});

// ===========================================================================
// memory.ts — instruction loading
// ===========================================================================
describe('MemoryManager — instruction loading', () => {
  it('loadProjectInstructions hits the missing-local-variant catch and stops at .git', () => {
    const cwd = mkTmp();
    fs.mkdirSync(path.join(cwd, '.git'), { recursive: true });
    fs.writeFileSync(path.join(cwd, 'CLAUDE.md'), '# Rules\nDo good work.');
    const out = new MemoryManager(cwd).loadProjectInstructions();
    assert.ok(out.includes('Do good work.'));
  });

  it('loadProjectInstructions includes a present local variant', () => {
    const cwd = mkTmp();
    fs.mkdirSync(path.join(cwd, '.git'), { recursive: true });
    fs.writeFileSync(path.join(cwd, 'CLAUDE.md'), '# Rules');
    fs.writeFileSync(path.join(cwd, 'CLAUDE.local.md'), 'local-secret-note');
    assert.ok(new MemoryManager(cwd).loadProjectInstructions().includes('local-secret-note'));
  });

  it('resolveFileInstructions walks to root and dedups via claimedPaths', () => {
    const cwd = mkTmp();
    fs.writeFileSync(path.join(cwd, 'CLAUDE.md'), 'root-level-instruction');
    fs.mkdirSync(path.join(cwd, 'src', 'deep'), { recursive: true });
    const mgr = new MemoryManager(cwd);
    assert.ok(mgr.resolveFileInstructions('src/deep/file.ts', new Set()).includes('root-level-instruction'));
    const claimed = new Set<string>();
    mgr.resolveFileInstructions('src/deep/file.ts', claimed);
    assert.equal(mgr.resolveFileInstructions('src/deep/other.ts', claimed), '');
  });

  it('resolveFileInstructions tolerates an unreadable instruction file', () => {
    const cwd = mkTmp();
    const f = path.join(cwd, 'CLAUDE.md');
    fs.writeFileSync(f, 'secret');
    fs.chmodSync(f, 0);
    const out = new MemoryManager(cwd).resolveFileInstructions('a.ts', new Set());
    assert.equal(typeof out, 'string');
    if (!IS_ROOT) assert.equal(out, '');
  });

  it('loadUserGlobalInstructions tolerates an unreadable global instruction file', () => {
    const home = mkTmp();
    fs.mkdirSync(path.join(home, '.superinference'), { recursive: true });
    const f = path.join(home, '.superinference', 'CLAUDE.md');
    fs.writeFileSync(f, 'global-secret');
    fs.chmodSync(f, 0);
    const prevHome = process.env.HOME;
    const prevUser = process.env.USERPROFILE;
    process.env.HOME = home;
    process.env.USERPROFILE = home;
    try {
      const out = new MemoryManager(mkTmp()).loadUserGlobalInstructions();
      assert.equal(typeof out, 'string');
      if (!IS_ROOT) assert.equal(out, '');
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      if (prevUser === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = prevUser;
    }
  });

  it('resolveAtImports inlines imports outside code fences but not inside them', () => {
    const base = mkTmp();
    fs.writeFileSync(path.join(base, 'imported.md'), 'IMPORTED_BODY');
    const basePath = path.join(base, 'root.md');
    const content = '@./imported.md\n```\n@./imported.md\n```';
    const out = new MemoryManager(base).resolveAtImports(content, basePath);
    const lines = out.split('\n');
    assert.equal(lines[0], 'IMPORTED_BODY');          // outside fence: inlined
    assert.ok(out.includes('```'));                   // fence preserved
    assert.ok(lines.some(l => l === '@./imported.md')); // inside fence: untouched
  });

  it('resolveAtImports handles ~ imports, directories, cycles and depth limits', () => {
    const base = mkTmp();
    const mgr = new MemoryManager(base);

    // ~ import to a non-existent home path -> left unchanged (home-relative branch).
    const tildeOut = mgr.resolveAtImports('@~/sm-cov-nonexistent-xyz', path.join(base, 'r.md'));
    assert.ok(tildeOut.includes('@~/sm-cov-nonexistent-xyz'));

    // @import pointing at a directory (not a file) -> left unchanged.
    fs.mkdirSync(path.join(base, 'adir'));
    const dirOut = mgr.resolveAtImports('@./adir', path.join(base, 'r.md'));
    assert.ok(dirOut.includes('@./adir'));

    // Circular import a -> b -> a.
    fs.writeFileSync(path.join(base, 'a.md'), '@./b.md');
    fs.writeFileSync(path.join(base, 'b.md'), '@./a.md');
    const cyc = mgr.resolveAtImports('@./a.md', path.join(base, 'root.md'));
    assert.ok(cyc.includes('[circular import:'));

    // Depth guard: a chain deeper than 5 stops inlining.
    for (let i = 0; i < 8; i++) {
      fs.writeFileSync(path.join(base, `d${i}.md`), `start-d${i} @./d${i + 1}.md`);
    }
    fs.writeFileSync(path.join(base, 'd8.md'), 'DEEPEST');
    const deep = mgr.resolveAtImports('@./d0.md', path.join(base, 'root.md'));
    assert.ok(!deep.includes('DEEPEST')); // depth cap prevents reaching the last file
  });

  it('loadProjectInstructions labels parent files and fires the hook callback', () => {
    const parent = mkTmp();
    const child = path.join(parent, 'child');
    fs.mkdirSync(child, { recursive: true });
    fs.writeFileSync(path.join(parent, 'CLAUDE.md'), 'parent-project-rule');
    fs.writeFileSync(path.join(parent, 'CLAUDE.local.md'), 'parent-local-rule');
    const mgr = new MemoryManager(child);
    const events: string[] = [];
    mgr.setHookCallback((ev) => events.push(ev));
    const out = mgr.loadProjectInstructions();
    assert.ok(out.includes('(parent)'));
    assert.ok(out.includes('parent-project-rule'));
    assert.ok(out.includes('parent-local-rule'));
    assert.ok(events.includes('instructionsLoaded'));
  });

  it('loadProjectInstructions tolerates an unreadable project instruction file', () => {
    const cwd = mkTmp();
    fs.mkdirSync(path.join(cwd, '.git'), { recursive: true });
    const f = path.join(cwd, 'CLAUDE.md');
    fs.writeFileSync(f, 'secret-rule');
    fs.chmodSync(f, 0);
    const out = new MemoryManager(cwd).loadProjectInstructions();
    assert.equal(typeof out, 'string');
    if (!IS_ROOT) assert.ok(!out.includes('secret-rule'));
  });

  it('resolveFileInstructions breaks at the filesystem root', () => {
    // cwd = "/" forces the parent === dir break branch at the fs root.
    const out = new MemoryManager('/').resolveFileInstructions('a.ts', new Set());
    assert.equal(typeof out, 'string');
  });

  it('collectInstructionSources gathers managed/project/user levels and stops at .git', async () => {
    const cwd = mkTmp();
    fs.mkdirSync(path.join(cwd, '.git'), { recursive: true });
    fs.writeFileSync(path.join(cwd, 'CLAUDE.md'), 'proj-rule');
    fs.writeFileSync(path.join(cwd, 'CLAUDE.local.md'), 'local-rule');
    const rulesDir = path.join(cwd, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    fs.writeFileSync(path.join(rulesDir, 'r.md'), 'managed-rule');
    writeMemory(cwd, 'mem1', 'a stored memory body', 0);
    const mgr = new MemoryManager(cwd);

    const all = await mgr.collectInstructionSources();
    assert.ok(all.some(s => s.level === 'managed'));
    assert.ok(all.some(s => s.level === 'project' && s.content.includes('proj-rule')));
    assert.ok(all.some(s => s.level === 'user' && s.content.includes('local-rule')));

    const forFile = await mgr.collectInstructionSources('src/x.ts');
    assert.ok(forFile.some(s => s.level === 'managed'));
  });
});

// ===========================================================================
// memory.ts — memory loading age/stale/unreadable + context
// ===========================================================================
function writeMemory(cwd: string, name: string, body: string, ageDays = 0, desc = `desc ${name}`): void {
  const memDir = path.join(cwd, '.superinference', 'memory');
  fs.mkdirSync(memDir, { recursive: true });
  const fp = path.join(memDir, `${name}.md`);
  fs.writeFileSync(fp, `---\nname: ${name}\ndescription: ${desc}\ntype: project\n---\n\n${body}\n`);
  if (ageDays > 0) {
    const t = new Date(Date.now() - ageDays * 86400000);
    fs.utimesSync(fp, t, t);
  }
}

describe('MemoryManager — memory loading + context', () => {
  it('marks >30-day memories STALE and skips unreadable .md entries', () => {
    const cwd = mkTmp();
    writeMemory(cwd, 'oldmem', 'ancient knowledge', 40);
    writeMemory(cwd, 'freshmem', 'recent knowledge', 0);
    fs.mkdirSync(path.join(cwd, '.superinference', 'memory', 'bogus.md')); // readFileSync -> EISDIR
    const mems = new MemoryManager(cwd).loadMemories();
    assert.ok(mems.find(m => m.name === 'oldmem')!.content.startsWith('[STALE'));
    assert.ok(!mems.find(m => m.name === 'freshmem')!.content.startsWith('[STALE'));
    assert.equal(mems.find(m => m.name === 'bogus'), undefined);
  });

  it('getMemoryContext includes project instructions + aged-memory note + drift warning', () => {
    const cwd = mkTmp();
    fs.mkdirSync(path.join(cwd, '.git'), { recursive: true });
    fs.writeFileSync(path.join(cwd, 'CLAUDE.md'), 'project-rule-xyz');
    writeMemory(cwd, 'agedmem', 'three day old fact', 3);
    const ctx = new MemoryManager(cwd).getMemoryContext();
    assert.ok(ctx.includes('## Project Instructions'));
    assert.ok(ctx.includes('project-rule-xyz'));
    assert.ok(ctx.includes('## Stored Memories'));
    assert.ok(ctx.includes('[Note: This memory is'));
    assert.ok(ctx.includes('Memory Drift Warning'));
  });

  it('getMemoryContext for only-fresh memories omits the drift warning', () => {
    const cwd = mkTmp();
    writeMemory(cwd, 'freshonly', 'brand new fact', 0);
    const ctx = new MemoryManager(cwd).getMemoryContext();
    assert.ok(ctx.includes('brand new fact'));
    assert.ok(!ctx.includes('Memory Drift Warning'));
  });

  it('getMemoryContext includes team memories section', () => {
    const cwd = mkTmp();
    const teamDir = path.join(cwd, '.superinference', 'memory', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 't.md'), '---\nname: t\n---\n\nshared-team-fact');
    const ctx = new MemoryManager(cwd).getMemoryContext();
    assert.ok(ctx.includes('## Team Memories'));
    assert.ok(ctx.includes('shared-team-fact'));
  });

  it('loadTeamMemories returns [] when a team entry is unreadable', () => {
    const cwd = mkTmp();
    const teamDir = path.join(cwd, '.superinference', 'memory', 'team');
    fs.mkdirSync(path.join(teamDir, 'dir.md'), { recursive: true }); // readFileSync -> EISDIR -> catch
    assert.deepEqual(new MemoryManager(cwd).loadTeamMemories(), []);
  });

  it('computes freshness labels across the age bands', () => {
    const cwd = mkTmp();
    writeMemory(cwd, 'm-today', 'b', 0);
    writeMemory(cwd, 'm-yesterday', 'b', 1);
    writeMemory(cwd, 'm-days', 'b', 4);
    writeMemory(cwd, 'm-weeks', 'b', 14);
    writeMemory(cwd, 'm-months', 'b', 70);
    const byName = Object.fromEntries(new MemoryManager(cwd).loadMemories().map(m => [m.name, m.freshness]));
    assert.equal(byName['m-today'], 'today');
    assert.equal(byName['m-yesterday'], 'yesterday');
    assert.equal(byName['m-days'], '4 days ago');
    assert.equal(byName['m-weeks'], '2 weeks ago');
    assert.equal(byName['m-months'], '2 months ago');
  });
});

// ===========================================================================
// memory.ts — relevance scoring
// ===========================================================================
describe('MemoryManager — relevance scoring', () => {
  it('scores every memory, ranks the relevant one first, handles token-less + absent tokens', () => {
    const cwd = mkTmp();
    writeMemory(cwd, 'm1', 'the database migration scripts live here', 0, 'database migration');
    writeMemory(cwd, 'm2', 'login and authentication flows', 0, 'authentication');
    writeMemory(cwd, 'm3', 'deployment pipeline notes', 0, 'deploy');
    writeMemory(cwd, 'm4', 'frontend widget styling details', 0, 'ui');
    writeMemory(cwd, 'm5', 'structured logging configuration', 0, 'logging');
    writeMemory(cwd, 'm6', 'a b c d e', 0, 'a b'); // token-less -> early return 0
    const picks = new MemoryManager(cwd).selectRelevantMemories('database migration', 5);
    assert.equal(picks.length, 5);
    assert.equal(picks[0].name, 'm1');
  });

  it('returns all when count <= maxResults, and first-N for a token-less query', () => {
    const cwd = mkTmp();
    writeMemory(cwd, 'only', 'solo body', 0);
    assert.equal(new MemoryManager(cwd).selectRelevantMemories('anything', 5).length, 1);

    const cwd2 = mkTmp();
    for (let i = 0; i < 6; i++) writeMemory(cwd2, `q${i}`, `body body ${i}`, 0);
    assert.equal(new MemoryManager(cwd2).selectRelevantMemories('!! ?? ..', 3).length, 3);
  });

  it('scores memories that lack a description field', () => {
    const cwd = mkTmp();
    const memDir = path.join(cwd, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    // One memory with NO description frontmatter -> description-fallback branch.
    fs.writeFileSync(path.join(memDir, 'nodesc.md'), '---\nname: nodesc\ntype: project\n---\n\ndatabase tuning notes');
    for (let i = 0; i < 5; i++) writeMemory(cwd, `w${i}`, `filler body ${i}`, 0);
    const picks = new MemoryManager(cwd).selectRelevantMemories('database tuning', 3);
    assert.equal(picks.length, 3);
    assert.equal(picks[0].name, 'nodesc');
  });
});

// ===========================================================================
// memory.ts — session memory + rules + index
// ===========================================================================
describe('MemoryManager — session memory, rules, index', () => {
  it('saveSessionMemory no-ops on empty facts and round-trips real facts', () => {
    const cwd = mkTmp();
    const mgr = new MemoryManager(cwd);
    mgr.saveSessionMemory('s-empty', []);
    assert.equal(fs.existsSync(path.join(mgr.getMemoryDir(), 'session-s_empty.md')), false);

    const facts: SessionFact[] = [
      { fact: 'chose postgres', category: 'decision', confidence: 0.9 },
      { fact: 'fixed null deref', category: 'error_fix', confidence: 0.8 },
      { fact: 'another decision', category: 'decision', confidence: 0.75 },
    ];
    mgr.saveSessionMemory('abc/123', facts);
    const loaded = mgr.loadSessionMemory('abc/123');
    assert.equal(loaded.length, 3);
    assert.ok(loaded.some(f => f.fact === 'chose postgres' && f.category === 'decision'));
    assert.ok(loaded.some(f => f.category === 'error_fix'));
  });

  it('loadSessionMemory returns [] for a missing file', () => {
    assert.deepEqual(new MemoryManager(mkTmp()).loadSessionMemory('does-not-exist'), []);
  });

  it('loadRulesForFile respects matching/non-matching paths frontmatter', async () => {
    const cwd = mkTmp();
    const rulesDir = path.join(cwd, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    fs.writeFileSync(path.join(rulesDir, 'ts-only.md'), '---\npaths: src/*.ts\n---\nTypeScript rule body');
    fs.writeFileSync(path.join(rulesDir, 'py-only.md'), '---\npaths: src/*.py\n---\nPython rule body');
    fs.writeFileSync(path.join(rulesDir, 'always.md'), 'Unconditional rule');
    const sources = await new MemoryManager(cwd).loadRulesForFile(cwd, 'src/app.ts');
    const bodies = sources.map(s => s.content);
    assert.ok(bodies.some(b => b.includes('TypeScript rule body')));
    assert.ok(bodies.some(b => b.includes('Unconditional rule')));
    assert.ok(!bodies.some(b => b.includes('Python rule body')));
    assert.ok(sources.every(s => s.level === 'managed'));
  });

  it('loadRulesDirectory / loadRulesForFile skip subdirectories and non-md/txt files', async () => {
    const cwd = mkTmp();
    const rulesDir = path.join(cwd, '.superinference', 'rules');
    fs.mkdirSync(path.join(rulesDir, 'nested'), { recursive: true }); // a directory entry -> skipped
    fs.writeFileSync(path.join(rulesDir, 'notes.json'), 'ignored');    // non md/txt -> skipped
    fs.writeFileSync(path.join(rulesDir, 'real.md'), 'real-rule-body');
    fs.writeFileSync(path.join(rulesDir, 'also.txt'), 'txt-rule-body');
    const mgr = new MemoryManager(cwd);
    const dirRules = await mgr.loadRulesDirectory(cwd);
    assert.ok(dirRules.some(r => r.includes('real-rule-body')));
    assert.ok(dirRules.some(r => r.includes('txt-rule-body')));
    assert.ok(!dirRules.some(r => r.includes('ignored')));
    const fileRules = await mgr.loadRulesForFile(cwd, 'src/x.ts');
    assert.ok(fileRules.some(s => s.content.includes('real-rule-body')));
  });

  it('loadRulesDirectory / loadRulesForFile tolerate an unreadable rule file', async () => {
    const cwd = mkTmp();
    const rulesDir = path.join(cwd, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    const f = path.join(rulesDir, 'blocked.md');
    fs.writeFileSync(f, 'rule body');
    fs.chmodSync(f, 0);
    const mgr = new MemoryManager(cwd);
    const dirRules = await mgr.loadRulesDirectory(cwd);
    const fileRules = await mgr.loadRulesForFile(cwd, 'src/x.ts');
    assert.ok(Array.isArray(dirRules));
    assert.ok(Array.isArray(fileRules));
    if (!IS_ROOT) {
      assert.deepEqual(dirRules, []);
      assert.deepEqual(fileRules, []);
    }
  });

  it('MEMORY.md index add/read/remove round-trip and tolerate a bad index path', async () => {
    const cwd = mkTmp();
    const mgr = new MemoryManager(cwd);
    await mgr.addToMemoryIndex(cwd, '- [topic](topic.md) — a topic');
    await mgr.addToMemoryIndex(cwd, '- [topic](topic.md) — a topic'); // dedup, no dup line
    let index = await mgr.readMemoryIndex(cwd);
    assert.equal(index.filter(l => l.includes('topic.md')).length, 1);
    await mgr.removeFromMemoryIndex(cwd, 'topic.md');
    index = await mgr.readMemoryIndex(cwd);
    assert.equal(index.filter(l => l.includes('topic.md')).length, 0);

    // MEMORY.md as a directory -> writeFile throws -> best-effort catch, no throw.
    const cwd2 = mkTmp();
    fs.mkdirSync(path.join(cwd2, '.superinference', 'memory', 'MEMORY.md'), { recursive: true });
    await assert.doesNotReject(() => new MemoryManager(cwd2).addToMemoryIndex(cwd2, '- [x](x.md)'));
  });
});
