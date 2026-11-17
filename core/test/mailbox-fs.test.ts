/**
 * U022 (w86) — file-backed, cross-process teammate mailbox transport.
 *
 * `core/src/tools/mailbox-fs.ts` is the durable transport that lets a teammate
 * running as a SEPARATE OS process of the same team receive messages (parity with
 * Claude Code's file-based `utils/teammateMailbox.ts` / `PaneBackendExecutor`
 * `writeToMailbox`). It covers four confirmed gaps:
 *   - cross-process file-based mailbox (append/drain over a shared JSONL inbox),
 *   - per-team namespacing (`<teamsRoot>/<teamName>/inboxes/<agent>.jsonl`, default team),
 *   - concurrency/crash safety via the canonical cross-process lock (re-read after
 *     lock + atomic rename + guaranteed release — reused, not re-implemented),
 *   - peer discovery (`listInboxPeers`, Claude's ListPeers).
 *
 * These are pure-module tests against a temp inbox tree (no team singleton needed).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  appendToInbox,
  takeFsInbox,
  listInboxPeers,
  inboxFilePath,
  inboxDir,
  DEFAULT_TEAM_NAME,
  type FsMailboxLocation,
} from '../src/tools/mailbox-fs';
import { MAX_MAILBOX, type AgentMessage } from '../src/tools/send-message';

let tmp: string;
let loc: FsMailboxLocation;

function msg(over: Partial<AgentMessage> = {}): AgentMessage {
  return { id: over.id, from: 'main', to: 'worker', content: 'hello', timestamp: Date.now(), ...over };
}

describe('mailbox-fs — cross-process file-backed teammate mailbox', () => {
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mbfs-'));
    loc = { teamsRoot: path.join(tmp, '.ami', 'teams'), teamName: 'alpha' };
  });
  afterEach(() => {
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  it('per-team namespacing: inbox path is <teamsRoot>/<teamName>/inboxes/<agent>.jsonl', () => {
    const p = inboxFilePath(loc, 'worker');
    assert.equal(p, path.join(loc.teamsRoot, 'alpha', 'inboxes', 'worker.jsonl'));
    assert.equal(inboxDir(loc), path.join(loc.teamsRoot, 'alpha', 'inboxes'));
  });

  it('defaults the team name when none is supplied', () => {
    const noTeam: FsMailboxLocation = { teamsRoot: loc.teamsRoot };
    assert.equal(inboxFilePath(noTeam, 'worker'), path.join(loc.teamsRoot, DEFAULT_TEAM_NAME, 'inboxes', 'worker.jsonl'));
    const blank: FsMailboxLocation = { teamsRoot: loc.teamsRoot, teamName: '   ' };
    assert.equal(inboxDir(blank), path.join(loc.teamsRoot, DEFAULT_TEAM_NAME, 'inboxes'));
  });

  it('two different teams do not share an inbox (namespace isolation)', () => {
    const beta: FsMailboxLocation = { teamsRoot: loc.teamsRoot, teamName: 'beta' };
    appendToInbox(loc, 'worker', msg({ id: 'a', content: 'for-alpha' }));
    appendToInbox(beta, 'worker', msg({ id: 'b', content: 'for-beta' }));
    const a = takeFsInbox(loc, 'worker', new Set());
    const b = takeFsInbox(beta, 'worker', new Set());
    assert.deepEqual(a.map(m => m.content), ['for-alpha']);
    assert.deepEqual(b.map(m => m.content), ['for-beta']);
  });

  it('append then take roundtrips the message and clears the inbox', () => {
    appendToInbox(loc, 'worker', msg({ id: 'm1', content: 'ship it' }));
    assert.ok(fs.existsSync(inboxFilePath(loc, 'worker')));
    const taken = takeFsInbox(loc, 'worker', new Set());
    assert.equal(taken.length, 1);
    assert.equal(taken[0].content, 'ship it');
    // Fully drained → file removed, second take is empty.
    assert.equal(fs.existsSync(inboxFilePath(loc, 'worker')), false);
    assert.deepEqual(takeFsInbox(loc, 'worker', new Set()), []);
  });

  it('re-reads after the lock so sequential appends accumulate (no clobber)', () => {
    // A "write-only" (non-re-reading) implementation would lose the first message.
    appendToInbox(loc, 'worker', msg({ id: 'm1', content: 'first' }));
    appendToInbox(loc, 'worker', msg({ id: 'm2', content: 'second' }));
    appendToInbox(loc, 'worker', msg({ id: 'm3', content: 'third' }));
    const taken = takeFsInbox(loc, 'worker', new Set());
    assert.deepEqual(taken.map(m => m.content), ['first', 'second', 'third']);
  });

  it('append is idempotent by id (a retry does not double-deliver)', () => {
    appendToInbox(loc, 'worker', msg({ id: 'dup', content: 'once' }));
    appendToInbox(loc, 'worker', msg({ id: 'dup', content: 'once' }));
    const taken = takeFsInbox(loc, 'worker', new Set());
    assert.equal(taken.length, 1);
  });

  it('caps retention at MAX_MAILBOX, dropping the oldest', () => {
    for (let i = 0; i < MAX_MAILBOX + 5; i++) {
      appendToInbox(loc, 'worker', msg({ id: `m${i}`, content: `c${i}` }));
    }
    const taken = takeFsInbox(loc, 'worker', new Set());
    assert.equal(taken.length, MAX_MAILBOX);
    // Oldest five were dropped; the newest survive.
    assert.equal(taken[0].content, 'c5');
    assert.equal(taken[taken.length - 1].content, `c${MAX_MAILBOX + 4}`);
  });

  it('takeFsInbox drops same-process duplicates (id already known in memory)', () => {
    appendToInbox(loc, 'worker', msg({ id: 'local', content: 'already-in-memory' }));
    appendToInbox(loc, 'worker', msg({ id: 'remote', content: 'from-another-process' }));
    const taken = takeFsInbox(loc, 'worker', new Set(['local']));
    assert.deepEqual(taken.map(m => m.content), ['from-another-process']);
    // The dropped duplicate is NOT redelivered later either.
    assert.deepEqual(takeFsInbox(loc, 'worker', new Set()), []);
  });

  it('takeFsInbox with a predicate keeps the non-matching messages queued', () => {
    appendToInbox(loc, 'worker', msg({ id: 'ctrl', content: '{"type":"shutdown_request"}' }));
    appendToInbox(loc, 'worker', msg({ id: 'chat', content: 'just chatting' }));
    const control = takeFsInbox(loc, 'worker', new Set(), (m) => m.content.startsWith('{'));
    assert.deepEqual(control.map(m => m.id), ['ctrl']);
    // The non-matching message stays and is drained on a later unfiltered take.
    const rest = takeFsInbox(loc, 'worker', new Set());
    assert.deepEqual(rest.map(m => m.id), ['chat']);
  });

  it('encodes agent ids safely (no path escape for name@team / separators)', () => {
    const p = inboxFilePath(loc, 'worker@alpha');
    assert.ok(p.startsWith(inboxDir(loc) + path.sep));
    assert.ok(!p.includes('@'), 'agent id is url-encoded in the filename');
    appendToInbox(loc, 'worker@alpha', msg({ id: 'x', to: 'worker@alpha' }));
    assert.equal(takeFsInbox(loc, 'worker@alpha', new Set()).length, 1);
    // A traversal attempt resolves inside the inboxes dir, never above it.
    const evil = inboxFilePath(loc, '../../escape');
    assert.ok(evil.startsWith(inboxDir(loc) + path.sep));
  });

  it('listInboxPeers enumerates decoded names, ignoring lock/tmp siblings', () => {
    appendToInbox(loc, 'worker', msg());
    appendToInbox(loc, 'reviewer@alpha', msg({ to: 'reviewer@alpha' }));
    // Simulate stray non-inbox files that must be ignored.
    fs.writeFileSync(path.join(inboxDir(loc), 'worker.jsonl.lock'), '1');
    fs.writeFileSync(path.join(inboxDir(loc), 'x.tmp-123-abc'), 'partial');
    const peers = listInboxPeers(loc);
    assert.deepEqual(peers, ['reviewer@alpha', 'worker']);
  });

  it('listInboxPeers is empty when the team has no inbox directory yet', () => {
    assert.deepEqual(listInboxPeers(loc), []);
  });

  it('skips a torn/corrupt JSONL line instead of throwing', () => {
    appendToInbox(loc, 'worker', msg({ id: 'ok', content: 'valid' }));
    // Append a garbage line directly (as a crash mid-write would leave).
    fs.appendFileSync(inboxFilePath(loc, 'worker'), '{not json\n');
    const taken = takeFsInbox(loc, 'worker', new Set());
    assert.deepEqual(taken.map(m => m.content), ['valid']);
  });

  it('leaves no .tmp artifacts behind after an atomic write', () => {
    appendToInbox(loc, 'worker', msg({ id: 'm1' }));
    appendToInbox(loc, 'worker', msg({ id: 'm2' }));
    const leftovers = fs.readdirSync(inboxDir(loc)).filter(f => f.includes('.tmp-'));
    assert.deepEqual(leftovers, []);
  });
});
