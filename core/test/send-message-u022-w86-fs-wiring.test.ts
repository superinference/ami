/**
 * U022 (w86) — wiring of the cross-process file-backed mailbox into send_message.
 *
 * Proves the file transport ({@link ../src/tools/mailbox-fs}) is reached end to end
 * through the REAL send/poll path, and only when opted in:
 *   - OFF by default: with AMI_MAILBOX_FS unset, no inbox file is written and
 *     pollMailbox behaves exactly as before (regression lock).
 *   - ON + team established: a plain-text send persists a durable copy to the
 *     recipient's inbox file; pollMailbox merges a message written by ANOTHER
 *     process and stamps it `crossSession` (Claude's <cross-session-message>).
 *   - Dedup: a same-process send is delivered once (not duplicated via the file).
 *   - Discovery: a cross-process peer is a known/reachable recipient by bare name
 *     (Claude's ListPeers).
 *
 * Red-green: revert the enqueue/pollMailbox/known-recipients hooks (or the
 * crossSession stamp) and the ON-path assertions fail; the OFF-path lock proves the
 * default behavior is unchanged.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  sendMessageTool,
  pollMailbox,
  resetAllMailboxes,
  listCrossSessionPeers,
} from '../src/tools/send-message';
import { restoreTeam, resetTeam } from '../src/tools/team';
import { appendToInbox, inboxFilePath, type FsMailboxLocation } from '../src/tools/mailbox-fs';
import type { ToolContext } from '../src/types';

let tmp: string;
let teamDir: string;
let loc: FsMailboxLocation;

function ctx(): ToolContext {
  return { cwd: tmp, abortSignal: new AbortController().signal } as unknown as ToolContext;
}

function establishTeam(): void {
  teamDir = path.join(tmp, '.ami', 'teams', 'alpha');
  fs.mkdirSync(teamDir, { recursive: true });
  restoreTeam(
    {
      name: 'alpha',
      leadAgentId: 'lead-1',
      members: [{ name: 'lead', agentId: 'lead-1', joinedAt: new Date().toISOString() }],
      createdAt: new Date().toISOString(),
    },
    teamDir,
  );
  loc = { teamsRoot: path.join(tmp, '.ami', 'teams'), teamName: 'alpha' };
}

describe('U022 w86 — file-backed mailbox wiring', () => {
  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mbwire-'));
    delete process.env.AMI_MAILBOX_FS;
    resetTeam();
    resetAllMailboxes();
  });
  afterEach(() => {
    delete process.env.AMI_MAILBOX_FS;
    resetTeam();
    resetAllMailboxes();
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best-effort */ }
  });

  it('OFF by default: a send writes no inbox file and poll is unchanged', async () => {
    establishTeam(); // team exists, but the feature flag is unset
    await sendMessageTool.execute({ to: 'worker', content: 'hi there' }, ctx());
    assert.equal(fs.existsSync(inboxFilePath(loc, 'worker')), false);
    assert.deepEqual(listCrossSessionPeers(), []);
    const drained = pollMailbox('worker');
    assert.equal(drained.length, 1);
    assert.equal(drained[0].crossSession, undefined);
  });

  it('disabled when no team directory is established even if the flag is set', async () => {
    process.env.AMI_MAILBOX_FS = '1';
    // No restoreTeam → getCurrentTeamDir() is null → transport inactive.
    await sendMessageTool.execute({ to: 'worker', content: 'no team yet' }, ctx());
    assert.deepEqual(listCrossSessionPeers(), []);
    const drained = pollMailbox('worker');
    assert.equal(drained.length, 1);
    assert.equal(drained[0].crossSession, undefined);
  });

  it('ON: a plain-text send persists a durable copy to the recipient inbox file', async () => {
    process.env.AMI_MAILBOX_FS = '1';
    establishTeam();
    await sendMessageTool.execute({ to: 'worker', content: 'ship the fix' }, ctx());
    const file = inboxFilePath(loc, 'worker');
    assert.ok(fs.existsSync(file), 'durable inbox file written');
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n').map(l => JSON.parse(l));
    assert.equal(lines.length, 1);
    assert.equal(lines[0].content, 'ship the fix');
    assert.equal(lines[0].from, 'main');
  });

  it('ON: pollMailbox merges a message written by another process and marks it crossSession', () => {
    process.env.AMI_MAILBOX_FS = '1';
    establishTeam();
    // Simulate a SEPARATE process delivering to this agent's inbox.
    appendToInbox(loc, 'worker', { id: 'remote-1', from: 'reviewer', to: 'worker', content: 'please rebase', timestamp: Date.now() });
    const drained = pollMailbox('worker');
    assert.equal(drained.length, 1);
    assert.equal(drained[0].content, 'please rebase');
    assert.equal(drained[0].crossSession, true);
    assert.ok(drained[0].source, 'source is classified on a cross-session message');
    // Drained → the inbox file is cleared.
    assert.equal(fs.existsSync(inboxFilePath(loc, 'worker')), false);
  });

  it('ON: a same-process send is delivered exactly once (no file duplicate)', async () => {
    process.env.AMI_MAILBOX_FS = '1';
    establishTeam();
    await sendMessageTool.execute({ to: 'worker', content: 'only once' }, ctx());
    const drained = pollMailbox('worker');
    assert.equal(drained.length, 1, 'memory + file copy deduped to one');
    assert.equal(drained[0].content, 'only once');
  });

  it('ON: cross-process peers are discoverable and reachable by bare name', async () => {
    process.env.AMI_MAILBOX_FS = '1';
    establishTeam();
    appendToInbox(loc, 'reviewer', { id: 'r1', from: 'main', to: 'reviewer', content: 'hi', timestamp: Date.now() });
    // Discovery surface (Claude's ListPeers).
    assert.deepEqual(listCrossSessionPeers(), ['reviewer']);
    // A send to the cross-process peer is NOT flagged as an unknown recipient.
    const toPeer = await sendMessageTool.execute({ to: 'reviewer', content: 'rebase please' }, ctx());
    assert.ok(!/not a known\/live agent/.test(toPeer.output), 'cross-process peer is reachable');
    // A send to a genuinely unknown name lists the cross-process peer among known agents.
    const toGhost = await sendMessageTool.execute({ to: 'ghost', content: 'who are you' }, ctx());
    assert.ok(/not a known\/live agent/.test(toGhost.output));
    assert.ok(/reviewer/.test(toGhost.output), 'known-agents affordance includes the cross-process peer');
  });

  it('ON: a predicate poll merges only matching cross-session messages', () => {
    process.env.AMI_MAILBOX_FS = '1';
    establishTeam();
    appendToInbox(loc, 'worker', { id: 'ctrl', from: 'main', to: 'worker', content: '{"type":"shutdown_request","requestId":"x"}', timestamp: Date.now() });
    appendToInbox(loc, 'worker', { id: 'chat', from: 'peer', to: 'worker', content: 'hi', timestamp: Date.now() });
    const control = pollMailbox('worker', (m) => m.content.startsWith('{'));
    assert.deepEqual(control.map(m => m.id), ['ctrl']);
    // The chat message stays queued in the file for a later unfiltered drain.
    const rest = pollMailbox('worker');
    assert.deepEqual(rest.map(m => m.id), ['chat']);
  });
});
