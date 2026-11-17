import * as fs from 'fs';
import * as path from 'path';
import { withFileLockSync } from '../utils/file-lock';
import { MAX_MAILBOX, type AgentMessage } from './send-message';

/**
 * File-backed, cross-process teammate mailbox transport.
 *
 * AMI's primary teammate mailbox is an in-memory `Map` keyed by agent name
 * ({@link ./send-message}). That is correct and fast for teammates spawned
 * IN-PROCESS (the engine's own sub-agents), but it cannot reach a teammate running
 * as a SEPARATE OS process — a swarm where each agent is its own `ami` process
 * sharing one project. Claude Code solves this with a FILE-based mailbox
 * (`utils/swarm/backends/PaneBackendExecutor.ts` → `writeToMailbox`,
 * `utils/teammateMailbox.ts`): every spawn/sendMessage/terminate routes through a
 * per-`(team, agent)` inbox file, serialized across processes with a lock that
 * re-reads the inbox after acquiring so concurrent writers never lose an append.
 *
 * This module is the AMI equivalent: an OPT-IN (`AMI_MAILBOX_FS`) durable transport
 * that falls back to the in-memory `Map` when disabled. It deliberately does NOT
 * re-implement the lock — it reuses the ONE canonical cross-process primitive
 * ({@link withFileLockSync}, `../utils/file-lock`), which already provides
 * retry/backoff, PID-based stale-lock stealing, and guaranteed release in a
 * `finally` (covering the "concurrency-safe, crash-safe, guaranteed lock release"
 * requirement). Each inbox is a JSONL file under
 * `<teamsRoot>/<teamName>/inboxes/<agent>.jsonl`, so inboxes are namespaced PER TEAM
 * (parity with Claude's `{team}/{agent}` keying), and a `default` team name is used
 * when none is supplied.
 */

/**
 * The on-disk location of a team's inbox tree. `teamsRoot` is the shared
 * `.ami/teams` directory (same root `team.ts` persists team files under);
 * `teamName` namespaces the inboxes so two teams in one project never collide.
 */
export interface FsMailboxLocation {
  /** The `.ami/teams` directory every team in this project shares. */
  teamsRoot: string;
  /** The team whose inboxes are addressed; defaults to {@link DEFAULT_TEAM_NAME}. */
  teamName?: string;
}

/** Team name used when a caller does not specify one (parity: Claude's `'default'`). */
export const DEFAULT_TEAM_NAME = 'default';

/** Resolve the effective team name for a location, applying the default. */
function teamOf(loc: FsMailboxLocation): string {
  const t = loc.teamName && loc.teamName.trim() ? loc.teamName.trim() : DEFAULT_TEAM_NAME;
  return t;
}

/** Absolute path to a team's `inboxes/` directory. */
export function inboxDir(loc: FsMailboxLocation): string {
  return path.join(loc.teamsRoot, teamOf(loc), 'inboxes');
}

/**
 * Absolute path to one agent's inbox file. The agent name is URL-encoded so an
 * agentId containing `@`/`/` (the `name@team` form) or any other character can
 * never escape the inboxes directory or collide — the encoding is reversible, so
 * {@link listInboxPeers} recovers the original name.
 */
export function inboxFilePath(loc: FsMailboxLocation, agent: string): string {
  return path.join(inboxDir(loc), `${encodeURIComponent(agent)}.jsonl`);
}

/** Serialize one message to its durable JSONL record (drops receive-side-only fields). */
function toRecord(msg: AgentMessage): AgentMessage {
  const { crossSession: _drop, ...rest } = msg;
  void _drop;
  return rest;
}

/** Parse an inbox file into messages, skipping blank/corrupt lines. */
function readRecords(file: string): AgentMessage[] {
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch {
    return [];
  }
  const out: AgentMessage[] = [];
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (!t) continue;
    try {
      const o = JSON.parse(t) as AgentMessage;
      if (o && typeof o === 'object' && typeof o.from === 'string' && typeof o.content === 'string') {
        out.push(o);
      }
    } catch {
      // Skip a torn/partial line — a crash mid-write can leave one; the rest are intact.
    }
  }
  return out;
}

/** Atomically replace an inbox file: write a temp sibling, then rename over it. */
function writeAtomic(file: string, records: AgentMessage[]): void {
  const body = records.map((r) => JSON.stringify(r)).join('\n') + (records.length ? '\n' : '');
  const tmp = `${file}.tmp-${process.pid}-${Math.random().toString(36).slice(2)}`;
  fs.writeFileSync(tmp, body);
  fs.renameSync(tmp, file);
}

/**
 * Durably append `msg` to `agent`'s cross-process inbox. Serialized with the
 * canonical file lock: inside the critical section the inbox is RE-READ (so a
 * concurrent writer's append from another process is never clobbered), the message
 * is appended (deduped by id so a retry cannot double-deliver), the queue is capped
 * at {@link MAX_MAILBOX}, and the file is replaced atomically. The lock is released
 * in the primitive's `finally`, so a crash cannot wedge the inbox.
 */
export function appendToInbox(loc: FsMailboxLocation, agent: string, msg: AgentMessage): void {
  const file = inboxFilePath(loc, agent);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  withFileLockSync(file, () => {
    const existing = readRecords(file);
    if (msg.id && existing.some((r) => r.id === msg.id)) return; // idempotent retry
    existing.push(toRecord(msg));
    const capped = existing.length > MAX_MAILBOX ? existing.slice(existing.length - MAX_MAILBOX) : existing;
    writeAtomic(file, capped);
  });
}

/**
 * Take the cross-process messages destined for `agent` that are NOT already present
 * in its in-memory queue (`knownIds` — same-process copies an in-session send wrote
 * to BOTH transports), optionally filtered by `fn`. Messages that match (or all,
 * when no `fn`) are removed from the inbox and returned; non-matching messages stay
 * queued for a later selective drain. Same-process duplicate records are always
 * removed (the in-memory copy will be delivered instead), so the model never sees a
 * message twice. This is the merge leg that lets {@link ./send-message}'s
 * `pollMailbox` surface cross-process traffic alongside in-memory traffic.
 */
export function takeFsInbox(
  loc: FsMailboxLocation,
  agent: string,
  knownIds: Set<string>,
  fn?: (msg: AgentMessage) => boolean,
): AgentMessage[] {
  const file = inboxFilePath(loc, agent);
  if (!fs.existsSync(file)) return [];
  return withFileLockSync(file, () => {
    const records = readRecords(file);
    if (records.length === 0) return [];
    const taken: AgentMessage[] = [];
    const keep: AgentMessage[] = [];
    for (const r of records) {
      // A same-process duplicate (its id is already queued in memory) is dropped
      // from the file so it is delivered once, from the in-memory queue.
      if (r.id && knownIds.has(r.id)) continue;
      if (!fn || fn(r)) taken.push(r);
      else keep.push(r);
    }
    if (keep.length) writeAtomic(file, keep);
    else { try { fs.unlinkSync(file); } catch { /* raced */ } }
    return taken;
  });
}

/**
 * Discover the names of every teammate reachable over the cross-process inbox tree
 * — i.e. every agent that has (or had) an inbox file under this team. Backs the
 * "known agents" affordance so a cross-process peer is recognized as a valid
 * recipient by bare name (parity with Claude's `ListPeers` peer discovery). Names
 * are decoded back from their URL-encoded filenames.
 */
export function listInboxPeers(loc: FsMailboxLocation): string[] {
  const dir = inboxDir(loc);
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch {
    return [];
  }
  const names: string[] = [];
  for (const e of entries) {
    if (!e.endsWith('.jsonl')) continue; // ignore .lock / .tmp-* siblings
    try {
      names.push(decodeURIComponent(e.slice(0, -'.jsonl'.length)));
    } catch {
      // Undecodable filename — not one of ours; skip.
    }
  }
  return names.sort();
}
