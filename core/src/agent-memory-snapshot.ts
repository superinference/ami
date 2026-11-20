/**
 * Per-agent memory snapshot staleness detection (parity: the reference's
 * `tools/AgentTool/agentMemorySnapshot.ts`).
 *
 * A project can commit a SNAPSHOT of a subagent's persistent memory
 * (`.ami/agent-memory-snapshots/<agentType>/`). Each clone of the repo then
 * detects whether that snapshot is new (never initialized) or newer than what it
 * last synced, and can initialize / prompt-to-update its own local agent memory
 * accordingly. Two tiny metadata files drive the decision:
 *
 *   - `snapshot.json`          → `{ updatedAt }`   (in the committed snapshot dir)
 *   - `.snapshot-synced.json`  → `{ syncedFrom }`  (in the local agent-memory dir)
 *
 * All reads are corruption-tolerant: a missing, malformed, or schema-mismatched
 * file degrades cleanly to `null` (treated as "no snapshot" / "needs sync")
 * rather than throwing.
 *
 * This is the self-contained core building block; wiring that OFFERS the
 * initialize / update to the user is upper-layer orchestration that composes it.
 */

import * as fs from 'fs';
import * as path from 'path';
import { canonicalGitRoot } from './reference-journal';
import { getAgentMemoryDir, sanitizeAgentTypeForPath } from './memory';
import { log as coreLog, logForDiagnosticsNoPII } from './logger';
import type { AgentMemoryScope } from './skills';

/** Debug/introspection component tag for the agent-memory snapshot lifecycle. */
const SNAPSHOT_LOG_COMPONENT = 'agent-memory-snapshot';

/** Directory (relative to the repo root) holding committed per-agent snapshots. */
const SNAPSHOT_BASE = 'agent-memory-snapshots';
/** Metadata file written into the committed snapshot directory. */
export const SNAPSHOT_META_FILENAME = 'snapshot.json';
/** Metadata file written into the LOCAL agent-memory directory after a sync. */
export const SYNCED_META_FILENAME = '.snapshot-synced.json';

/** Timestamp metadata committed alongside a project snapshot. */
export interface SnapshotMeta {
  updatedAt: string;
}

/** Marker recording which snapshot timestamp the local memory last synced from. */
export interface SyncedMeta {
  syncedFrom: string;
}

/** Validate an untrusted value as {@link SnapshotMeta} (non-empty `updatedAt`). */
export function validateSnapshotMeta(value: unknown): SnapshotMeta | null {
  if (value && typeof value === 'object') {
    const updatedAt = (value as Record<string, unknown>).updatedAt;
    if (typeof updatedAt === 'string' && updatedAt.length > 0) return { updatedAt };
  }
  return null;
}

/** Validate an untrusted value as {@link SyncedMeta} (non-empty `syncedFrom`). */
export function validateSyncedMeta(value: unknown): SyncedMeta | null {
  if (value && typeof value === 'object') {
    const syncedFrom = (value as Record<string, unknown>).syncedFrom;
    if (typeof syncedFrom === 'string' && syncedFrom.length > 0) return { syncedFrom };
  }
  return null;
}

/**
 * Corruption-tolerant, schema-validated JSON read. Returns the validated value,
 * or `null` on ANY failure (missing file, malformed JSON, schema mismatch) — so
 * a partial / corrupt metadata file degrades gracefully instead of throwing.
 * Parity with the reference's `readJsonFile(path, schema)`.
 */
export function readJsonFile<T>(filePath: string, validate: (value: unknown) => T | null): T | null {
  try {
    return validate(JSON.parse(fs.readFileSync(filePath, 'utf-8')));
  } catch {
    return null;
  }
}

/** Committed snapshot directory for an agent type, keyed on the canonical git root. */
export function getSnapshotDirForAgent(agentType: string, cwd: string): string {
  return path.join(canonicalGitRoot(cwd), '.ami', SNAPSHOT_BASE, sanitizeAgentTypeForPath(agentType));
}

function getSnapshotJsonPath(agentType: string, cwd: string): string {
  return path.join(getSnapshotDirForAgent(agentType, cwd), SNAPSHOT_META_FILENAME);
}

function getSyncedJsonPath(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv,
): string {
  return path.join(getAgentMemoryDir(agentType, scope, cwd, env), SYNCED_META_FILENAME);
}

/** The three outcomes of a snapshot drift check. */
export const SNAPSHOT_ACTIONS = ['none', 'initialize', 'prompt-update'] as const;
export type SnapshotAction = (typeof SNAPSHOT_ACTIONS)[number];

/** Result of {@link checkAgentMemorySnapshot}. */
export interface SnapshotCheck {
  action: SnapshotAction;
  /** The snapshot's `updatedAt`, present for `initialize` / `prompt-update`. */
  snapshotTimestamp?: string;
}

/**
 * Decide whether a committed agent-memory snapshot should be offered to this
 * clone. Mirrors the reference's `checkAgentMemorySnapshot` three-state algorithm:
 *
 *  1. no `snapshot.json` (or corrupt)                    → `none`
 *  2. snapshot exists but no local `*.md` memory present → `initialize`
 *  3. local memory exists but no synced marker, OR the
 *     snapshot's `updatedAt` is newer than `syncedFrom`  → `prompt-update`
 *  4. otherwise (already synced to the latest snapshot)  → `none`
 */
export function checkAgentMemorySnapshot(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): SnapshotCheck {
  const snapshotMeta = readJsonFile(getSnapshotJsonPath(agentType, cwd), validateSnapshotMeta);
  if (!snapshotMeta) return { action: 'none' };

  const localMemDir = getAgentMemoryDir(agentType, scope, cwd, env);
  let hasLocalMemory = false;
  try {
    hasLocalMemory = fs
      .readdirSync(localMemDir, { withFileTypes: true })
      .some((d) => d.isFile() && d.name.endsWith('.md'));
  } catch {
    // Local memory directory doesn't exist yet.
  }

  if (!hasLocalMemory) {
    // Lifecycle introspection (parity: the reference's `logForDebugging('Initializing
    // agent memory for <agentType> from project snapshot')`, agentMemorySnapshot.ts):
    // surface the first-time-initialize decision so the snapshot sync lifecycle is
    // observable in the debug log.
    coreLog(
      SNAPSHOT_LOG_COMPONENT,
      `Initializing agent memory for ${agentType} from project snapshot`,
      { scope, snapshotTimestamp: snapshotMeta.updatedAt },
    );
    // No-PII introspection event (counts/scope/action only — never the memory
    // contents) so the snapshot sync lifecycle is observable off-box.
    logForDiagnosticsNoPII('info', 'agent_memory_snapshot', { action: 'initialize', scope });
    return { action: 'initialize', snapshotTimestamp: snapshotMeta.updatedAt };
  }

  const syncedMeta = readJsonFile(getSyncedJsonPath(agentType, scope, cwd, env), validateSyncedMeta);
  if (!syncedMeta || new Date(snapshotMeta.updatedAt) > new Date(syncedMeta.syncedFrom)) {
    // Lifecycle introspection (parity: the reference's `logForDebugging('Replacing
    // agent memory for <agentType> with project snapshot')`): surface that the
    // committed snapshot has advanced past what this clone last synced.
    coreLog(
      SNAPSHOT_LOG_COMPONENT,
      `Project snapshot for ${agentType} is newer than local agent memory — update available`,
      { scope, snapshotTimestamp: snapshotMeta.updatedAt, syncedFrom: syncedMeta?.syncedFrom ?? null },
    );
    logForDiagnosticsNoPII('info', 'agent_memory_snapshot', { action: 'prompt-update', scope });
    return { action: 'prompt-update', snapshotTimestamp: snapshotMeta.updatedAt };
  }

  return { action: 'none' };
}

/**
 * Record (in the committed snapshot dir) that the snapshot was updated at
 * `updatedAt`. Best-effort directory creation; throws only if the write itself
 * fails, so callers can surface a genuine permission problem.
 */
export function writeAgentSnapshotMeta(agentType: string, cwd: string, updatedAt: string): void {
  const dir = getSnapshotDirForAgent(agentType, cwd);
  fs.mkdirSync(dir, { recursive: true });
  const meta: SnapshotMeta = { updatedAt };
  fs.writeFileSync(path.join(dir, SNAPSHOT_META_FILENAME), JSON.stringify(meta));
}

/**
 * Mirror the committed snapshot's CONTENT files into this clone's local
 * agent-memory dir. Parity with the reference's `copySnapshotToLocal`:
 *
 *  - skips {@link SNAPSHOT_META_FILENAME} (metadata, not memory content);
 *  - skips any non-regular-file dirent, so a symlink planted in the snapshot
 *    dir is never dereferenced/copied (defensive containment);
 *  - best-effort: a read/write/readdir failure is logged and swallowed so a
 *    single unreadable/unwritable file (or a missing snapshot dir) never crashes
 *    the sync — it simply degrades to copying what it can.
 *
 * The local dir is created first (recursive, tolerant of a pre-existing dir).
 */
export function copySnapshotToLocal(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): void {
  const snapshotMemDir = getSnapshotDirForAgent(agentType, cwd);
  const localMemDir = getAgentMemoryDir(agentType, scope, cwd, env);
  fs.mkdirSync(localMemDir, { recursive: true });
  try {
    for (const dirent of fs.readdirSync(snapshotMemDir, { withFileTypes: true })) {
      // Content files only: skip the metadata file and anything that isn't a
      // regular file (directories, symlinks — never follow a planted symlink).
      if (!dirent.isFile() || dirent.name === SNAPSHOT_META_FILENAME) continue;
      const content = fs.readFileSync(path.join(snapshotMemDir, dirent.name), 'utf-8');
      fs.writeFileSync(path.join(localMemDir, dirent.name), content);
    }
  } catch (e) {
    coreLog(
      SNAPSHOT_LOG_COMPONENT,
      `Failed to copy project snapshot into local agent memory for ${agentType}`,
      { scope, error: String(e) },
    );
  }
}

/**
 * First-time seed of local agent memory from a committed snapshot: copy the
 * snapshot's content files in, then record the sync marker so a subsequent
 * {@link checkAgentMemorySnapshot} returns `none`. Parity with the reference's
 * `initializeFromSnapshot`. Use this for the `initialize` action.
 */
export function initializeFromSnapshot(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  snapshotTimestamp: string,
  env: NodeJS.ProcessEnv = process.env,
): void {
  coreLog(
    SNAPSHOT_LOG_COMPONENT,
    `Initializing agent memory for ${agentType} from project snapshot`,
    { scope, snapshotTimestamp },
  );
  logForDiagnosticsNoPII('info', 'agent_memory_snapshot', { action: 'initialize-sync', scope });
  copySnapshotToLocal(agentType, scope, cwd, env);
  saveAgentSyncedMeta(agentType, scope, cwd, snapshotTimestamp, env);
}

/**
 * Full replace of local agent memory with a committed snapshot: FIRST delete the
 * clone's existing `*.md` memory files (so entries removed from the snapshot do
 * not linger as orphans), THEN copy the snapshot in and record the sync marker.
 * Parity with the reference's `replaceFromSnapshot`. Use this for the
 * `prompt-update` action once the user accepts the update.
 *
 * Destructive deletion is scoped strictly to `*.md` files (any other local file
 * is left untouched), and the orphan-cleanup readdir is wrapped so a
 * not-yet-existing local dir is tolerated.
 */
export function replaceFromSnapshot(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  snapshotTimestamp: string,
  env: NodeJS.ProcessEnv = process.env,
): void {
  coreLog(
    SNAPSHOT_LOG_COMPONENT,
    `Replacing agent memory for ${agentType} with project snapshot`,
    { scope, snapshotTimestamp },
  );
  logForDiagnosticsNoPII('info', 'agent_memory_snapshot', { action: 'replace-sync', scope });
  const localMemDir = getAgentMemoryDir(agentType, scope, cwd, env);
  try {
    for (const dirent of fs.readdirSync(localMemDir, { withFileTypes: true })) {
      // Orphan cleanup, scoped strictly to `.md` content files.
      if (dirent.isFile() && dirent.name.endsWith('.md')) {
        fs.unlinkSync(path.join(localMemDir, dirent.name));
      }
    }
  } catch {
    // Local memory directory doesn't exist yet — nothing to clean up.
  }
  copySnapshotToLocal(agentType, scope, cwd, env);
  saveAgentSyncedMeta(agentType, scope, cwd, snapshotTimestamp, env);
}

/**
 * Record (in the LOCAL agent-memory dir) that this clone has synced from the
 * snapshot taken at `syncedFrom`, so a later {@link checkAgentMemorySnapshot}
 * returns `none` until the snapshot advances again.
 */
export function saveAgentSyncedMeta(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  syncedFrom: string,
  env: NodeJS.ProcessEnv = process.env,
): void {
  const dir = getAgentMemoryDir(agentType, scope, cwd, env);
  fs.mkdirSync(dir, { recursive: true });
  const meta: SyncedMeta = { syncedFrom };
  fs.writeFileSync(path.join(dir, SYNCED_META_FILENAME), JSON.stringify(meta));
  // Lifecycle introspection (parity: the reference's snapshot-sync `logForDebugging`
  // lines, agentMemorySnapshot.ts): record that this clone has committed a sync
  // marker so the "synced to <timestamp>" transition is visible in the debug log.
  coreLog(
    SNAPSHOT_LOG_COMPONENT,
    `Saved agent-memory snapshot sync marker for ${agentType}`,
    { scope, syncedFrom },
  );
  logForDiagnosticsNoPII('info', 'agent_memory_snapshot', { action: 'synced', scope });
}

/**
 * The three user-facing outcomes of an agent-memory snapshot-update prompt
 * (parity: the reference's `SnapshotUpdateDialog`, which resolves to
 * `'merge' | 'keep' | 'replace'`, `dialogLaunchers.tsx` L33):
 *
 *  - `keep`    → keep the clone's local memory; just record the sync so the
 *                prompt stops re-appearing (the safe no-op).
 *  - `replace` → discard local memory and take the committed snapshot wholesale.
 *  - `merge`   → keep local memory but ask the agent to reconcile it with the
 *                newer snapshot; a merge PROMPT is produced for the caller to
 *                inject into the agent's input.
 */
export type SnapshotUpdateResolution = (typeof SNAPSHOT_UPDATE_RESOLUTIONS)[number];

/** All valid {@link SnapshotUpdateResolution} values (for validation/iteration). */
export const SNAPSHOT_UPDATE_RESOLUTIONS = [
  'merge',
  'keep',
  'replace',
] as const;

/**
 * Safe default chosen when the update prompt is CANCELLED (parity: the reference's
 * `onCancel={() => done('keep')}`, `dialogLaunchers.tsx` L37). Cancelling never
 * mutates local memory — it degrades to the no-op `keep`.
 */
export const DEFAULT_SNAPSHOT_UPDATE_RESOLUTION: SnapshotUpdateResolution = 'keep';

/**
 * Normalize an untrusted / cancelled resolution value to a valid
 * {@link SnapshotUpdateResolution}, degrading anything that is not one of the
 * three known outcomes (including a cancel `null`/`undefined`) to the safe
 * {@link DEFAULT_SNAPSHOT_UPDATE_RESOLUTION} (`keep`). Mirrors the cancel
 * semantics of the reference's SnapshotUpdateDialog so an aborted/garbage choice
 * can never silently replace or lose local agent memory.
 */
export function resolveSnapshotUpdateChoice(value: unknown): SnapshotUpdateResolution {
  return SNAPSHOT_UPDATE_RESOLUTIONS.includes(value as SnapshotUpdateResolution)
    ? (value as SnapshotUpdateResolution)
    : DEFAULT_SNAPSHOT_UPDATE_RESOLUTION;
}

/**
 * Build the merge prompt injected into the agent's input when the user chooses
 * `merge` on a snapshot-update prompt (parity: the reference's `buildMergePrompt`,
 * imported in `main.tsx` L2268 and prepended to `inputPrompt`). The prompt tells
 * the agent to reconcile its newer committed project snapshot with its local
 * persistent memory rather than silently discarding either side.
 */
export function buildSnapshotMergePrompt(agentType: string, scope: AgentMemoryScope): string {
  return (
    `Your project has an updated committed memory snapshot for the "${agentType}" agent ` +
    `(${scope} scope) that is newer than your local persistent memory. ` +
    `Reconcile them: read both your local agent-memory files and the project snapshot, ` +
    `then merge the snapshot's new knowledge into your local memory — keeping entries ` +
    `that are still true, updating stale ones, and adding what the snapshot introduces. ` +
    `Do not blindly overwrite your local memory; integrate the two.`
  );
}

/** Result of {@link applySnapshotUpdateResolution}. */
export interface SnapshotUpdateOutcome {
  /** The resolution that was actually applied (after normalization). */
  resolution: SnapshotUpdateResolution;
  /** The merge prompt to inject, present only for the `merge` resolution. */
  mergePrompt?: string;
}

/**
 * Apply a user's three-way snapshot-update choice, dispatching to the matching
 * sync operation (parity: the reference's `main.tsx` L2260-2273 handling of the
 * `SnapshotUpdateDialog` result). The raw `choice` is first normalized through
 * {@link resolveSnapshotUpdateChoice}, so a cancel (`null`/`undefined`) or any
 * unexpected value degrades to the safe `keep`:
 *
 *  - `replace` → {@link replaceFromSnapshot} (discard local, take the snapshot).
 *  - `merge`   → mark synced (so we stop re-prompting) and return a merge prompt
 *                for the caller to inject; local memory is preserved for the
 *                agent to reconcile.
 *  - `keep`    → mark synced only; local memory is untouched (the safe no-op).
 *
 * Returns which resolution ran and, for `merge`, the prompt to inject.
 */
export function applySnapshotUpdateResolution(
  agentType: string,
  scope: AgentMemoryScope,
  snapshotTimestamp: string,
  choice: unknown,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): SnapshotUpdateOutcome {
  const resolution = resolveSnapshotUpdateChoice(choice);
  coreLog(
    SNAPSHOT_LOG_COMPONENT,
    `Resolving agent-memory snapshot update for ${agentType} as "${resolution}"`,
    { scope, resolution, snapshotTimestamp },
  );
  logForDiagnosticsNoPII('info', 'agent_memory_snapshot', { action: `resolve-${resolution}`, scope });
  switch (resolution) {
    case 'replace':
      replaceFromSnapshot(agentType, scope, cwd, snapshotTimestamp, env);
      return { resolution };
    case 'merge':
      // Keep local memory; record the sync so we stop re-prompting, and hand the
      // caller a prompt that tells the agent to reconcile the two.
      saveAgentSyncedMeta(agentType, scope, cwd, snapshotTimestamp, env);
      return { resolution, mergePrompt: buildSnapshotMergePrompt(agentType, scope) };
    case 'keep':
      // Safe no-op: leave local memory alone, just acknowledge the snapshot.
      saveAgentSyncedMeta(agentType, scope, cwd, snapshotTimestamp, env);
      return { resolution };
  }
}
