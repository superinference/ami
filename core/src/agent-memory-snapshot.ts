/**
 * Per-agent memory snapshot staleness detection (parity: Claude Code's
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
 * Parity with Claude Code's `readJsonFile(path, schema)`.
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
export type SnapshotAction = 'none' | 'initialize' | 'prompt-update';

/** Result of {@link checkAgentMemorySnapshot}. */
export interface SnapshotCheck {
  action: SnapshotAction;
  /** The snapshot's `updatedAt`, present for `initialize` / `prompt-update`. */
  snapshotTimestamp?: string;
}

/**
 * Decide whether a committed agent-memory snapshot should be offered to this
 * clone. Mirrors Claude Code's `checkAgentMemorySnapshot` three-state algorithm:
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
    // Lifecycle introspection (parity: Claude Code's `logForDebugging('Initializing
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
    // Lifecycle introspection (parity: Claude Code's `logForDebugging('Replacing
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
  // Lifecycle introspection (parity: Claude Code's snapshot-sync `logForDebugging`
  // lines, agentMemorySnapshot.ts): record that this clone has committed a sync
  // marker so the "synced to <timestamp>" transition is visible in the debug log.
  coreLog(
    SNAPSHOT_LOG_COMPONENT,
    `Saved agent-memory snapshot sync marker for ${agentType}`,
    { scope, syncedFrom },
  );
  logForDiagnosticsNoPII('info', 'agent_memory_snapshot', { action: 'synced', scope });
}
