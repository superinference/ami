import { FileAttributionState, recordFileContribution } from '../session';

/**
 * In-process, session-scoped store of per-file AGENT attribution. It accumulates
 * how many characters the agent authored in each file it edits/creates during a
 * session, so a later `git_commit` can surface a per-file contribution figure in
 * its attribution introspection note. This is the SOURCE wiring for the existing
 * (previously unconsumed) {@link FileAttributionState} / {@link recordFileContribution}
 * data model in session.ts — parity with Claude Code's AttributionSnapshot, whose
 * `fileStates` track `claudeContribution` characters per file.
 *
 * Keyed by the tool-context `sessionId` (a sub-engine's `agentId` is folded in by
 * the caller when it wants per-agent isolation). Absent/empty session ids share a
 * single global bucket — mirroring the todo-store's `default:` fallback, so bare
 * engines and tests still record consistently. The map lives only for the process
 * lifetime; nothing is persisted here (the git note is the durable surface).
 */

const GLOBAL_KEY = '__global__';

const sessionFileAttribution = new Map<string, Record<string, FileAttributionState>>();

function keyFor(sessionId: string | undefined): string {
  return sessionId && sessionId.length > 0 ? sessionId : GLOBAL_KEY;
}

/**
 * Record that the agent authored `contributedChars` characters of `filePath`,
 * whose new full content is `content` (last modified at `mtime`). Delegates the
 * per-file accumulation + content-hash refresh to session.ts's
 * {@link recordFileContribution} (the one canonical update), keeping the running
 * total across successive edits in the same session.
 */
export function recordAgentFileContribution(
  sessionId: string | undefined,
  filePath: string,
  content: string,
  contributedChars: number,
  mtime: number,
): void {
  const key = keyFor(sessionId);
  const prev = sessionFileAttribution.get(key) ?? {};
  sessionFileAttribution.set(
    key,
    recordFileContribution(prev, filePath, content, contributedChars, mtime),
  );
}

/**
 * The per-file attribution map for a session (keyed by absolute file path), or an
 * empty object when nothing has been recorded. Returns the live reference — the
 * caller must not mutate it (consumers only read).
 */
export function getSessionFileAttribution(
  sessionId: string | undefined,
): Record<string, FileAttributionState> {
  return sessionFileAttribution.get(keyFor(sessionId)) ?? {};
}
